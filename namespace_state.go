package mapper

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"io/ioutil"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	apistorage "google.golang.org/api/storage/v1"
)

type (
	// namespaceState processes a single namespace for a job
	namespaceState struct {
		common
		lock

		// Namespace is the namespace to process
		Namespace string `datastore:"namespace,noindex"`

		// ShardsTotal is the total number of shards generated for this namespace
		ShardsTotal int `datastore:"shards_total,noindex"`

		// ShardsSuccessful is the number of shards completed successfully
		ShardsSuccessful int `datastore:"shards_successful,noindex"`

		// ShardsFailed is the number of shards failed
		ShardsFailed int `datastore:"shards_failed,noindex"`

		job *jobState
	}
)

const (
	namespaceKind = "namespace"
)

func (s *namespaceState) jobID() string {
	parts := strings.Split(s.id, "-")
	return parts[0] + "-" + parts[1]
}

func (s *namespaceState) namespaceFilename() string {
	parts := strings.Split(s.id, "-")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace
	// TODO: set filename or at least extension
	return parts[0] + "-" + parts[1] + "/" + ns + ".json"
}

func (s *namespaceState) shardFilename(shard int) string {
	parts := strings.Split(s.id, "-")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace / shard
	// TODO: set filename or at least extension
	return parts[0] + "-" + parts[1] + "/" + ns + "/" + strconv.Itoa(shard) + ".json"
}

func (s *namespaceState) split(c context.Context) error {
	keyRanges := []*KeyRange{}

	if s.job.Shards == 1 {
		keyRanges = append(keyRanges, newKeyRange(s.Namespace, nil, nil, Ascending, false))
	} else {
		ns, _ := appengine.Namespace(c, s.Namespace)
		minimumShards := s.job.Shards
		oversamplingFactor := config.OversamplingFactor
		limit := minimumShards * oversamplingFactor

		q := s.job.Query.toDatastoreQuery()
		q = q.Order("__scatter__")
		q = q.Limit(limit)
		q = q.KeysOnly()

		randomKeys, err := q.GetAll(ns, nil)
		if err != nil {
			log.Errorf(c, "error %s", err.Error())
		}
		if len(randomKeys) == 0 {
			log.Errorf(c, "no keys")
			// There are no entities with scatter property. We have no idea how to split.

			// TODO: split property key
			keyRanges = append(keyRanges, newKeyRange(s.Namespace, nil, nil, Ascending, true))
		} else {
			// this assumes that all the keys are consistently int or string (reasonable)
			if randomKeys[0].IntID() > 0 {
				sort.Sort(byIntKey(randomKeys))
			} else {
				sort.Sort(byStringKey(randomKeys))
			}

			log.Debugf(c, "splits %d", len(randomKeys))
			// TODO: use the number of random keys to do a rough approximation of the
			// minimum size of the table. A scatter property is added roughly once every
			// 512 records (although the wiki says 0.78% chance which is more like 128)
			// When the number of random keys is below some threshold, reduce the number
			// of shards accordingly so we're not burning tasks for trivial numbers of
			// records - some namespaces may only need a single task to process

			if len(randomKeys) > minimumShards {
				randomKeys, _ = s.chooseSplitPoints(randomKeys, minimumShards)
			}

			keyRanges = append(keyRanges, newKeyRange(s.Namespace, nil, randomKeys[0], Ascending, false))
			for i := 0; i < len(randomKeys)-1; i++ {
				keyRanges = append(keyRanges, newKeyRange(s.Namespace, randomKeys[i], randomKeys[i+1], Ascending, true))
			}
			keyRanges = append(keyRanges, newKeyRange(s.Namespace, randomKeys[len(randomKeys)-1], nil, Ascending, true))
		}
	}

	s.ShardsTotal = len(keyRanges)

	// if no keyranges then nothing to do for this namespace, short-circuit to completed

	// Even though this method does not schedule shard task and save shard state
	// transactionally, it's safe for taskqueue to retry this logic because
	// the initial shard_state for each shard is the same from any retry.
	// This is an important yet reasonable assumption on ShardState.

	keys := make([]*datastore.Key, len(keyRanges))
	for shardNumber := 0; shardNumber < len(keyRanges); shardNumber++ {
		id := fmt.Sprintf("%s-%d", s.id, shardNumber)
		key := datastore.NewKey(c, config.DatastorePrefix+shardKind, id, 0, nil)
		keys[shardNumber] = key
	}

	shards := make([]*shardState, len(keyRanges))
	if err := storage.GetMulti(c, keys, shards); err != nil {
		if me, ok := err.(appengine.MultiError); ok {
			for shardNumber, merr := range me {
				if merr == datastore.ErrNoSuchEntity {
					// keys[shardNumber] is missing, create the shard for it
					key := keys[shardNumber]

					shard := new(shardState)
					shard.start()
					shard.Shard = shardNumber
					shard.Namespace = s.Namespace
					shard.KeyRange = keyRanges[shardNumber]
					shard.Description = keyRanges[shardNumber].String()

					if _, err := ScheduleLock(c, key, shard, config.BasePath+shardURL, nil, s.queue); err != nil {
						return err
					}
				}
			}
		} else {
			// some other error
			return err
		}
	}

	return nil
}

// Returns the best split points given a random set of datastore.Keys
func (s *namespaceState) chooseSplitPoints(sortedKeys []*datastore.Key, shardCount int) ([]*datastore.Key, error) {
	indexStride := float64(len(sortedKeys)) / float64(shardCount)
	if indexStride < 1 {
		return nil, fmt.Errorf("not enough keys")
	}
	results := make([]*datastore.Key, 0, shardCount)
	for i := 1; i < shardCount; i++ {
		idx := int(math.Floor(indexStride*float64(i) + 0.5))
		results = append(results, sortedKeys[idx])
	}

	return results, nil
}

// rollup shards into single namespace file
func (s *namespaceState) rollup(c context.Context) error {
	// nothing to do if no output writing
	if s.job.Bucket == "" {
		return nil
	}

	var service *apistorage.Service
	if appengine.IsDevAppServer() {
		jsonKey, err := ioutil.ReadFile("service-account.json")
		if err != nil {
			return err
		}
		conf, err := google.JWTConfigFromJSON(jsonKey, apistorage.DevstorageReadWriteScope)
		if err != nil {
			return err
		}
		client := conf.Client(c)
		service, err = apistorage.New(client)
		if err != nil {
			return err
		}
	} else {
		var err error
		token := google.AppEngineTokenSource(c, apistorage.DevstorageReadWriteScope)
		client := oauth2.NewClient(c, token)
		service, err = apistorage.New(client)
		if err != nil {
			return err
		}
	}

	// TODO: if only 1 shard, copy / move file instead
	// TODO: check if shard file already exists and skip compose
	// TODO: logic to handle more than 32 composable files

	namespaceFilename := s.namespaceFilename()
	log.Debugf(c, "compose %s", namespaceFilename)
	req := &apistorage.ComposeRequest{
		Destination:   &apistorage.Object{Name: namespaceFilename},
		SourceObjects: make([]*apistorage.ComposeRequestSourceObjects, s.ShardsTotal),
	}
	for i := 0; i < s.ShardsTotal; i++ {
		shardFilename := s.shardFilename(i)
		log.Debugf(c, "source %s", shardFilename)
		req.SourceObjects[i] = &apistorage.ComposeRequestSourceObjects{Name: shardFilename}
	}

	res, err := service.Objects.Compose(s.job.Bucket, namespaceFilename, req).Context(c).Do()
	if err != nil {
		return err
	}
	log.Debugf(c, "created shard file %s gen %d %d bytes", res.Name, res.Generation, res.Size)

	// delete slice files
	for i := 0; i < s.ShardsTotal; i++ {
		shardFilename := s.shardFilename(i)
		log.Debugf(c, "delete %s", shardFilename)
		service.Objects.Delete(s.job.Bucket, shardFilename).Context(c).Do()
		// do we care about errors? provide separate cleanup option
	}

	return nil
}

/* datastore */
func (s *namespaceState) Load(props []datastore.Property) error {
	datastore.LoadStruct(s, props)
	s.common.Load(props)
	s.lock.Load(props)
	return nil
}

func (s *namespaceState) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(s)
	if err != nil {
		return nil, err
	}

	jprops, err := s.common.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, jprops...)

	lprops, err := s.lock.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, lprops...)

	return props, nil
}
