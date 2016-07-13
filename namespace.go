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
	"google.golang.org/appengine/taskqueue"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	apistorage "google.golang.org/api/storage/v1"
)

type (
	// namespace processes a single namespace for a job
	namespace struct {
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

		job *job
	}
)

const (
	namespaceKind = "namespace"
)

func (n *namespace) setJob(job *job) {
	n.job = job
}

func (n *namespace) copyFrom(x namespace) {
	n.common = x.common
	n.lock = x.lock
	n.Namespace = x.Namespace
	n.ShardsTotal = x.ShardsTotal
	n.ShardsSuccessful = x.ShardsSuccessful
	n.ShardsFailed = x.ShardsFailed
}

func (n *namespace) jobKey(c context.Context, config Config) *datastore.Key {
	return datastore.NewKey(c, config.DatastorePrefix+jobKind, n.jobID(), 0, nil)
}

func (n *namespace) jobID() string {
	parts := strings.Split(n.id, "-")
	return parts[0] + "-" + parts[1]
}

func (n *namespace) namespaceFilename() string {
	parts := strings.Split(n.id, "-")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace
	// TODO: set filename or at least extension
	return parts[0] + "-" + parts[1] + "/" + ns + ".json"
}

func (n *namespace) shardFilename(shard int) string {
	parts := strings.Split(n.id, "-")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace / shard
	// TODO: set filename or at least extension
	return parts[0] + "-" + parts[1] + "/" + ns + "/" + strconv.Itoa(shard) + ".json"
}

func (n *namespace) split(c context.Context, config Config) error {
	keyRanges := []*KeyRange{}

	// TODO: split query on more than just key range, include any property filters
	// TODO: store query on shard instead of KeyRange and use that for processing

	if n.job.Shards == 1 {
		keyRanges = append(keyRanges, newKeyRange(n.Namespace, nil, nil, Ascending, false))
	} else {
		ns, _ := appengine.Namespace(c, n.Namespace)
		minimumShards := n.job.Shards
		oversamplingFactor := config.Oversampling
		limit := minimumShards * oversamplingFactor

		q := n.job.Query.toDatastoreQuery()
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
			keyRanges = append(keyRanges, newKeyRange(n.Namespace, nil, nil, Ascending, true))
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
				randomKeys, _ = n.chooseSplitPoints(randomKeys, minimumShards)
			}

			keyRanges = append(keyRanges, newKeyRange(n.Namespace, nil, randomKeys[0], Ascending, false))
			for i := 0; i < len(randomKeys)-1; i++ {
				keyRanges = append(keyRanges, newKeyRange(n.Namespace, randomKeys[i], randomKeys[i+1], Ascending, true))
			}
			keyRanges = append(keyRanges, newKeyRange(n.Namespace, randomKeys[len(randomKeys)-1], nil, Ascending, true))
		}
	}

	n.ShardsTotal = len(keyRanges)

	// Even though this method does not schedule shard task and save shard state
	// transactionally, it's safe for taskqueue to retry this logic because
	// the initial shard_state for each shard is the same from any retry.
	// This is an important yet reasonable assumption on shard.

	keys := make([]*datastore.Key, len(keyRanges))
	for shardNumber := 0; shardNumber < len(keyRanges); shardNumber++ {
		id := fmt.Sprintf("%s-%d", n.id, shardNumber)
		key := datastore.NewKey(c, config.DatastorePrefix+shardKind, id, 0, nil)
		keys[shardNumber] = key
	}

	shards := make([]*shard, len(keyRanges))
	if err := storage.GetMulti(c, keys, shards); err != nil {
		if me, ok := err.(appengine.MultiError); ok {
			for shardNumber, merr := range me {
				if merr == datastore.ErrNoSuchEntity {
					// keys[shardNumber] is missing, create the shard for it
					k := keys[shardNumber]

					s := new(shard)
					s.start()
					s.Shard = shardNumber
					s.Namespace = n.Namespace
					s.KeyRange = keyRanges[shardNumber]
					s.Description = keyRanges[shardNumber].String()

					if err := ScheduleLock(c, k, s, config.Path+shardURL, nil, n.queue); err != nil {
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

func (n *namespace) update(c context.Context, config Config, key *datastore.Key) error {
	// update namespace status within a transaction
	return storage.RunInTransaction(c, func(tc context.Context) error {
		fresh := new(namespace)
		if err := storage.Get(tc, key, fresh); err != nil {
			return err
		}

		// shards can already be processing ahead of this total being written
		fresh.ShardsTotal = n.ShardsTotal
		fresh.RequestID = ""

		// if all shards have completed, schedule namespace/completed to update job
		if fresh.ShardsSuccessful == fresh.ShardsTotal {
			t := NewLockTask(key, fresh, config.Path+namespaceCompleteURL, nil)
			if _, err := taskqueue.Add(tc, t, n.queue); err != nil {
				log.Errorf(c, "add task %s", err.Error())
				return err
			}
		}

		if _, err := storage.Put(tc, key, fresh); err != nil {
			return err
		}

		return nil
	}, &datastore.TransactionOptions{XG: true, Attempts: attempts})
}

func (n *namespace) completed(c context.Context, config Config, key *datastore.Key) error {
	n.complete()
	n.RequestID = ""

	// update namespace status and job within a transaction
	fresh := new(namespace)
	jobKey := n.jobKey(c, config)
	job := new(job)
	return storage.RunInTransaction(c, func(tc context.Context) error {
		keys := []*datastore.Key{key, jobKey}
		vals := []interface{}{fresh, job}
		if err := storage.GetMulti(tc, keys, vals); err != nil {
			return err
		}

		if job.Abort {
			return nil
		}

		fresh.copyFrom(*n)
		job.NamespacesSuccessful++
		job.common.rollup(n.common)

		if job.NamespacesSuccessful == job.NamespacesTotal && !job.Iterating {
			t := NewLockTask(jobKey, job, config.Path+jobCompleteURL, nil)
			if _, err := taskqueue.Add(tc, t, n.queue); err != nil {
				return err
			}
		}

		if _, err := storage.PutMulti(tc, keys, vals); err != nil {
			return err
		}
		return nil
	}, &datastore.TransactionOptions{XG: true, Attempts: attempts})
}

// Returns the best split points given a random set of datastore.Keys
func (n *namespace) chooseSplitPoints(sortedKeys []*datastore.Key, shardCount int) ([]*datastore.Key, error) {
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
func (n *namespace) rollup(c context.Context) error {
	// nothing to do if no output writing
	if n.job.Bucket == "" {
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

	namespaceFilename := n.namespaceFilename()
	log.Debugf(c, "compose %s", namespaceFilename)
	req := &apistorage.ComposeRequest{
		Destination:   &apistorage.Object{Name: namespaceFilename},
		SourceObjects: make([]*apistorage.ComposeRequestSourceObjects, n.ShardsTotal),
	}
	for i := 0; i < n.ShardsTotal; i++ {
		shardFilename := n.shardFilename(i)
		log.Debugf(c, "source %s", shardFilename)
		req.SourceObjects[i] = &apistorage.ComposeRequestSourceObjects{Name: shardFilename}
	}

	res, err := service.Objects.Compose(n.job.Bucket, namespaceFilename, req).Context(c).Do()
	if err != nil {
		return err
	}
	log.Debugf(c, "created shard file %s gen %d %d bytes", res.Name, res.Generation, res.Size)

	// delete slice files
	for i := 0; i < n.ShardsTotal; i++ {
		shardFilename := n.shardFilename(i)
		log.Debugf(c, "delete %s", shardFilename)
		service.Objects.Delete(n.job.Bucket, shardFilename).Context(c).Do()
		// do we care about errors? provide separate cleanup option
	}

	return nil
}

// Load implements the datastore PropertyLoadSaver imterface
func (n *namespace) Load(props []datastore.Property) error {
	datastore.LoadStruct(n, props)
	n.common.Load(props)
	n.lock.Load(props)
	return nil
}

// Save implements the datastore PropertyLoadSaver imterface
func (n *namespace) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(n)
	if err != nil {
		return nil, err
	}

	jprops, err := n.common.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, jprops...)

	lprops, err := n.lock.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, lprops...)

	return props, nil
}
