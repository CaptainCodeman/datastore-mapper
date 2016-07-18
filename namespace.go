package mapper

import (
	"fmt"
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

		// Query is the datastore query for this namespace
		Query *Query `datastore:"-"`

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
	queries, err := n.Query.split(c, n.job.Shards, config.Oversampling)
	if err != nil {
		return err
	}

	n.ShardsTotal = len(queries)

	// from original source:
	// Even though this method does not schedule shard task and save shard state
	// transactionally, it's safe for taskqueue to retry this logic because
	// the initial shard_state for each shard is the same from any retry.
	// This is an important yet reasonable assumption on shard.

	// I'm not so sure - it feels like the query splitting could easily return
	// a different set of values unless the datastore is in read-only mode ...
	// so maybe have one task to split the query, then another (that can be
	// repeated) which does the shard scheduling ...

	keys := make([]*datastore.Key, n.ShardsTotal)
	for i := 0; i < n.ShardsTotal; i++ {
		id := fmt.Sprintf("%s-%d", n.id, i)
		key := datastore.NewKey(c, config.DatastorePrefix+shardKind, id, 0, nil)
		keys[i] = key
	}

	shards := make([]*shard, n.ShardsTotal)
	if err := storage.GetMulti(c, keys, shards); err != nil {
		if me, ok := err.(appengine.MultiError); ok {
			for i, merr := range me {
				if merr == datastore.ErrNoSuchEntity {
					// keys[i] is missing, create the shard for it
					k := keys[i]

					q := queries[i].Namespace(n.Namespace)

					s := new(shard)
					s.start(q)
					s.Shard = i
					s.Namespace = n.Namespace
					s.Description = q.String()

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

// rollup shards into single namespace file
func (n *namespace) rollup(c context.Context) error {
	// nothing to do if no output writing or no output writtern
	if n.job.Bucket == "" || n.Count == 0 {
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

	for _, prop := range props {
		switch prop.Name {
		case "query":
			n.Query = &Query{}
			if err := n.Query.GobDecode(prop.Value.([]byte)); err != nil {
				return err
			}
		}
	}

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
