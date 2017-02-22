package mapper

import (
	"fmt"
	"strconv"
	"strings"

	"io/ioutil"

	"github.com/captaincodeman/datastore-locker"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	apistorage "google.golang.org/api/storage/v1"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

type (
	// namespace processes a single namespace for a job
	namespace struct {
		locker.Lock
		common

		// Namespace is the namespace to process
		Namespace string `datastore:"namespace,noindex"`

		// ShardsTotal is the total number of shards generated for this namespace
		ShardsTotal int `datastore:"shards_total,noindex"`

		// ShardsSuccessful is the number of shards completed successfully
		ShardsSuccessful int `datastore:"shards_successful,noindex"`

		// ShardsFailed is the number of shards failed
		ShardsFailed int `datastore:"shards_failed,noindex"`
	}
)

const (
	namespaceKind = "namespace"
)

func (n *namespace) setJob(job *job) {
	n.job = job
}

func (n *namespace) copyFrom(x namespace) {
	n.Lock = x.Lock
	n.common = x.common
	n.Namespace = x.Namespace
	n.ShardsTotal = x.ShardsTotal
	n.ShardsSuccessful = x.ShardsSuccessful
	n.ShardsFailed = x.ShardsFailed
}

func (n *namespace) jobID() string {
	parts := strings.Split(n.id, "/")
	return parts[0] + "/" + parts[1]
}

func (n *namespace) jobKey(c context.Context, config Config) *datastore.Key {
	return datastore.NewKey(c, config.DatastorePrefix+jobKind, n.jobID(), 0, nil)
}

func (n *namespace) namespaceFilename() string {
	parts := strings.Split(n.id, "/")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace
	// TODO: set filename or at least extension
	return parts[0] + "/" + parts[1] + "/" + ns + ".json"
}

func (n *namespace) shardFilename(shard int) string {
	parts := strings.Split(n.id, "/")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace / shard
	// TODO: set filename or at least extension
	return parts[0] + "/" + parts[1] + "/" + ns + "/" + strconv.Itoa(shard) + ".json"
}

func (n *namespace) split(c context.Context, mapper *mapper) error {
	queries, err := n.Query.split(c, n.job.Shards, mapper.config.Oversampling)
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
		id := fmt.Sprintf("%s/%d", n.id, i)
		key := datastore.NewKey(c, mapper.config.DatastorePrefix+shardKind, id, 0, nil)
		keys[i] = key
	}

	shards := make([]*shard, n.ShardsTotal)
	if err := datastore.GetMulti(c, keys, shards); err != nil {
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

					if err := mapper.locker.Schedule(c, k, s, mapper.config.Path+shardURL, nil); err != nil {
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

func (n *namespace) update(c context.Context, mapper *mapper, key *datastore.Key) error {
	queue, ok := locker.QueueFromContext(c)
	if !ok {
		queue = mapper.config.DefaultQueue
	}

	// update namespace status within a transaction
	return datastore.RunInTransaction(c, func(tc context.Context) error {
		fresh := new(namespace)
		if err := datastore.Get(tc, key, fresh); err != nil {
			return err
		}

		// shards can already be processing ahead of this total being written
		fresh.ShardsTotal = n.ShardsTotal

		// if all shards have completed, schedule namespace/completed to update job
		if fresh.ShardsSuccessful == fresh.ShardsTotal {
			t := mapper.locker.NewTask(key, fresh, mapper.config.Path+namespaceCompleteURL, nil)
			if _, err := taskqueue.Add(tc, t, queue); err != nil {
				log.Errorf(c, "add task %s", err.Error())
				return err
			}
		}

		if _, err := datastore.Put(tc, key, fresh); err != nil {
			return err
		}

		return nil
	}, &datastore.TransactionOptions{XG: true})
}

func (n *namespace) completed(c context.Context, mapper *mapper, key *datastore.Key) error {
	n.complete()

	// update namespace status and job within a transaction
	queue, ok := locker.QueueFromContext(c)
	if !ok {
		queue = mapper.config.DefaultQueue
	}
	fresh := new(namespace)
	jobKey := n.jobKey(c, *mapper.config)
	job := new(job)

	return datastore.RunInTransaction(c, func(tc context.Context) error {
		keys := []*datastore.Key{key, jobKey}
		vals := []interface{}{fresh, job}
		if err := datastore.GetMulti(tc, keys, vals); err != nil {
			return err
		}

		if job.Abort {
			return nil
		}

		fresh.copyFrom(*n)
		fresh.Lock.Complete()

		job.NamespacesSuccessful++
		job.common.rollup(n.common)

		if job.NamespacesSuccessful == job.NamespacesTotal && !job.Iterating {
			t := mapper.locker.NewTask(jobKey, job, mapper.config.Path+jobCompleteURL, nil)
			if _, err := taskqueue.Add(tc, t, queue); err != nil {
				return err
			}
		}

		if _, err := datastore.PutMulti(tc, keys, vals); err != nil {
			return err
		}
		return nil
	}, &datastore.TransactionOptions{XG: true})
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

	target := n.namespaceFilename()
	sources := make([]string, n.ShardsTotal)
	for i := 0; i < n.ShardsTotal; i++ {
		sources[i] = n.shardFilename(i)
	}

	return n.combineFiles(c, service, target, sources)
}

func (n *namespace) combineFiles(c context.Context, service *apistorage.Service, target string, sources []string) error {
	if _, err := service.Objects.Get(n.job.Bucket, target).Context(c).Do(); err != nil {

		count := len(sources)

		switch {

		case count == 1:
			// copy single file
			_, err := service.Objects.Copy(n.job.Bucket, sources[0], n.job.Bucket, target, nil).Context(c).Do()
			return err

		case count <= 32:
			// combine source files to target
			log.Debugf(c, "compose %s from %d sources", target, count)
			req := &apistorage.ComposeRequest{
				Destination:   &apistorage.Object{Name: target},
				SourceObjects: make([]*apistorage.ComposeRequestSourceObjects, count),
			}
			for i := 0; i < count; i++ {
				log.Debugf(c, "source %d %s", i, sources[i])
				req.SourceObjects[i] = &apistorage.ComposeRequestSourceObjects{Name: sources[i]}
			}

			res, err := service.Objects.Compose(n.job.Bucket, target, req).Context(c).Do()
			if err != nil {
				return err
			}
			log.Debugf(c, "created file %s gen %d %d bytes", res.Name, res.Generation, res.Size)

		case count > 32:
			// rollup to intermediate files
			steps := ((count - 1) / 32) + 1
			targets := make([]string, steps)
			for i := 0; i < steps; i++ {
				targets[i] = fmt.Sprintf("%s-%d", target, i)

				start := i * 32
				stop := i*32 + 32
				if stop > count {
					stop = count
				}
				if err := n.combineFiles(c, service, targets[i], sources[start:stop]); err != nil {
					return err
				}
			}

			if err := n.combineFiles(c, service, target, targets); err != nil {
				return err
			}
		}

		// delete source files
		for i := 0; i < count; i++ {
			log.Debugf(c, "delete %s", sources[i])
			if err := service.Objects.Delete(n.job.Bucket, sources[i]).Context(c).Do(); err != nil {
				// TODO: check for 404 error and ignore but return err for failures (to ensure cleanup)
				return nil
			}
		}
	}

	return nil
}

// Load implements the datastore PropertyLoadSaver imterface
func (n *namespace) Load(props []datastore.Property) error {
	datastore.LoadStruct(n, props)
	n.common.Load(props)

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

	cprops, err := n.common.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, cprops...)

	return props, nil
}
