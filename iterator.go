package mapper

import (
	"fmt"
	"time"

	"github.com/captaincodeman/datastore-locker"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

type (
	// iterator processes namespaces for a job
	iterator struct {
		locker.Lock
		common

		// Namespace is the namespace to begin iterating from
		Namespace string `datastore:"namespace,noindex"`
	}
)

const (
	iteratorKind = "iterator"
)

func (it *iterator) setJob(job *job) {
	it.job = job
}

func (it *iterator) copyFrom(x iterator) {
	it.Lock = x.Lock
	it.common = x.common
	it.Namespace = x.Namespace
}

func (it *iterator) jobKey(c context.Context, config Config) *datastore.Key {
	return datastore.NewKey(c, config.DatastorePrefix+jobKind, it.id, 0, nil)
}

func (it *iterator) jobID() string {
	return it.id
}

func (it *iterator) process(c context.Context, mapper *mapper) error {
	// process the current namespace - split the query and create shards
	log.Debugf(c, "process %s", it.Namespace)

	it.Count++

	id := fmt.Sprintf("%s-%s", it.id, it.Namespace)
	key := datastore.NewKey(c, mapper.config.DatastorePrefix+namespaceKind, id, 0, nil)

	q := it.Query.Namespace(it.Namespace)

	ns := new(namespace)
	ns.start(q)
	ns.Namespace = it.Namespace

	return mapper.locker.Schedule(c, key, ns, mapper.config.Path+namespaceURL, nil)
}

func (it *iterator) createQuery(c context.Context) *datastore.Query {
	q := datastore.NewQuery("__namespace__")
	if it.Namespace != "" {
		q = q.Filter("__key__ >=", datastore.NewKey(c, "__namespace__", it.Namespace, 0, nil))
	}
	q = q.Order("__key__")
	q = q.KeysOnly()
	return q
}

func (it *iterator) iterate(c context.Context, mapper *mapper) (bool, error) {
	// use the full 10 minutes allowed (assuming front-end instance type)
	c, _ = context.WithTimeout(c, time.Duration(10)*time.Minute)

	// if the query defines the specific namespaces to process then we
	// don't really need to query for them - we can just process that
	// list immediately
	if len(it.Query.namespaces) > 0 {
		for _, namespace := range it.Query.namespaces {
			it.Namespace = namespace
			it.process(c, mapper)
		}
		return true, nil
	}

	// we query and proecess in batches of 'size' and also create a
	// timeout signal that is checked after each batch. So, a batch has
	// to take less than 5 minutes to execute
	size := 50
	timeout := make(chan bool, 1)
	timer := time.AfterFunc(time.Duration(5)*time.Minute, func() {
		timeout <- true
	})
	defer timer.Stop()

	for {
		count := 0
		q := it.createQuery(c).Limit(size)
		t := q.Run(c)
		for {
			key, err := t.Next(nil)
			if err == datastore.Done {
				break
			}
			if err != nil {
				log.Errorf(c, "error %s", err.Error())
				return false, err
			}

			it.Namespace = key.StringID()
			if err := it.process(c, mapper); err != nil {
				return false, err
			}
			count++
		}

		// did we process a full batch? if not, we hit the end of the dataset
		if count < size {
			return true, nil
		}

		// check if we've timed out
		select {
		case <-timeout:
			// timeout without completing
			return false, nil
		default:
			// continue processing in thsi request
		}
	}
}

func (it *iterator) completed(c context.Context, mapper *mapper, key *datastore.Key) error {
	// mark iterator as complete
	it.complete()

	// update iterator status and job within a transaction
	queue, ok := locker.QueueFromContext(c)
	if !ok {
		queue = mapper.config.DefaultQueue
	}
	fresh := new(iterator)
	jobKey := it.jobKey(c, *mapper.config)
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

		fresh.copyFrom(*it)
		fresh.Lock.Complete()

		job.NamespacesTotal += int(it.Count)
		job.Iterating = false
		
		// only the iterator walltime is rolled up into the job counts
		job.WallTime += it.WallTime

		// it's unlikely (but possible) that the shards and namespaces completed
		// before this task so handle case that job is now also fully complete
		if job.NamespacesSuccessful == job.NamespacesTotal && !job.Iterating {
			t := mapper.locker.NewTask(jobKey, job, mapper.config.Path+jobCompleteURL, nil)
			if _, err := taskqueue.Add(tc, t, queue); err != nil {
				return err
			}
		}

		if _, err := storage.PutMulti(tc, keys, vals); err != nil {
			return err
		}
		return nil
	}, &datastore.TransactionOptions{XG: true})
}

// Load implements the datastore PropertyLoadSaver imterface
func (it *iterator) Load(props []datastore.Property) error {
	datastore.LoadStruct(it, props)
	it.common.Load(props)
	return nil
}

// Save implements the datastore PropertyLoadSaver imterface
func (it *iterator) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(it)
	if err != nil {
		return nil, err
	}

	cprops, err := it.common.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, cprops...)

	return props, nil
}
