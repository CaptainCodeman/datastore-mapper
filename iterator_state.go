package mapper

import (
	"fmt"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

type (
	// iterator processes namespaces for a job
	iterator struct {
		common
		lock

		// Namespace is the namespace to begin iterating from
		Namespace string `datastore:"namespace,noindex"`

		job *jobState
	}
)

const (
	iteratorKind = "iterator"
)

func (it *iterator) process(c context.Context) error {
	// process the current namespace - split the query and create shards
	log.Debugf(c, "process %s", it.Namespace)

	it.Count++

	id := fmt.Sprintf("%s-%s", it.id, it.Namespace)
	key := datastore.NewKey(c, config.DatastorePrefix+namespaceKind, id, 0, nil)

	s := new(namespaceState)
	s.start()
	s.Namespace = it.Namespace

	_, err := ScheduleLock(c, key, s, config.BasePath+namespaceURL, nil, it.queue)
	return err

	/*
		ns, _ := appengine.Namespace(c, n.Namespace)

		q := datastore.NewQuery("photo")
		q = q.Order("__scatter__")
		q = q.Limit(3)
		q = q.KeysOnly()
		randomKeys, err := q.GetAll(ns, nil)
		if err != nil || len(randomKeys) == 0 {
			return nil
		}

		for i := 0; i < len(randomKeys); i++ {
			log.Debugf(c, "key %d %s", i, randomKeys[i].String())
		}
	*/
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

func (it *iterator) iterate(c context.Context) (bool, error) {
	// use the full 10 minutes allowed (assuming front-end instance type)
	c, _ = context.WithTimeout(c, time.Duration(10)*time.Minute)

	// if the query defines the specific namespaces to process then we
	// don't really need to query for them - we can just process that
	// list immediately
	if len(it.job.Query.namespace) > 0 {
		for _, namespace := range it.job.Query.namespace {
			it.Namespace = namespace
			it.process(c)
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
			if err := it.process(c); err != nil {
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

/* datastore */
func (it *iterator) Load(props []datastore.Property) error {
	datastore.LoadStruct(it, props)
	it.common.Load(props)
	it.lock.Load(props)
	return nil
}

func (it *iterator) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(it)
	if err != nil {
		return nil, err
	}

	jprops, err := it.common.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, jprops...)

	lprops, err := it.lock.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, lprops...)

	return props, nil
}
