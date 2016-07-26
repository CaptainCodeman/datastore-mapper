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

		// Cursor is the datastore cursor to start from
		Cursor string `datastore:"cursor,noindex"`
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
	it.Cursor = x.Cursor
}

func (it *iterator) jobKey(c context.Context, config Config) *datastore.Key {
	return datastore.NewKey(c, config.DatastorePrefix+jobKind, it.id, 0, nil)
}

func (it *iterator) jobID() string {
	return it.id
}

func (it *iterator) process(c context.Context, mapper *mapper, namespaceStr string) error {
	// process the namespace - split the query and create shards
	log.Debugf(c, "process %s", namespaceStr)

	it.Count++

	id := fmt.Sprintf("%s/%s", it.id, namespaceStr)
	key := datastore.NewKey(c, mapper.config.DatastorePrefix+namespaceKind, id, 0, nil)

	q := it.Query.Namespace(namespaceStr)

	ns := new(namespace)
	ns.start(q)
	ns.Namespace = namespaceStr

	// TODO: what if *this* task is restarted - ns tasks
	// would be re-created and could alreadt be executing

	return mapper.locker.Schedule(c, key, ns, mapper.config.Path+namespaceURL, nil)
}

func (it *iterator) createQuery(c context.Context) *datastore.Query {
	q := datastore.NewQuery("__namespace__")
	switch it.Query.selection {
	case all:
	// no filter
	case empty:
		q = q.Filter("__key__ =", datastore.NewKey(c, "__namespace__", "", 1, nil))
	case named:
		q = q.Filter("__key__ >", datastore.NewKey(c, "__namespace__", "", 1, nil))
	}
	q = q.Order("__key__")
	q = q.KeysOnly()
	return q
}

func (it *iterator) iterate(c context.Context, mapper *mapper) (bool, error) {
	taskTimeout := time.After(mapper.config.TaskTimeout)
	taskRunning := true

	// if the query defines the specific namespaces to process
	// then we can just process that list directly
	if it.Query.selection == selected {
		for _, namespace := range it.Query.namespaces {
			it.process(c, mapper, namespace)
		}
		return true, nil
	}

	q := it.createQuery(c)

	var cursor *datastore.Cursor
	if it.Cursor != "" {
		newCursor, err := datastore.DecodeCursor(it.Cursor)
		if err != nil {
			log.Errorf(c, "get start cursor error %s", err.Error())
			return false, err
		}
		cursor = &newCursor
	}

	// main task loop to repeat datastore query with cursor
	for taskRunning {

		// if cursor is set, start the query at that point
		if cursor != nil {
			q = q.Start(*cursor)
		}

		// limit how long the cursor can run before we requery
		cursorTimeout := time.After(mapper.config.CursorTimeout)
		// datastore cursor context needs to run for the max allowed
		cc, _ := context.WithTimeout(c, time.Duration(60)*time.Second)
		t := q.Run(cc)

		// item loop to iterate cursor
	cursorLoop:
		for {
			key, err := t.Next(nil)
			if err == datastore.Done {
				// we reached the end
				return true, nil
			}

			if err != nil {
				log.Errorf(c, "error %s", err.Error())
				return false, err
			}

			namespace := key.StringID()
			if err := it.process(c, mapper, namespace); err != nil {
				return false, err
			}
			it.Count++

			select {
			case <-taskTimeout:
				// clearing the flag breaks us out of the task loop but also lets us update the
				// cursor first when we break from the inner cursorLoop
				taskRunning = false
				break cursorLoop
			default:
				select {
				case <-cursorTimeout:
					// this forces a new cursor and query so we don't suffer from datastore timeouts
					break cursorLoop
				default:
					// no timeout so carry on with the current cursor
					continue cursorLoop
				}
			}
		}

		// we need to get the cursor for where we are upto whether we are requerying
		// within this task or scheduling a new continuation slice
		newCursor, err := t.Cursor()
		if err != nil {
			log.Errorf(c, "get next cursor error %s", err.Error())
			return false, err
		}
		cursor = &newCursor
		it.Cursor = cursor.String()
	}

	return false, nil
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
