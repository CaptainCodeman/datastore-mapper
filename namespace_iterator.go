package mapper

import (
	"bytes"
	"fmt"
	"time"

	"encoding/gob"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

type (
	// namespaceIterator processes namespaces for a job
	namespaceIterator struct {
		// Lock ensures single atomic operation
		Lock

		// Job is the job processor type
		Job Job `datastore:"-"`

		// Namespace is the namespace to begin iterating from
		Namespace string `datastore:"namespace,noindex"`

		// Active indicated if this iterator is active
		Active bool `datastore:"active,noindex"`

		// Count is the number of namespaces
		Count int64 `datastore:"count,noindex"`

		// Query provides the source to process
		Query *Query `datastore:"-"`

		// private fields used by local instance
		job   string
		queue string
	}
)

const (
	namespaceIteratorKind = "namespace_iterator"
	namespaceIteratorURL  = "/namespace-iterate"
	namespaceKind         = "__namespace__"
)

func init() {
	Server.HandleFunc(namespaceIteratorURL, namespaceIteratorHandler)
}

func namespaceIteratorHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	id, seq, queue, _ := ParseLock(r)

	c := appengine.NewContext(r)
	key := datastore.NewKey(c, config.DatastorePrefix+namespaceIteratorKind, id, 0, nil)
	ns := new(namespaceIterator)

	if err := GetLock(c, key, ns, seq); err != nil {
		if serr, ok := err.(*LockError); ok {
			// for locking errors, the error gives us the response to use
			w.WriteHeader(serr.Response)
			w.Write([]byte(serr.Error()))
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}
		log.Errorf(c, "error %s", err.Error())
		return
	}

	ns.job = id
	ns.queue = queue

	completed, err := ns.iterate(c)
	if err != nil {
		// this will cause a task retry
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	if completed {
		// all done !
		ns.Active = false
		if _, err := storage.Put(c, key, ns); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	// schedule continuation
	t := taskqueue.NewPOSTTask(config.BasePath+namespaceIteratorURL, nil)
	ScheduleLock(c, key, ns, t, queue)
}

func (n *namespaceIterator) process(c context.Context) error {
	// process the current namespace - split the query and create shards
	log.Debugf(c, "process %s", n.Namespace)

	n.Count++

	// the splitter should be idempotent so no need for datastore lock
	// but just in case we need any counters or other state we'll use it

	id := fmt.Sprintf("%s-%s", n.job, n.Namespace)
	key := datastore.NewKey(c, config.DatastorePrefix+namespaceSplitterKind, id, 0, nil)

	ns := new(namespaceSplitter)
	ns.Namespace = n.Namespace
	ns.Query = n.Query
	ns.Job = n.Job

	t := taskqueue.NewPOSTTask(config.BasePath+namespaceSplitterURL, nil)
	_, err := ScheduleLock(c, key, ns, t, n.queue)
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

func (n *namespaceIterator) createQuery(c context.Context) *datastore.Query {
	q := datastore.NewQuery(namespaceKind)
	if n.Namespace != "" {
		q = q.Filter("__key__ >=", datastore.NewKey(c, namespaceKind, n.Namespace, 0, nil))
	}
	q = q.Order("__key__")
	q = q.KeysOnly()
	return q
}

func (n *namespaceIterator) iterate(c context.Context) (bool, error) {
	// use the full 10 minutes allowed (assuming front-end instance type)
	c, _ = context.WithTimeout(c, time.Duration(10)*time.Minute)

	// if the query defines the specific namespaces to process then we
	// don't really need to query for them - we can just process that
	// list immediately
	if len(n.Query.namespace) > 0 {
		for _, namespace := range n.Query.namespace {
			n.Namespace = namespace
			n.process(c)
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
		q := n.createQuery(c).Limit(size)
		it := q.Run(c)
		for {
			key, err := it.Next(nil)
			if err == datastore.Done {
				break
			}
			if err != nil {
				log.Errorf(c, "error %s", err.Error())
				return false, err
			}

			n.Namespace = key.StringID()
			if err := n.process(c); err != nil {
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
func (n *namespaceIterator) Load(props []datastore.Property) error {
	datastore.LoadStruct(n, props)

	for _, prop := range props {
		switch prop.Name {
		case "job":
			payload := bytes.NewBuffer(prop.Value.([]byte))
			enc := gob.NewDecoder(payload)
			if err := enc.Decode(&n.Job); err != nil {
				return err
			}
		case "query":
			n.Query = &Query{}
			if err := n.Query.GobDecode(prop.Value.([]byte)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *namespaceIterator) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(n)

	payload := new(bytes.Buffer)
	enc := gob.NewEncoder(payload)
	if err := enc.Encode(&n.Job); err != nil {
		return nil, err
	}
	props = append(props, datastore.Property{Name: "job", Value: payload.Bytes(), NoIndex: true, Multiple: false})

	b, err := n.Query.GobEncode()
	if err != nil {
		return nil, err
	}
	props = append(props, datastore.Property{Name: "query", Value: b, NoIndex: true, Multiple: false})

	return props, err
}
