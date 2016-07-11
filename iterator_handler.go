package mapper

import (
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

const (
	iteratorURL         = "/iterate"
	iteratorCompleteURL = iteratorURL + "/complete"
)

func init() {
	Server.HandleFunc(iteratorURL, iteratorHandler)
	Server.HandleFunc(iteratorCompleteURL, iteratorCompleteHandler)
}

func iteratorHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	c := appengine.NewContext(r)

	id, seq, queue, _ := ParseLock(r)
	log.Infof(c, "iterator %s %d", id, seq)

	k := datastore.NewKey(c, config.DatastorePrefix+iteratorKind, id, 0, nil)
	it := new(iterator)

	if err := GetLock(c, k, it, seq); err != nil {
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

	it.id = id
	it.queue = queue

	j, err := getJob(c, id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		ClearLock(c, k, it, false)
		log.Errorf(c, "error %s", err.Error())
		return
	}

	if j.Abort {
		w.WriteHeader(http.StatusOK)
		return
	}

	it.job = j

	completed, err := it.iterate(c)
	if err != nil {
		// this will cause a task retry
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		ClearLock(c, k, it, false)
		log.Errorf(c, "error %s", err.Error())
		return
	}

	var url string
	if completed {
		// schedule completion
		url = config.BasePath + iteratorCompleteURL
	} else {
		// schedule continuation
		url = config.BasePath + iteratorURL
	}
	_, err = ScheduleLock(c, k, it, url, nil, queue)
	if err != nil {
		// this will cause a task retry
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		ClearLock(c, k, it, false)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func iteratorCompleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	c := appengine.NewContext(r)

	id, seq, queue, _ := ParseLock(r)
	log.Infof(c, "iterator complete %s %d", id, seq)

	k := datastore.NewKey(c, config.DatastorePrefix+iteratorKind, id, 0, nil)
	it := new(iterator)

	if err := GetLock(c, k, it, seq); err != nil {
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

	it.id = id
	it.queue = queue

	// mark iterator as complete
	it.complete()
	it.RequestID = ""

	// update iterator status and job within a transaction
	jk := datastore.NewKey(c, config.DatastorePrefix+jobKind, id, 0, nil)
	j := new(jobState)
	err := storage.RunInTransaction(c, func(tc context.Context) error {
		if err := storage.Get(tc, jk, j); err != nil {
			return err
		}
		if j.Abort {
			return nil
		}

		j.NamespacesTotal += int(it.Count)
		j.Iterating = false
		// only the iterator walltime is rolled up into the job counts
		j.WallTime += it.WallTime

		if _, err := storage.Put(tc, k, it); err != nil {
			return err
		}
		if _, err := storage.Put(tc, jk, j); err != nil {
			return err
		}
		return nil
	}, &datastore.TransactionOptions{XG: true, Attempts: attempts})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		ClearLock(c, k, it, false)
		log.Errorf(c, "error %s", err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}