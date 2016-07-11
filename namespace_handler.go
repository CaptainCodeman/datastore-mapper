package mapper

import (
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

const (
	namespaceURL         = "/namespace"
	namespaceCompleteURL = "/namespace/complete"
)

func init() {
	Server.HandleFunc(namespaceURL, namespaceHandler)
	Server.HandleFunc(namespaceCompleteURL, namespaceCompleteHandler)
}

func namespaceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	c := appengine.NewContext(r)

	id, seq, queue, _ := ParseLock(r)
	log.Infof(c, "namespace %s %d", id, seq)

	k := datastore.NewKey(c, config.DatastorePrefix+namespaceKind, id, 0, nil)
	ns := new(namespaceState)

	if err := GetLock(c, k, ns, seq); err != nil {
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

	ns.id = id
	ns.queue = queue

	j, err := getJob(c, ns.jobID())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Errorf(c, "error %s", err.Error())
		ClearLock(c, k, ns, false)
		return
	}

	if j.Abort {
		w.WriteHeader(http.StatusOK)
		return
	}

	ns.job = j

	err = ns.split(c)
	if err != nil {
		// this will cause a task retry
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Errorf(c, "error %s", err.Error())
		ClearLock(c, k, ns, false)
		return
	}

	// TODO: create task to update iterator / job
	if _, err := storage.Put(c, k, ns); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Errorf(c, "error %s", err.Error())
		ClearLock(c, k, ns, false)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func namespaceCompleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	c := appengine.NewContext(r)

	id, seq, queue, _ := ParseLock(r)
	log.Infof(c, "namespace complete %s %d", id, seq)

	k := datastore.NewKey(c, config.DatastorePrefix+namespaceKind, id, 0, nil)
	ns := new(namespaceState)

	if err := GetLock(c, k, ns, seq); err != nil {
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

	ns.id = id
	ns.queue = queue

	j, err := getJob(c, ns.jobID())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Errorf(c, "error %s", err.Error())
		ClearLock(c, k, ns, false)
		return
	}

	if j.Abort {
		w.WriteHeader(http.StatusOK)
		return
	}

	ns.job = j

	if err := ns.rollup(c); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Errorf(c, "error %s", err.Error())
		ClearLock(c, k, ns, false)
		return
	}

	ns.complete()
	ns.RequestID = ""

	// update namespace status and job within a transaction
	jk := datastore.NewKey(c, config.DatastorePrefix+jobKind, ns.jobID(), 0, nil)
	err = storage.RunInTransaction(c, func(tc context.Context) error {
		if err := storage.Get(tc, jk, j); err != nil {
			return err
		}
		if j.Abort {
			return nil
		}

		j.NamespacesSuccessful++
		j.common.rollup(ns.common)

		// TODO: schedule a job completion for symetry and final cleanup, notification etc... ?
		if j.NamespacesSuccessful == j.NamespacesTotal && !j.Iterating {
			j.complete()
			j.RequestID = ""
		}

		if _, err := storage.Put(tc, k, ns); err != nil {
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
		log.Errorf(c, "error %s", err.Error())
		ClearLock(c, k, ns, false)
		return
	}

	w.WriteHeader(http.StatusOK)
}
