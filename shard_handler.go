package mapper

import (
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

const (
	shardURL         = "/shard"
	shardCompleteURL = shardURL + "/complete"
)

func init() {
	Server.HandleFunc(shardURL, shardHandler)
	Server.HandleFunc(shardCompleteURL, shardCompleteHandler)
}

func shardHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	c := appengine.NewContext(r)

	id, seq, queue, _ := ParseLock(r)
	log.Infof(c, "shard %s %d", id, seq)

	k := datastore.NewKey(c, config.DatastorePrefix+shardKind, id, 0, nil)
	s := new(shardState)

	if err := GetLock(c, k, s, seq); err != nil {
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

	s.id = id
	s.queue = queue

	j, err := getJob(c, s.jobID())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		ClearLock(c, k, s, false)
		return
	}

	if j.Abort {
		w.WriteHeader(http.StatusOK)
		return
	}

	s.job = j

	completed, err := s.iterate(c)
	if err != nil {
		// this will cause a task retry
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		ClearLock(c, k, s, false)
		return
	}

	var url string
	if completed {
		// schedule completion
		url = config.BasePath + shardCompleteURL
	} else {
		// schedule continuation
		url = config.BasePath + shardURL
	}
	_, err = ScheduleLock(c, k, s, url, nil, queue)
	if err != nil {
		// this will cause a task retry
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		ClearLock(c, k, s, false)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func shardCompleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	c := appengine.NewContext(r)

	id, seq, queue, _ := ParseLock(r)
	log.Infof(c, "shard %s %d", id, seq)

	k := datastore.NewKey(c, config.DatastorePrefix+shardKind, id, 0, nil)
	s := new(shardState)

	if err := GetLock(c, k, s, seq); err != nil {
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

	s.id = id
	s.queue = queue

	j, err := getJob(c, s.jobID())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		ClearLock(c, k, s, false)
		return
	}

	if j.Abort {
		w.WriteHeader(http.StatusOK)
		return
	}

	s.job = j

	if err := s.rollup(c); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		log.Errorf(c, "error %s", err.Error())
		ClearLock(c, k, s, false)
		return
	}
	s.complete()
	s.Cursor = ""
	s.RequestID = ""

	// update shard status and owning namespace within a transaction
	sk := datastore.NewKey(c, config.DatastorePrefix+namespaceKind, s.namespaceID(), 0, nil)
	sp := new(namespaceState)
	err = storage.RunInTransaction(c, func(tc context.Context) error {
		if err := storage.Get(tc, sk, sp); err != nil {
			return err
		}

		sp.ShardsSuccessful++
		sp.common.rollup(s.common)

		if _, err := storage.Put(tc, k, s); err != nil {
			return err
		}
		if _, err := storage.Put(tc, sk, sp); err != nil {
			return err
		}

		// if all shards have completed, schedule namespace/completed to update job
		if sp.ShardsSuccessful == sp.ShardsTotal {
			t := NewLockTask(sk, sp, config.BasePath+namespaceCompleteURL, nil)
			if _, err := storage.Put(tc, sk, sp); err != nil {
				return err
			}
			if _, err := taskqueue.Add(tc, t, queue); err != nil {
				return err
			}
		}

		return nil
	}, &datastore.TransactionOptions{XG: true, Attempts: attempts})

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		ClearLock(c, k, s, false)
		return
	}

	w.WriteHeader(http.StatusOK)
}
