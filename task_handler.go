package mapper

import (
	"fmt"

	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

type (
	taskEntity interface {
		taskable
		lockable
	}

	// subTask is any non-job task that needs to check if job is still active
	subTask interface {
		jobID() string
		setJob(job *job)
	}

	// taskHandler is a custom handler type to avoid repetition
	taskHandler func(c context.Context, config Config, key *datastore.Key, entity taskEntity) error
)

func factory(c context.Context, prefix, kind, id string) (*datastore.Key, taskEntity) {
	key := datastore.NewKey(c, prefix+kind, id, 0, nil)
	var entity taskEntity

	switch kind {
	case jobKind:
		entity = new(job)
	case iteratorKind:
		entity = new(iterator)
	case namespaceKind:
		entity = new(namespace)
	case shardKind:
		entity = new(shard)
	}

	return key, entity
}

func (m *mapper) handleTask(path, kind string, handler taskHandler) {
	fn := func(w http.ResponseWriter, r *http.Request) {
		c := appengine.NewContext(r)

		// ensure request is a task request
		if r.Method != "POST" {
			log.Warningf(c, "expected POST, got %s", r.Method)
			return
		}

		// X-AppEngine-QueueName
		// X-AppEngine-TaskName
		// X-AppEngine-TaskRetryCount
		// X-AppEngine-TaskExecutionCount
		// X-AppEngine-TaskETA

		if r.Header.Get("X-Appengine-TaskName") == "" {
			log.Warningf(c, "non task request")
			return
		}

		// get the lock details for this task
		id, seq, queue, _ := ParseLock(r)

		if m.config.LogVerbose {
			log.Infof(c, "path %s kind %s id %s seq %d", r.URL.Path, kind, id, seq)
		}

		// create the key and entity for the lock
		key, entity := factory(c, m.config.DatastorePrefix, kind, id)

		// try to obtain the lock
		if err := GetLock(c, key, entity, seq); err != nil {
			log.Errorf(c, "get lock failed %s", err.Error())
			// for locking errors, the error gives us the response to use
			if serr, ok := err.(LockError); ok {
				w.WriteHeader(serr.Response)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			fmt.Fprintf(w, err.Error())
			return
		}

		entity.setCommon(id, nil, queue)

		// determine if we're a job task (so have already loaded the job)
		// or a sub task (in which case we need to load it) so that we can
		// abort if the flag has been set or set the job so that the handler
		// can access the jobSpec and Query (should we pass though through?)
		var j *job
		sub, isSub := entity.(subTask)
		if isSub {
			jobID := sub.jobID()
			if m.config.LogVerbose {
				log.Infof(c, "owning job %s", jobID)
			}
			// job, err := m.getJob(c, jobID)
			key := datastore.NewKey(c, m.config.DatastorePrefix+jobKind, jobID, 0, nil)
			j = new(job)
			err := storage.Get(c, key, j)
			if err != nil {
				// we need the job so error if we couldn't load it
				log.Errorf(c, "error loading job %s", err.Error)
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprintf(w, err.Error())
				ClearLock(c, key, false)
				return
			}
			sub.setJob(j)
		} else {
			j = entity.(*job)
		}

		if j.Abort {
			// abort means we don't do anything but return OK so the
			// task is marked complete and drains
			log.Warningf(c, "job aborted")
			w.WriteHeader(http.StatusOK)
			ClearLock(c, key, false)
			return
		}

		jobSpec, err := CreateJobInstance(j.JobName)
		if err != nil {
			log.Errorf(c, "error creating job instance %s", err.Error)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, err.Error())
			ClearLock(c, key, false)
			return
		}

		entity.setCommon(id, jobSpec, queue)

		common := entity.getCommon()
		log.Infof(c, "query: %s", common.Query)

		// call the actual handler
		if m.config.LogVerbose {
			log.Infof(c, "calling handler")
		}
		if err := handler(c, *m.config, key, entity); err != nil {
			log.Errorf(c, "task error %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, err.Error())
			ClearLock(c, key, false)
			return
		}

		if m.config.LogVerbose {
			log.Infof(c, "task successful")
		}

		w.WriteHeader(http.StatusOK)
	}

	// register handler with mux
	m.HandleFunc(path, fn)
}
