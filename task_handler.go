package mapper

import (
	"net/http"

	"github.com/captaincodeman/datastore-locker"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

type (
	// interface that all our entities implement
	taskEntity interface {
		locker.Lockable
		getCommon() *common
	}

	// childTask is any non-job task that needs to check if job is still active
	childTask interface {
		jobID() string
		setJob(job *job)
	}

	// taskHandler is a custom handler type to avoid repetition
	taskHandler func(c context.Context, config Config, key *datastore.Key, entity taskEntity) error
)

func jobFactory() locker.Lockable {
	return new(job)
}

func iteratorFactory() locker.Lockable {
	return new(iterator)
}

func namespaceFactory() locker.Lockable {
	return new(namespace)
}

func shardFactory() locker.Lockable {
	return new(shard)
}

// convert the locker.TaskHandler
func (m *mapper) handlerAdapter(handler taskHandler, factory locker.EntityFactory) http.Handler {
	fn := func(c context.Context, r *http.Request, key *datastore.Key, entity locker.Lockable) error {
		tentity := entity.(taskEntity)
		common := tentity.getCommon()
		common.id = key.StringID()

		// determine if we're a job task (so have already loaded the job)
		// or a sub task (in which case we need to load it) so that we can
		// abort if the flag has been set or set the job so that the handler
		// can access the jobSpec and Query (should we pass though through?)
		var j *job
		child, isChild := entity.(childTask)
		if isChild {
			jobID := child.jobID()
			if m.config.LogVerbose {
				log.Infof(c, "owning job %s", jobID)
			}
			key := datastore.NewKey(c, m.config.DatastorePrefix+jobKind, jobID, 0, nil)
			j = new(job)
			if err := storage.Get(c, key, j); err != nil {
				// we need the job so error if we couldn't load it
				return err
			}
			common.job = j
		} else {
			j = entity.(*job)
		}

		if j.Abort {
			// abort means we don't process anything
			// the task is marked complete and drains
			return nil
		}

		jobSpec, err := CreateJobInstance(j.JobName)
		if err != nil {
			log.Errorf(c, "error creating job instance %v", err)
			return err
		}

		common.jobSpec = jobSpec

		// call the actual handler
		if m.config.LogVerbose {
			log.Infof(c, "calling handler")
		}

		return handler(c, *m.config, key, tentity)
	}

	return http.Handler(m.locker.Handle(fn, factory))
}
