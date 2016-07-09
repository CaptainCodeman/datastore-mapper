package mapper

import (
	"strconv"

	"hash/adler32"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/taskqueue"
)

type (
	// Job is the interface that any mapreduce job struct needs to implement
	// Each job needs to be registered using RegisterJob
	Job interface {
		// Query creates the datastore query to define the entities that the job
		// should process. It is called when a new job is being started and passed
		// the request in order to extract any parameters that may be required
		Query(r *http.Request) (*Query, error)
	}

	JobSingle interface {
		// Next will be called to process each item
		Single(c context.Context, counters Counters, key *datastore.Key) error
	}

	JobBatched interface {
		// Next will be called to process each batch
		Batch(c context.Context, counters Counters, keys []*datastore.Key) error
	}

	// JobLifecycle is the interface that any mapper job struct can implement to
	// be notified of job lifecycle events
	JobLifecycle interface {
		JobStarted(c context.Context)
		JobCompleted(c context.Context)
	}

	// ShardLifecycle is the interface that any mapper job struct can implement to
	// be notified of shard lifecycle events
	ShardLifecycle interface {
		ShardStarted(c context.Context)
		ShardCompleted(c context.Context)
	}

	// SliceLifecycle is the interface that any mapper job struct can implement to
	// be notified of slice lifecycle events
	SliceLifecycle interface {
		SliceStarted(c context.Context)
		SliceCompleted(c context.Context)
	}
)

// StartJob launches a job on the given queue. It is not executed immediately but
// scheduled to run as a task which performs splitting of the input reader based
// on the number of shards. Prefix should be a unique identifier, typically the
// value of the X-Appengine-Request-Id-Hash request header. All tasks will execute
// on the queue specified or the default set in config (or the default queue if
// that is also empty).
//
// Ideally we'd pass request into here instead but we kind of need it to be context
// for testing purposes so ...
func StartJob(c context.Context, job Job, query *Query, queue string, shards int) error {
	requestID := appengine.RequestID(c)
	prefix := strconv.FormatUint(uint64(adler32.Checksum([]byte(requestID))), 16)

	ns := new(namespaceIterator)
	ns.Active = true
	ns.Job = job
	ns.Query = query

	key := datastore.NewKey(c, config.DatastorePrefix+namespaceIteratorKind, prefix, 0, nil)

	t := taskqueue.NewPOSTTask(config.BasePath+namespaceIteratorURL, nil)
	if _, err := ScheduleLock(c, key, ns, t, queue); err != nil {
		return err
	}
	return nil
}
