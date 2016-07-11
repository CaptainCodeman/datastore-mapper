package mapper

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"hash/adler32"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
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
		Single(c context.Context, w io.Writer, counters Counters, key *datastore.Key) error
	}

	JobBatched interface {
		// Next will be called to process each batch
		Batch(c context.Context, w io.Writer, counters Counters, keys []*datastore.Key) error
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
// on the number of shards.
func StartJob(r *http.Request) error {
	values := r.URL.Query()
	name := values.Get("name")
	job, err := CreateJobInstance(name)
	if err != nil {
		return err
	}

	shards, err := strconv.Atoi(values.Get("shards"))
	if shards == 0 || err != nil {
		shards = config.ShardCount
	}
	queue := values.Get("queue")
	if queue == "" {
		queue = config.Queue
	}
	bucket := values.Get("bucket")

	query, _ := job.Query(r)

	c := appengine.NewContext(r)
	requestHash := r.Header.Get("X-Appengine-Request-Id-Hash")
	if requestHash == "" {
		// this should only happen when testing, we just need a short hash
		requestID := appengine.RequestID(c)
		requestHash = strconv.FormatUint(uint64(adler32.Checksum([]byte(requestID))), 16)
	}

	id := fmt.Sprintf("%s-%s", strings.Replace(name, ".", "", 1), requestHash)
	state := &jobState{
		Job:       job,
		Query:     query,
		Bucket:    bucket,
		Shards:    shards,
		Iterating: true,
	}
	state.common.start()

	k := datastore.NewKey(c, config.DatastorePrefix+jobKind, id, 0, nil)
	if _, err := ScheduleLock(c, k, state, config.BasePath+jobURL, nil, queue); err != nil {
		return err
	}
	return nil
}
