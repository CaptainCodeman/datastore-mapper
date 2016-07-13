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
	// JobSpec is the interface that any mapreduce job struct needs to implement
	// Each job needs to be registered using RegisterJob
	JobSpec interface {
		// Query creates the datastore query to define the entities that the job
		// should process. It is called when a new job is being started and passed
		// the request in order to extract any parameters that may be required
		Query(r *http.Request) (*Query, error)
	}

	// JobSingle is the interface that any mapper job struct needs to implement
	// for single item processing.
	JobSingle interface {
		// Next will be called to process each key in turn
		Next(c context.Context, w io.Writer, counters Counters, key *datastore.Key) error
	}

	// JobBatched is the interface that any mapper job struct needs to implement
	// for batch item processing.
	JobBatched interface {
		// NextBatch will be called to process each batch of keys in turn
		NextBatch(c context.Context, w io.Writer, counters Counters, keys []*datastore.Key) error
	}

	// JobLifecycle is the interface that any mapper job struct can implement to
	// be notified of job lifecycle events
	JobLifecycle interface {
		// JobStarted is called when a mapper job is started
		JobStarted(c context.Context, id string)

		// JobStarted is called when a mapper job is completed
		JobCompleted(c context.Context, id string)
	}

	// NamespaceLifecycle is the interface that any mapper job struct can implement to
	// be notified of namespace lifecycle events
	NamespaceLifecycle interface {
		// NamespaceStarted is called when a mapper job for an individual
		// namespace is started
		NamespaceStarted(c context.Context, id string, namespace string)

		// NamespaceStarted is called when a mapper job for an individual
		// namespace is completed
		NamespaceCompleted(c context.Context, id string, namespace string)
	}

	// ShardLifecycle is the interface that any mapper job struct can implement to
	// be notified of shard lifecycle events
	ShardLifecycle interface {
		// ShardStarted is called when a mapper job for an individual
		// shard within a namespace is started
		ShardStarted(c context.Context, id string, namespace string, shard int)

		// ShardStarted is called when a mapper job for an individual
		// shard within a namespace is completed
		ShardCompleted(c context.Context, id string, namespace string, shard int)
	}

	// SliceLifecycle is the interface that any mapper job struct can implement to
	// be notified of slice lifecycle events
	SliceLifecycle interface {
		// SliceStarted is called when a mapper job for an individual slice of a
		// shard within a namespace is started
		SliceStarted(c context.Context, id string, namespace string, shard, slice int)

		// SliceStarted is called when a mapper job for an individual slice of a
		// shard within a namespace is completed
		SliceCompleted(c context.Context, id string, namespace string, shard, slice int)
	}
)

func init() {
	server.HandleFunc("/start", server.startJobHandler)
}

// StartJob launches a job on the given queue. It is not executed immediately but
// scheduled to run as a task which performs splitting of the input reader based
// on the number of shards.
func (m *mapper) startJobHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	values := r.URL.Query()
	name := values.Get("name")
	jobSpec, err := CreateJobInstance(name)
	if err != nil {
		return
		// return err
	}

	shards, err := strconv.Atoi(values.Get("shards"))
	if shards == 0 || err != nil {
		shards = m.config.Shards
	}
	queue := values.Get("queue")
	if queue == "" {
		queue = m.config.Queue
	}
	bucket := values.Get("bucket")

	query, _ := jobSpec.Query(r)

	c := appengine.NewContext(r)
	requestHash := r.Header.Get("X-Appengine-Request-Id-Hash")
	if requestHash == "" {
		// this should only happen when testing, we just need a short hash
		requestID := appengine.RequestID(c)
		requestHash = strconv.FormatUint(uint64(adler32.Checksum([]byte(requestID))), 16)
	}

	id := fmt.Sprintf("%s-%s", strings.Replace(name, ".", "", 1), requestHash)
	job := &job{
		Job:       jobSpec,
		Query:     query,
		Bucket:    bucket,
		Shards:    shards,
		Iterating: true,
	}
	job.common.start()

	key := datastore.NewKey(c, m.config.DatastorePrefix+jobKind, id, 0, nil)
	ScheduleLock(c, key, job, m.config.Path+jobURL, nil, queue)
}
