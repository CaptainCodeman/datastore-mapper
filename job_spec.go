package mapper

import (
	"fmt"
	"io"
	"strconv"

	"hash/adler32"
	"net/http"

	"github.com/captaincodeman/datastore-locker"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

type (
	// JobSpec is the interface use by the mapper to create the datastore query spec
	JobSpec interface {
		// Query creates the datastore query spec to define the entities that the job
		// should process. It is called when a new job is being initiated and passed
		// the request in order to extract any parameters from it that may be required
		Query(r *http.Request) (*Query, error)
		Next(c context.Context, counters Counters, key *datastore.Key) error
	}

	// JobOutput is the interface that a mapper job should implement if it wants to
	// write file output. Files will be created for each slice and rolled up into
	// shards and then namespaces
	JobOutput interface {
		Output(w io.Writer)
	}

	// JobEntity is the interface that a mapper job should implement if it wants
	// to map directly over datastore entities. i.e. *not* use a KeysOnly query.
	// Implementing this interface will cause a full entity query to be performed and
	// the entity will be loaded into whatever this function returns which should be a
	// named field within the job struct.
	// It will be called once at the beginning of any slice processing and the field
	// will not live beyond the slice lifetime.
	JobEntity interface {
		Make() interface{}
	}

	// TODO: batch processing of keys (for GetMulti within job)

	// JobLifecycle is the interface that any mapper job struct can implement to
	// be notified of job lifecycle events. Use this if you want to perform any actions
	// at the beginning and / or end of a job.
	JobLifecycle interface {
		// JobStarted is called when a mapper job is started
		JobStarted(c context.Context, id string)

		// JobStarted is called when a mapper job is completed
		JobCompleted(c context.Context, id string)
	}

	// NamespaceLifecycle is the interface that any mapper job struct can implement to
	// be notified of namespace lifecycle events. Use this is you want to perform any
	// actions at the beginning and / or end of processing for each namespace.
	NamespaceLifecycle interface {
		// NamespaceStarted is called when a mapper job for an individual
		// namespace is started
		NamespaceStarted(c context.Context, id string, namespace string)

		// NamespaceStarted is called when a mapper job for an individual
		// namespace is completed
		NamespaceCompleted(c context.Context, id string, namespace string)
	}

	// ShardLifecycle is the interface that any mapper job struct can implement to
	// be notified of shard lifecycle events. Use this is you want to perform any
	// actions at the beginning and / or end of processing for each shard.
	ShardLifecycle interface {
		// ShardStarted is called when a mapper job for an individual
		// shard within a namespace is started
		ShardStarted(c context.Context, id string, namespace string, shard int)

		// ShardStarted is called when a mapper job for an individual
		// shard within a namespace is completed
		ShardCompleted(c context.Context, id string, namespace string, shard int)
	}

	// SliceLifecycle is the interface that any mapper job struct can implement to
	// be notified of slice lifecycle events. Use this is you want to perform any
	// actions at the beginning and / or end of processing for each slice.
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
	c := appengine.NewContext(r)

	values := r.URL.Query()
	name := values.Get("name")
	jobSpec, err := CreateJobInstance(name)
	if err != nil {
		log.Errorf(c, "failed to create job %s %v", name, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	shards, err := strconv.Atoi(values.Get("shards"))
	if shards == 0 || err != nil {
		shards = m.config.Shards
	}

	queue := values.Get("queue")
	if queue != "" {
		// override the queue for this request
		// (used by locker.Schedule later)
		c = locker.WithQueue(c, queue)
	}
	bucket := values.Get("bucket")

	query, err := jobSpec.Query(r)
	if err != nil {
		log.Errorf(c, "failed to create query %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	requestHash := r.Header.Get("X-Appengine-Request-Id-Hash")
	if requestHash == "" {
		// this should only happen when testing, we just need a short hash
		requestID := appengine.RequestID(c)
		requestHash = strconv.FormatUint(uint64(adler32.Checksum([]byte(requestID))), 16)
	}

	id := fmt.Sprintf("%s/%s", name, requestHash)
	job := &job{
		JobName:   name,
		JobSpec:   jobSpec,
		Bucket:    bucket,
		Shards:    shards,
		Iterating: true,
	}
	job.common.start(query)

	key := datastore.NewKey(c, m.config.DatastorePrefix+jobKind, id, 0, nil)
	if err := m.locker.Schedule(c, key, job, m.config.Path+jobURL, nil); err != nil {
		log.Errorf(c, "error scheduling job %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Debugf(c, "scheduled job %s queue:%s bucket:%s shards:%d", id, queue, bucket, shards)
	w.WriteHeader(http.StatusAccepted)
}
