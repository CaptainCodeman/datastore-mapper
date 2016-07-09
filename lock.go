package mapper

/*
Provides a lease mechanism to prevent duplicate execution of a task by
coordinating state stored in the task with the datastore / memcache so
the logic can be easily re-used across different entity types.
*/

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"math/rand"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

type (
	// Lock is used to protect sequential tasks from being re-executed due to the
	// at-least-once semantics of the task queue. It will detect situations where
	// a task has died *without* clearing the lock so that it can timeout without
	// blocking for an extended period of time or causing a deadlock.
	//
	// This Lock struct should be embedded within the entity and the Sequence added
	// to the task header
	Lock struct {
		// Timestamp is the time that this lock was written
		Timestamp time.Time `datastore:"ts"`

		// Request is the request id that obtained the current lock
		RequestID string `datastore:"req"`

		// Sequence is the task sequence number
		Sequence int `datastore:"seq"`

		// Retries is the number of retries that have been attempted
		Retries int `datastore:"retries"`
	}

	// lockable is the interface that lockable entities should implement
	// they will do this automatically simply by embedding Lock in the struct
	// This is used to ensure than entities we deal with have a Lock field
	lockable interface {
		lock() *Lock
	}

	// LockError is a custom error that includes the recommended http response
	LockError struct {
		Response int
		text     string
	}

	// TODO: pass in to function if additional datastore writes need to take
	// part in the same transaction
	extra func(c context.Context)
)

var (
	// we return the http status code for callers to return as a convenience
	// and to help ensure the logic is correct. Using ServiceUnavailable (503)
	// causes a task retry without flooding the logs with visible errors. The
	// OK (200) response will cause the task to be dropped as 'successful' so
	// is used if the task needs to be cancelled

	// ErrLockFailed signals that a lock attempt failed and the task trying to
	// aquire it should be retried
	ErrLockFailed = LockError{http.StatusServiceUnavailable, "lock failed (retry)"}

	// ErrAlreadyLocked signals that another task has already aquired the lock
	// so the task should be aborted
	ErrAlreadyLocked = LockError{http.StatusOK, "already locked (abandon)"}
)

const (
	// attempts is the number of transaction retries to attempt
	attempts = 10

	// Once a lock has been held longer than this duration the logs API
	// will be checked to determine if the request has completed or not
	leaseDuration = time.Duration(1) * time.Minute

	// On rare occassions entries may be missing from the logs so if a
	// lock has been held for more than this duration we assume that the
	// task has died. 10 mins is the task timeout on a frontend instance
	leaseTimeout = time.Duration(10)*time.Minute + time.Duration(30)*time.Second
)

func (e LockError) Error() string {
	return fmt.Sprintf("%s", e.text)
}

func (lock *Lock) lock() *Lock {
	return lock
}

// ParseLock returns the queue name and sequence from a task request
func ParseLock(r *http.Request) (id string, seq int, queue string, err error) {
	id = r.Header.Get("X-Mapper-ID")
	queue = r.Header.Get("X-AppEngine-QueueName")
	seq, err = strconv.Atoi(r.Header.Get("X-Mapper-Sequence"))
	return
}

// ScheduleLock schedules a task with lock
func ScheduleLock(c context.Context, key *datastore.Key, entity lockable, task *taskqueue.Task, queueName string) (*taskqueue.Task, error) {
	requestID := appengine.RequestID(c)

	lock := entity.lock()
	lock.Timestamp = getTime()
	lock.RequestID = ""
	lock.Retries = 0
	lock.Sequence++

	// set unique but predictable task name based on entity type, id/name and sequence
	entityType := reflect.TypeOf(entity).Elem()
	entityName := strings.Replace(entityType.String(), ".", "-", 1)
	if key.IntID() == 0 {
		task.Name = fmt.Sprintf(config.TaskPrefix+"%s-%s-%d", entityName, key.StringID(), lock.Sequence)
		task.Header.Set("X-Mapper-ID", key.StringID())
	} else {
		task.Name = fmt.Sprintf(config.TaskPrefix+"%s-%d-%d", entityName, key.IntID(), lock.Sequence)
		task.Header.Set("X-Mapper-ID", strconv.FormatInt(key.IntID(), 10))
	}
	// we could get this from the name, but it's nice to be explicit
	task.Header.Set("X-Mapper-Sequence", strconv.Itoa(lock.Sequence))

	log.Debugf(c, "Schedule %s %d %s %s from %s", key.String(), lock.Sequence, task.Name, queueName, requestID)

	// this is tricky because we want to write our entity and also
	// schedule a task which is normally easy to do transacitonally ...
	// except that isn't allowed when using named tasks so instead
	// we write the entity and fail the transaction if the task
	// scheduling part fails which should semantically be the same
	err := storage.RunInTransaction(c, func(tc context.Context) error {
		if _, err := storage.Put(tc, key, entity); err != nil {
			log.Errorf(c, "putting entity %s", err.Error())
			return err
		}
		// transactional enqueue requires tasks with no name so we
		// need to use the context from *outside* of the transaction
		_, err := taskqueue.Add(c, task, queueName)
		if err == taskqueue.ErrTaskAlreadyAdded {
			// duplicate tasks are ok, just ignore them
			log.Warningf(c, "duplicate task")
			return nil
		}
		if err != nil {
			log.Errorf(c, "scheduling task %s", err.Error())
			// for actual errors we need to retry but add a
			// random delay to avoid contention.
			randomDelay()
			return err
		}
		return nil
	}, &datastore.TransactionOptions{XG: true, Attempts: attempts})

	return task, err
}

// GetLock attempts to get and lock an entity with the given identifier
// If successful it will write a new Lock entity to the datastore
// and return nil, otherwise it will return an error to indicate
// the reason for failure.
func GetLock(c context.Context, key *datastore.Key, entity lockable, sequence int) error {
	requestID := appengine.RequestID(c)
	success := false

	log.Debugf(c, "GetLock %s %d from %s", key.String(), sequence, requestID)

	// we need to run in a transaction for consistency guarantees
	// in case two tasks start at the exact same moment and each
	// of them sees no lock in place
	err := storage.RunInTransaction(c, func(tc context.Context) error {
		err := storage.Get(tc, key, entity)
		if err == datastore.ErrNoSuchEntity {
			// if the entity isn't there, something is wrong - it needs to
			// be scheduled first - the only likely explanations are that the
			// datastore entity has been deleted or the request hasn't really
			// come from the task queue. This shouldn't really happen
			// TODO: warn / panic / return error to drop task ...
			log.Errorf(c, "entity not found")
			return nil
		}
		if err != nil {
			log.Errorf(c, "getting entity %s", err.Error())
			// for actual datastore errors we need to retry but add a
			// random delay to avoid contention.
			randomDelay()
			return err
		}

		// is the lock available?
		lock := entity.lock()
		if lock.RequestID == "" && lock.Sequence == sequence {
			log.Debugf(c, "lock available")
			lock.Timestamp = getTime()
			lock.RequestID = requestID
			if _, err := storage.Put(tc, key, entity); err != nil {
				log.Errorf(c, "putting entity %s", err.Error())
				return err
			}
			success = true
			return nil
		}

		// lock already exists, return nil because there is no point doing
		// any more retries but we'll need to figure out if we can take it
		return nil
	}, &datastore.TransactionOptions{XG: false, Attempts: attempts})

	// if there was any error then we failed to get the lock but don't
	// really know why - returning an error indicates to the caller that
	// they should mark the task as failed so it will be re-attempted
	if err != nil {
		log.Errorf(c, "lock failed %s", err.Error())
		return ErrLockFailed
	}

	// success is true if we got the lock
	if success {
		return nil
	}

	// If there wasn't any error but we weren't successful then a lock is
	// already in place. We're most likely here because a duplicate task has
	// been scheduled (at least once delivery) so we need to examine the lock
	lock := entity.lock()
	log.Debugf(c, "lock %v %d %d %s", lock.Timestamp, lock.Sequence, lock.Retries, lock.RequestID)

	// if the lock sequence is already past this task so it should be dropped
	if lock.Sequence > sequence {
		log.Errorf(c, "lock superseded")
		return ErrAlreadyLocked
	}

	// if the lock is within the lease duration we return that it's locked so
	// that it will be retried
	if lock.Timestamp.Add(leaseDuration).After(getTime()) {
		log.Errorf(c, "lock leased")
		return ErrAlreadyLocked
	}

	// if the lock has been held for longer than the lease duration then we
	// start querying the logs api to see if the previous request completed.
	// if it has then we will be overwriting the lock. It's possible that the
	// log entry is missing or we simply don't have access to them (managed VM)
	// so the lease timeout is a failsafe to catch extreme undetectable failures
	if lock.Timestamp.Add(leaseTimeout).Before(getTime()) && previousRequestEnded(c, lock.RequestID) {
		if err := overwriteLock(c, key, entity, requestID); err == nil {
			// success (at least we grabbed the lock)
			return nil
		}
	}

	return ErrAlreadyLocked
}

// ClearLock clears the current lease, it should be called at the end of every task
// execution, even if things fail, to try and prevent unecessary locks and to count
// the number of retries
func ClearLock(c context.Context, key *datastore.Key, entity lockable, retry bool) error {
	log.Debugf(c, "ClearLock %s %t", key.String(), retry)
	err := storage.RunInTransaction(c, func(tc context.Context) error {
		if err := storage.Get(tc, key, entity); err != nil {
			log.Errorf(c, "get entity failed %s", err.Error())
			return err
		}
		lock := entity.lock()
		lock.Timestamp = getTime()
		lock.RequestID = ""
		if retry {
			lock.Retries++
		}
		if _, err := storage.Put(tc, key, entity); err != nil {
			log.Errorf(c, "put entity failed %s", err.Error())
			return err
		}
		return nil
	}, &datastore.TransactionOptions{XG: false, Attempts: attempts})
	return err
}

// overwrite the current lock
func overwriteLock(c context.Context, key *datastore.Key, entity lockable, requestID string) error {
	log.Debugf(c, "overwriteLock %s %s", key.String(), requestID)
	err := storage.RunInTransaction(c, func(tc context.Context) error {
		if err := storage.Get(tc, key, entity); err != nil {
			log.Errorf(c, "get entity failed %s", err.Error())
			return err
		}
		lock := entity.lock()
		lock.Timestamp = getTime()
		lock.RequestID = requestID
		if _, err := storage.Put(tc, key, entity); err != nil {
			log.Errorf(c, "put entity failed %s", err.Error())
			return err
		}
		return nil
	}, &datastore.TransactionOptions{XG: false, Attempts: attempts})
	return err
}

// determine whether old request has ended according to logs
func previousRequestEnded(c context.Context, requestID string) bool {
	q := &log.Query{
		RequestIDs: []string{requestID},
	}
	results := q.Run(c)
	record, err := results.Next()
	if err == log.Done {
		// no record found so it hasn't ended
		log.Warningf(c, "no log entry for %s", requestID)
		return false
	}
	if err != nil {
		// Managed VMs do not have access to the logservice API
		log.Warningf(c, "err getting log for %s %v", requestID, err)
		return false
	}
	// log.Debugf(c, "log record %v", record)
	return record.Finished
}

func randomDelay() {
	d := time.Duration(rand.Int63n(4)+1) * time.Second
	time.Sleep(d)
}
