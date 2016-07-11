package mapper

/*
Provides a lease mechanism to prevent duplicate execution of a task by
coordinating state stored in the task with the datastore / memcache so
the logic can be easily re-used across different entity types.
*/

import (
	"fmt"
	"strconv"
	"time"

	"math/rand"
	"net/http"
	"net/url"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

type (
	// lock is used to protect sequential tasks from being re-executed due to the
	// at-least-once semantics of the task queue. It will detect situations where
	// a task has died *without* clearing the lock so that it can timeout without
	// blocking for an extended period of time or causing a deadlock.
	//
	// This lock struct should be embedded within the entity and the Sequence added
	// to the task header
	lock struct {
		// Timestamp is the time that this lock was written
		Timestamp time.Time `datastore:"lock_timestamp"`

		// Request is the request id that obtained the current lock
		RequestID string `datastore:"lock_request"`

		// Sequence is the task sequence number
		Sequence int `datastore:"lock_seq"`

		// Retries is the number of retries that have been attempted
		Retries int `datastore:"lock_retries"`
	}

	// lockable is the interface that lockable entities should implement
	// they will do this automatically simply by embedding lock in the struct
	// This is used to ensure than entities we deal with have a lock field
	lockable interface {
		getlock() *lock
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

func (lock *lock) getlock() *lock {
	return lock
}

// ParseLock returns the queue name and sequence from a task request
func ParseLock(r *http.Request) (id string, seq int, queue string, err error) {
	id = r.Header.Get("X-Mapper-ID")
	queue = r.Header.Get("X-AppEngine-QueueName")
	seq, err = strconv.Atoi(r.Header.Get("X-Mapper-Sequence"))
	return
}

func NewLockTask(key *datastore.Key, entity lockable, path string, params url.Values) *taskqueue.Task {
	lock := entity.getlock()
	lock.Timestamp = getTime()
	lock.RequestID = ""
	lock.Retries = 0
	lock.Sequence++

	// set task headers so that we can retrieve the matching entity
	// and check that the executing task is the one we're expecting
	var id string
	if key.IntID() == 0 {
		id = key.StringID()
	} else {
		id = strconv.FormatInt(key.IntID(), 10)
	}

	task := taskqueue.NewPOSTTask(path, params)
	task.Header.Set("X-Mapper-ID", id)
	task.Header.Set("X-Mapper-Sequence", strconv.Itoa(lock.Sequence))

	return task
}

// ScheduleLock schedules a task with lock
func ScheduleLock(c context.Context, key *datastore.Key, entity lockable, path string, params url.Values, queue string) (*taskqueue.Task, error) {
	task := NewLockTask(key, entity, path, params)

	// we write the datastore entity and schedule the task within a
	// transaction which guarantees that both happen and the entity
	// will be committed to the datastore when the task executes but
	// the task won't be scheduled if our entity update loses out
	err := storage.RunInTransaction(c, func(tc context.Context) error {
		if _, err := storage.Put(tc, key, entity); err != nil {
			// returning the err will cause a transaction attempt
			// add a random delay to avoid contention
			randomDelay()
			return err
		}
		if _, err := taskqueue.Add(tc, task, queue); err != nil {
			// returning the err will cause a transaction attempt
			// add a random delay to avoid contention
			randomDelay()
			return err
		}
		return nil
	}, &datastore.TransactionOptions{XG: true, Attempts: attempts})

	return task, err
}

// GetLock attempts to get and lock an entity with the given identifier
// If successful it will write a new lock entity to the datastore
// and return nil, otherwise it will return an error to indicate
// the reason for failure.
func GetLock(c context.Context, key *datastore.Key, entity lockable, sequence int) error {
	requestID := appengine.RequestID(c)
	success := false

	// we need to run in a transaction for consistency guarantees
	// in case two tasks start at the exact same moment and each
	// of them sees no lock in place
	err := storage.RunInTransaction(c, func(tc context.Context) error {
		// reset flag here in case of transaction retries
		success = false

		if err := storage.Get(tc, key, entity); err != nil {
			// returning the err will cause a transaction attempt
			// add a random delay to avoid contention
			randomDelay()
			return err
		}
		// we got the entity successfully, check if it's locked
		// and try to claim the lease if it isn't
		lock := entity.getlock()
		if lock.RequestID == "" && lock.Sequence == sequence {
			lock.Timestamp = getTime()
			lock.RequestID = requestID
			if _, err := storage.Put(tc, key, entity); err != nil {
				return err
			}
			success = true
			return nil
		}

		// lock already exists, return nil because there is no point doing
		// any more retries but we'll need to figure out if we can claim it
		return nil
	}, &datastore.TransactionOptions{XG: false, Attempts: attempts})

	// if there was any error then we failed to get the lock due to datastore
	// errors. Returning an error indicates to the caller that they should mark
	// the task as failed so it will be re-attempted
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
	// been scheduled or executed so we need to examine the lock itself
	lock := entity.getlock()
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
		val := new(datastore.PropertyList)
		if err := storage.Get(tc, key, val); err != nil {
			log.Errorf(c, "get entity failed %s", err.Error())
			return err
		}
		for _, prop := range *val {
			switch prop.Name {
			case "lock_timestamp":
				prop.Value = getTime()
			case "lock_request":
				prop.Value = ""
			case "lock_retries":
				if retry {
					prop.Value = int(prop.Value.(int64) + 1)
				}
			}
		}
		if _, err := storage.Put(tc, key, val); err != nil {
			log.Errorf(c, "put entity failed %s", err.Error())
			return err
		}
		return nil
	}, nil)
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
		lock := entity.getlock()
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

/* datastore */
func (l *lock) Load(props []datastore.Property) error {
	return datastore.LoadStruct(l, props)
}

func (l *lock) Save() ([]datastore.Property, error) {
	return datastore.SaveStruct(l)
}
