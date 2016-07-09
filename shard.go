package mapper

import (
	"bytes"
	"strings"
	"time"

	"encoding/gob"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

type (
	// shard processes shards for a job (slices of a namespace)
	shard struct {
		// Lock ensures single atomic operation
		Lock

		// Shard is the shard number within this namespace
		Shard int `datastore:"shard,noindex"`

		// Job is the job processor type
		Job Job `datastore:"-"`

		// Namespace is the namespace for this shard
		Namespace string `datastore:"namespace,noindex"`

		// Description is the description for this shard
		Description string `datastore:"description,noindex"`

		// Active indicated if this shard is active
		Active bool `datastore:"active,noindex"`

		// Query provides the source to process
		Query *Query `datastore:"-"`

		// KeyRange is the keyrange for this shard
		KeyRange *KeyRange `datastore:"-"`

		// Cursor is the datastore cursor to start from
		Cursor string `datastore:"cursor,noindex"`

		// Count is the number of records
		Count int64 `datastore:"count,noindex"`

		// Counters holds the shards counters map
		Counters Counters `datastore:"-"`

		// private fields used by local instance
		queue string
	}
)

const (
	shardKind = "shard"
	shardURL  = "/shard"
)

func init() {
	Server.HandleFunc(shardURL, shardHandler)
}

func shardHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	id, seq, queue, _ := ParseLock(r)

	c := appengine.NewContext(r)
	key := datastore.NewKey(c, config.DatastorePrefix+shardKind, id, 0, nil)
	s := new(shard)

	if err := GetLock(c, key, s, seq); err != nil {
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

	s.queue = queue

	completed, err := s.iterate(c)
	if err != nil {
		// this will cause a task retry
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	if completed {
		// all done !
		s.Active = false
		s.Cursor = ""
		if _, err := storage.Put(c, key, s); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	// schedule continuation
	t := taskqueue.NewPOSTTask(config.BasePath+shardURL, nil)
	ScheduleLock(c, key, s, t, queue)
}

func (s *shard) createQuery(c context.Context) *datastore.Query {
	q := s.Query.toDatastoreQuery()
	return q
}

func (s *shard) iterate(c context.Context) (bool, error) {
	// switch namespace
	c, _ = appengine.Namespace(c, s.Namespace)

	// use the full 10 minutes allowed (assuming front-end instance type)
	c, _ = context.WithTimeout(c, time.Duration(10)*time.Minute)

	size := int(s.Query.limit)
	if size == 0 {
		size = 100
	}

	q := s.createQuery(c)
	q = s.KeyRange.FilterQuery(q)
	q = q.Limit(size)

	var cursor *datastore.Cursor
	if s.Cursor != "" {
		newCursor, err := datastore.DecodeCursor(s.Cursor)
		if err != nil {
			log.Errorf(c, "get start cursor error %s", err.Error())
			return false, err
		}
		cursor = &newCursor
	}

	// we query and proecess in batches of 'size' and also create a
	// timeout signal that is checked after each batch. So, a batch has
	// to take less than 5 minutes to execute
	timeout := make(chan bool, 1)
	timer := time.AfterFunc(time.Duration(5)*time.Minute, func() {
		timeout <- true
	})
	defer timer.Stop()

	// TODO: check one of these is enabled when job is started
	single, useSingle := s.Job.(JobSingle)
	batch, useBatch := s.Job.(JobBatched)
	if useSingle && useBatch {
		useSingle = false
	}

	for {
		count := 0

		if cursor != nil {
			q = q.Start(*cursor)
		}

		it := q.Run(c)
		keys := make([]*datastore.Key, 0, size)
		for {
			key, err := it.Next(nil)
			if err == datastore.Done {
				break
			}
			if err != nil {
				log.Errorf(c, "error %s", err.Error())
				return false, err
			}

			if useSingle {
				// TODO: check for return errors
				single.Single(c, s.Counters, key)
				s.Count++
			} else {
				keys = append(keys, key)
			}

			count++
		}

		if useBatch {
			// TODO: check for return errors
			batch.Batch(c, s.Counters, keys)
			s.Count += int64(len(keys))
		}

		// did we process a full batch? if not, we hit the end of the dataset
		if count < size {
			return true, nil
		}

		newCursor, err := it.Cursor()
		if err != nil {
			log.Errorf(c, "get next cursor error %s", err.Error())
			return false, err
		}
		cursor = &newCursor
		s.Cursor = cursor.String()

		// check if we've timed out
		select {
		case <-timeout:
			// timeout without completing
			return false, nil
		default:
			// continue processing in thsi request
		}
	}
}

/* datastore */
func (s *shard) Load(props []datastore.Property) error {
	datastore.LoadStruct(s, props)
	s.Counters = make(map[string]int64)

	for _, prop := range props {
		switch prop.Name {
		case "job":
			payload := bytes.NewBuffer(prop.Value.([]byte))
			enc := gob.NewDecoder(payload)
			if err := enc.Decode(&s.Job); err != nil {
				return err
			}
		case "query":
			s.Query = &Query{}
			if err := s.Query.GobDecode(prop.Value.([]byte)); err != nil {
				return err
			}
		case "key_range":
			s.KeyRange = &KeyRange{}
			payload := bytes.NewBuffer(prop.Value.([]byte))
			enc := gob.NewDecoder(payload)
			if err := enc.Decode(&s.KeyRange); err != nil {
				return err
			}
		default:
			if strings.HasPrefix(prop.Name, "counters.") {
				key := prop.Name[9:len(prop.Name)]
				s.Counters[key] = prop.Value.(int64)
			}
		}
	}

	return nil
}

func (s *shard) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(s)

	payload := new(bytes.Buffer)
	enc := gob.NewEncoder(payload)
	if err := enc.Encode(&s.Job); err != nil {
		return nil, err
	}
	props = append(props, datastore.Property{Name: "job", Value: payload.Bytes(), NoIndex: true, Multiple: false})

	b, err := s.Query.GobEncode()
	if err != nil {
		return nil, err
	}
	props = append(props, datastore.Property{Name: "query", Value: b, NoIndex: true, Multiple: false})

	payload = new(bytes.Buffer)
	enc = gob.NewEncoder(payload)
	if err := enc.Encode(&s.KeyRange); err != nil {
		return nil, err
	}
	props = append(props, datastore.Property{Name: "key_range", Value: payload.Bytes(), NoIndex: true, Multiple: false})

	for key, value := range s.Counters {
		props = append(props, datastore.Property{Name: "counters." + key, Value: value, NoIndex: true, Multiple: false})
	}

	return props, err
}
