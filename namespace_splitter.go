package mapper

import (
	"bytes"
	"fmt"
	"math"
	"sort"

	"encoding/gob"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

type (
	// namespaceSplitter processes a single namespace for a job
	namespaceSplitter struct {
		// Lock ensures single atomic operation
		Lock

		// Job is the job processor type
		Job Job `datastore:"-"`

		// Namespace is the namespace to split
		Namespace string `datastore:"namespace,noindex"`

		// Active indicated if this splitter is active
		Active bool `datastore:"active,noindex"`

		// Count is the number of shards
		Count int64 `datastore:"count,noindex"`

		// Query provides the source to process
		Query *Query `datastore:"-"`

		// private fields used by local instance
		id    string
		queue string
	}
)

const (
	namespaceSplitterKind = "namespace_splitter"
	namespaceSplitterURL  = "/namespace-split"
)

func init() {
	Server.HandleFunc(namespaceSplitterURL, namespaceSplitterHandler)
}

func namespaceSplitterHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	id, seq, queue, _ := ParseLock(r)

	c := appengine.NewContext(r)
	key := datastore.NewKey(c, config.DatastorePrefix+namespaceSplitterKind, id, 0, nil)
	ns := new(namespaceSplitter)

	if err := GetLock(c, key, ns, seq); err != nil {
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

	ns.id = id
	ns.queue = queue

	err := ns.split(c)
	if err != nil {
		// this will cause a task retry
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	// splitting done
	if _, err := storage.Put(c, key, ns); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (n *namespaceSplitter) split(c context.Context) error {
	ns, _ := appengine.Namespace(c, n.Namespace)

	keyRanges := []*KeyRange{}

	minimumShards := 12
	oversamplingFactor := 32
	limit := minimumShards * oversamplingFactor

	q := n.Query.toDatastoreQuery()
	q = q.Order("__scatter__")
	q = q.Limit(limit)
	q = q.KeysOnly()

	randomKeys, err := q.GetAll(ns, nil)
	if err != nil {
		log.Errorf(c, "error %s", err.Error())
	}
	if len(randomKeys) == 0 {
		log.Errorf(c, "no keys")
		// There are no entities with scatter property. We have no idea how to split.

		// TODO: split property key
		keyRanges = append(keyRanges, newKeyRange(n.Namespace, nil, nil, Ascending, true))
	} else {
		// this assumes that all the keys are consistently int or string (reasonable)
		if randomKeys[0].IntID() > 0 {
			sort.Sort(byIntKey(randomKeys))
		} else {
			sort.Sort(byStringKey(randomKeys))
		}

		log.Debugf(c, "splits %d", len(randomKeys))
		// TODO: use the number of random keys to do a rough approximation of the
		// minimum size of the table. A scatter property is added roughly once every
		// 512 records (although the wiki says 0.78% chance which is more like 128)
		// When the number of random keys is below some threshold, reduce the number
		// of shards accordingly so we're not burning tasks for trivial numbers of
		// records - some namespaces may only need a single task to process

		if len(randomKeys) > minimumShards {
			randomKeys, _ = n.chooseSplitPoints(randomKeys, minimumShards)
		}

		keyRanges = append(keyRanges, newKeyRange(n.Namespace, nil, randomKeys[0], Ascending, false))
		for i := 0; i < len(randomKeys)-1; i++ {
			keyRanges = append(keyRanges, newKeyRange(n.Namespace, randomKeys[i], randomKeys[i+1], Ascending, true))
		}
		keyRanges = append(keyRanges, newKeyRange(n.Namespace, randomKeys[len(randomKeys)-1], nil, Ascending, true))
	}

	/*
		for i, kr := range keyRanges {
			log.Debugf(c, "%d kr: %s", i, kr.String())
		}
	*/

	// if no keyranges then nothing to do for this splitter, short-circuit to completed

	// Even though this method does not schedule shard task and save shard state
	// transactionally, it's safe for taskqueue to retry this logic because
	// the initial shard_state for each shard is the same from any retry.
	// This is an important yet reasonable assumption on ShardState.

	keys := make([]*datastore.Key, len(keyRanges))
	for shardNumber := 0; shardNumber < len(keyRanges); shardNumber++ {
		id := fmt.Sprintf("%s-%d", n.id, shardNumber)
		key := datastore.NewKey(c, config.DatastorePrefix+shardKind, id, 0, nil)
		keys[shardNumber] = key
	}

	shards := make([]*shard, len(keyRanges))
	if err := storage.GetMulti(c, keys, shards); err != nil {
		if me, ok := err.(appengine.MultiError); ok {
			for shardNumber, merr := range me {
				if merr == datastore.ErrNoSuchEntity {
					// keys[shardNumber] is missing, create the shard for it
					key := keys[shardNumber]

					s := new(shard)
					s.Active = true
					s.Namespace = n.Namespace
					s.Query = n.Query
					s.KeyRange = keyRanges[shardNumber]
					s.Shard = shardNumber
					s.Description = keyRanges[shardNumber].String()
					s.Job = n.Job
					s.Counters = NewCounters()

					t := taskqueue.NewPOSTTask(config.BasePath+shardURL, nil)
					if _, err := ScheduleLock(c, key, s, t, n.queue); err != nil {
						return err
					}
				}
			}
		} else {
			// some other error
			return err
		}
	}

	return nil
}

// Returns the best split points given a random set of datastore.Keys
func (n *namespaceSplitter) chooseSplitPoints(sortedKeys []*datastore.Key, shardCount int) ([]*datastore.Key, error) {
	indexStride := float64(len(sortedKeys)) / float64(shardCount)
	if indexStride < 1 {
		return nil, fmt.Errorf("not enough keys")
	}
	results := make([]*datastore.Key, 0, shardCount)
	for i := 1; i < shardCount; i++ {
		idx := int(math.Floor(indexStride*float64(i) + 0.5))
		results = append(results, sortedKeys[idx])
	}

	return results, nil
}

/* datastore */
func (n *namespaceSplitter) Load(props []datastore.Property) error {
	datastore.LoadStruct(n, props)

	for _, prop := range props {
		switch prop.Name {
		case "job":
			payload := bytes.NewBuffer(prop.Value.([]byte))
			enc := gob.NewDecoder(payload)
			if err := enc.Decode(&n.Job); err != nil {
				return err
			}
		case "query":
			n.Query = &Query{}
			if err := n.Query.GobDecode(prop.Value.([]byte)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (n *namespaceSplitter) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(n)

	payload := new(bytes.Buffer)
	enc := gob.NewEncoder(payload)
	if err := enc.Encode(&n.Job); err != nil {
		return nil, err
	}
	props = append(props, datastore.Property{Name: "job", Value: payload.Bytes(), NoIndex: true, Multiple: false})

	b, err := n.Query.GobEncode()
	if err != nil {
		return nil, err
	}
	props = append(props, datastore.Property{Name: "query", Value: b, NoIndex: true, Multiple: false})

	return props, err
}
