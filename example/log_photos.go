package main

import (
	"io"
	"time"

	"encoding/json"
	"net/http"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"

	"github.com/captaincodeman/datastore-mapper"
)

type (
	photoLogger struct{}

	photoOutput struct {
		*Photo
		Namespace string `json:"namespace"`
	}
)

const (
	dateFormat = "2006-01-02"
)

func init() {
	mapper.RegisterJob(&photoLogger{})
}

func (p *photoLogger) Query(r *http.Request) (*mapper.Query, error) {
	v := r.URL.Query()

	fromStr := v.Get("from")
	toStr := v.Get("to")
	now := time.Now().UTC()

	var from time.Time
	var to time.Time
	var err error

	// default to previous day but allow any
	if toStr == "" {
		to = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	} else {
		to, err = time.Parse(dateFormat, toStr)
		if err != nil {
			return nil, err
		}
	}

	if fromStr == "" {
		from = to.Add(time.Duration(-24) * time.Hour)
	} else {
		from, err = time.Parse(dateFormat, fromStr)
		if err != nil {
			return nil, err
		}
	}

	q := mapper.NewQuery("photo")
	q = q.Namespace("") // TODO: allow namespace to be set from request, use * for 'all'
	q = q.Filter("taken >=", from)
	q = q.Filter("taken <", to)
	q = q.Limit(100) // this is the batch size to use when processing (maybe too obscure?)

	return q, nil
}

// Next processes the next item
func (p *photoLogger) Next(c context.Context, w io.Writer, counters mapper.Counters, key *datastore.Key) error {
	photo := new(Photo)
	if err := nds.Get(c, key, photo); err != nil {
		log.Errorf(c, err.Error())
		return err
	}
	photo.ID = key.IntID()

	// log.Debugf(c, "%s photo %d by %d %s", photo.Taken.Format(dateFormat), key.IntID(), photo.Photographer.ID, photo.Photographer.Name)
	counters.Increment(photo.Photographer.Name, 1)

	out := &photoOutput{
		Photo:     photo,
		Namespace: key.Namespace(),
	}
	enc := json.NewEncoder(w)
	enc.Encode(out)

	return nil
}

// NextBatch processes the next batch of keys
func (p *photoLogger) NextBatch(c context.Context, w io.Writer, counters mapper.Counters, keys []*datastore.Key) error {
	photos := make([]*Photo, len(keys))
	if err := nds.GetMulti(c, keys, photos); err != nil {
		log.Errorf(c, err.Error())
		return err
	}

	enc := json.NewEncoder(w)
	for i, photo := range photos {
		photo.ID = keys[i].IntID()
		counters.Increment(photo.Photographer.Name, 1)

		out := &photoOutput{
			Photo:     photo,
			Namespace: keys[i].Namespace(),
		}
		enc.Encode(out)
	}

	return nil
}

// job lifecycle notifications
func (p *photoLogger) JobStarted(c context.Context, id string) {
	log.Debugf(c, "JobStarted %s", id)
}
func (p *photoLogger) JobCompleted(c context.Context, id string) {
	log.Debugf(c, "JobCompleted %s", id)
}
func (p *photoLogger) NamespaceStarted(c context.Context, id string, namespace string) {
	log.Debugf(c, "NamespaceStarted %s %s", id, namespace)
}
func (p *photoLogger) NamespaceCompleted(c context.Context, id string, namespace string) {
	log.Debugf(c, "NamespaceCompleted %s %s", id, namespace)
}
func (p *photoLogger) ShardStarted(c context.Context, id string, namespace string, shard int) {
	log.Debugf(c, "ShardStarted %s %s %d", id, namespace, shard)
}
func (p *photoLogger) ShardCompleted(c context.Context, id string, namespace string, shard int) {
	log.Debugf(c, "ShardCompleted %s %s %d", id, namespace, shard)
}
func (p *photoLogger) SliceStarted(c context.Context, id string, namespace string, shard, slice int) {
	log.Debugf(c, "SliceStarted %s %s %d %d", id, namespace, shard, slice)
}
func (p *photoLogger) SliceCompleted(c context.Context, id string, namespace string, shard, slice int) {
	log.Debugf(c, "SliceCompleted %s %s %d %d", id, namespace, shard, slice)
}
