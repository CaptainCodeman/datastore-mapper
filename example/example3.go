package main

import (
	"time"

	"net/http"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"

	"github.com/captaincodeman/datastore-mapper"
)

type (
	// parse request parameters to create query
	example3 struct{}
)

const (
	dateFormat = "2006-01-02"
)

func init() {
	mapper.RegisterJob(&example3{})
}

func (x *example3) Query(r *http.Request) (*mapper.Query, error) {
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
	q = q.NamespaceEmpty()
	q = q.Filter("taken >=", from)
	q = q.Filter("taken <", to)

	return q, nil
}

// Next processes the next item
func (x *example3) Next(c context.Context, counters mapper.Counters, key *datastore.Key) error {
	photo := new(Photo)
	if err := nds.Get(c, key, photo); err != nil {
		log.Errorf(c, err.Error())
		return err
	}
	photo.ID = key.IntID()

	counters.Increment(photo.Photographer.Name, 1)
	return nil
}
