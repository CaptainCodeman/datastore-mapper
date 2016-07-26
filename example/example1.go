package main

import (
	"net/http"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"

	"github.com/captaincodeman/datastore-mapper"
)

type (
	// simple keysonly iteration and aggregation using counters
	example1 struct{}
)

func init() {
	mapper.RegisterJob(&example1{})
}

func (x *example1) Query(r *http.Request) (*mapper.Query, error) {
	q := mapper.NewQuery("photo")
	q = q.NamespaceEmpty()
	return q, nil
}

// Next processes the next item
func (x *example1) Next(c context.Context, counters mapper.Counters, key *datastore.Key) error {
	// we need to load the entity ourselves
	photo := new(Photo)
	if err := nds.Get(c, key, photo); err != nil {
		return err
	}

	counters.Increment(photo.Photographer.Name, 1)
	return nil
}
