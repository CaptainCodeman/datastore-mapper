package main

import (
	"io"

	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"

	"github.com/captaincodeman/datastore-mapper"
)

type (
	// simple eager iteration and aggregation using counters
	example2 struct {
		photo *Photo
	}
)

func init() {
	mapper.RegisterJob(&example2{})
}

func (x *example2) Query(r *http.Request) (*mapper.Query, error) {
	q := mapper.NewQuery("photo")
	q = q.Namespace("")
	return q, nil
}

// Make creates the entity to load into
func (x *example2) Make() interface{} {
	x.photo = new(Photo)
	return x.photo
}

// Next processes the next item
func (x *example2) Next(c context.Context, w io.Writer, counters mapper.Counters, key *datastore.Key) error {
	// the photo instance was loaded from the query by the mapper
	counters.Increment(x.photo.Photographer.Name, 1)
	return nil
}
