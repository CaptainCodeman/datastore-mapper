package main

import (
	"io"

	"encoding/json"
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"

	"github.com/captaincodeman/datastore-mapper"
)

type (
	// export custom JSON to Cloud Storage
	example5 struct {
		photo *Photo
	}

	photoOutput struct {
		*Photo
		// add namespace for bigquery
		Namespace string `json:"namespace"`
	}
)

func init() {
	mapper.RegisterJob(&example5{})
}

func (x *example5) Query(r *http.Request) (*mapper.Query, error) {
	q := mapper.NewQuery("photo")
	q = q.Namespace("")
	return q, nil
}

// Make creates the entity to load into
func (x *example5) Make() interface{} {
	x.photo = new(Photo)
	return x.photo
}

// Next processes the next item
func (x *example5) Next(c context.Context, w io.Writer, counters mapper.Counters, key *datastore.Key) error {
	photo := x.photo
	photo.ID = key.IntID()

	out := &photoOutput{
		Photo:     photo,
		Namespace: key.Namespace(),
	}

	enc := json.NewEncoder(w)
	enc.Encode(out)

	return nil
}
