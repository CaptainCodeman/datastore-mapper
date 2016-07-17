package main

import (
	"time"

	"math/rand"
	"net/http"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/delay"
)

var (
	generateRandomFn = delay.Func("generateRandom", generateRandom)
	photographers    = []Photographer{
		{1, "Mr Canon"},
		{2, "Miss Nikon"},
		{3, "Mrs Pentax"},
		{4, "Ms Sony"},
	}
)

func init() {
	http.Handle("/_ah/warmup", http.HandlerFunc(warmupHandler))
}

func warmupHandler(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	v := r.URL.Query()
	f := v.Get("force")

	if f == "" {
		q := datastore.NewQuery("photo")
		q = q.Limit(1)
		q = q.KeysOnly()
		keys, _ := q.GetAll(c, nil)
		if len(keys) > 0 {
			return
		}
	}

	createInNamespace(c, "")

	/*
		for _, x := range "abc" {
			for _, y := range "abc" {
				for _, z := range "abc" {
					v := fmt.Sprintf("%c%c%c", x, y, z)
					createInNamespace(c, v)
				}
			}
		}
	*/

	w.WriteHeader(http.StatusOK)
}

// this will generate some random data
// on production this will create 96,768 entitied
func createInNamespace(c context.Context, namespace string) {
	c, _ = appengine.Namespace(c, namespace)

	for m := 1; m <= 12; m++ {
		for d := 1; d <= 28; d++ {
			taken := time.Date(2015, time.Month(m), d, 0, 0, 0, 0, time.UTC)
			generateRandomFn.Call(c, taken)
		}
	}
}

// this will generate some random data for the given day
// 24 * 12 on appengine (288)
// 24 only on development
func generateRandom(c context.Context, day time.Time) error {
	var x int
	if appengine.IsDevAppServer() {
		x = 1
	} else {
		x = 12
	}
	keys := make([]*datastore.Key, 24*x)
	photos := make([]*Photo, 24*x)

	id := 0
	for h := 0; h < 24; h++ {
		taken := day.Add(time.Duration(h) * time.Hour)
		for i := 0; i < x; i++ {
			photographer := photographers[rand.Int31n(4)]
			photos[id] = &Photo{
				Photographer: photographer,
				Uploaded:     time.Now().UTC(),
				Width:        8000,
				Height:       6000,
				Taken:        taken,
				TakenDay:     day,
			}
			keys[id] = datastore.NewIncompleteKey(c, "photo", nil)
			id++
		}
	}
	nds.PutMulti(c, keys, photos)
	return nil
}
