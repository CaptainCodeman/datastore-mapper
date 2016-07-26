package main

import (
	"net/http"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"

	"github.com/captaincodeman/datastore-mapper"
)

type (
	// lifecycle notifications
	example4 struct{}
)

func init() {
	mapper.RegisterJob(&example4{})
}

func (x *example4) Query(r *http.Request) (*mapper.Query, error) {
	q := mapper.NewQuery("photo")
	q = q.NamespaceEmpty()
	return q, nil
}

// Next processes the next item
func (x *example4) Next(c context.Context, counters mapper.Counters, key *datastore.Key) error {
	return nil
}

// job lifecycle notifications
func (x *example4) JobStarted(c context.Context, id string) {
	log.Debugf(c, "JobStarted %s", id)
}
func (x *example4) JobCompleted(c context.Context, id string) {
	log.Debugf(c, "JobCompleted %s", id)
}
func (x *example4) NamespaceStarted(c context.Context, id string, namespace string) {
	log.Debugf(c, "NamespaceStarted %s %s", id, namespace)
}
func (x *example4) NamespaceCompleted(c context.Context, id string, namespace string) {
	log.Debugf(c, "NamespaceCompleted %s %s", id, namespace)
}
func (x *example4) ShardStarted(c context.Context, id string, namespace string, shard int) {
	log.Debugf(c, "ShardStarted %s %s %d", id, namespace, shard)
}
func (x *example4) ShardCompleted(c context.Context, id string, namespace string, shard int) {
	log.Debugf(c, "ShardCompleted %s %s %d", id, namespace, shard)
}
func (x *example4) SliceStarted(c context.Context, id string, namespace string, shard, slice int) {
	log.Debugf(c, "SliceStarted %s %s %d %d", id, namespace, shard, slice)
}
func (x *example4) SliceCompleted(c context.Context, id string, namespace string, shard, slice int) {
	log.Debugf(c, "SliceCompleted %s %s %d %d", id, namespace, shard, slice)
}
