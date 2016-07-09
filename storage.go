package mapper

import (
	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
)

type (
	storageImpl struct {
		Get              func(c context.Context, key *datastore.Key, val interface{}) error
		GetMulti         func(c context.Context, keys []*datastore.Key, vals interface{}) error
		Put              func(c context.Context, key *datastore.Key, val interface{}) (*datastore.Key, error)
		PutMulti         func(c context.Context, keys []*datastore.Key, vals interface{}) ([]*datastore.Key, error)
		Delete           func(c context.Context, key *datastore.Key) error
		DeleteMulti      func(c context.Context, keys []*datastore.Key) error
		RunInTransaction func(c context.Context, f func(tc context.Context) error, opts *datastore.TransactionOptions) error
	}
)

var (
	storage storageImpl
)

func init() {
	// use memcached by default
	UseMemcache()
}

// UseDatastore causes datastore operations to use the raw appengine datastore functions
func UseDatastore() {
	storage = storageImpl{
		Get:              datastore.Get,
		GetMulti:         datastore.GetMulti,
		Put:              datastore.Put,
		PutMulti:         datastore.PutMulti,
		Delete:           datastore.Delete,
		DeleteMulti:      datastore.DeleteMulti,
		RunInTransaction: datastore.RunInTransaction,
	}
}

// UseMemcache causes datastore operations to use the nds memcached datastore functions
func UseMemcache() {
	storage = storageImpl{
		Get:              nds.Get,
		GetMulti:         nds.GetMulti,
		Put:              nds.Put,
		PutMulti:         nds.PutMulti,
		Delete:           nds.Delete,
		DeleteMulti:      nds.DeleteMulti,
		RunInTransaction: nds.RunInTransaction,
	}
}
