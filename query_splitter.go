package mapper

import (
	"golang.org/x/net/context"
	/*
		"google.golang.org/appengine"
		"google.golang.org/appengine/datastore"
		"google.golang.org/appengine/log"
		"google.golang.org/appengine/taskqueue"
	*/)

/*
Splits an arbitrary datastore query. This is used to shard queries within a namespace.
This is done in one of two ways:

1. If the query contains an inequality filter, the lower and upper bounds are determined (this
may involve querying the datastore) then the range is split naively. This works well when the
property that is being queried on is uniformly distributed.

2. If the query does not contain an inequality filter. The query will be partitioned by the
entity key. This is done by using the "__scatter__" property to get a random sample of the
keyspace and partitioning based on that. This can result in a poor distribution if there are
equality filters on the query that bias the selection with respect to certain regions of
keyspace.

The following clauses are not supported by this class: An inequality filter of unsupported type.
Filters that are incompatible with datastore cursors such as: Combining multiple clauses with an
OR. A filter on a value being NOT_EQUAL.
*/
func (q *Query) split(c context.Context, shards int) ([]*Query, error) {
	pr, err := newPropertyRange(q)
	if err != nil {
		return nil, err
	}

	if pr.propertyName == "" {
		// split on scatter with regular query
	}

	return nil, nil
}
