package mapper

import (
	"fmt"
	"math"
	"sort"

	"golang.org/x/net/context"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

type (
	// Direction is the ordering (Ascending / Descending)
	Direction bool

	byIntKey    []*datastore.Key
	byStringKey []*datastore.Key
)

const (
	// Ascending indicates the sort order should be ascending
	Ascending Direction = true

	// Descending indicates the sort order should be descending
	Descending Direction = false
)

func (s byIntKey) Len() int           { return len(s) }
func (s byIntKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byIntKey) Less(i, j int) bool { return s[i].IntID() < s[j].IntID() }

func (s byStringKey) Len() int           { return len(s) }
func (s byStringKey) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byStringKey) Less(i, j int) bool { return s[i].StringID() < s[j].StringID() }

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

TODO: add better explanation of the various splitting scenarios
*/
func (q *Query) split(c context.Context, shards, oversampling int) ([]*Query, error) {
	// NOTE: at this point the query will have a single namespace for the one we're processing
	// all the datastore operations we do for the query should be within that namespace
	c, _ = appengine.Namespace(c, q.namespaces[0])

	equality, pr, err := q.toEqualityListAndRange()
	if err != nil {
		return nil, err
	}

	var propertyRanges []*propertyRange

	switch {
	case shards == 1:
		// no splitting required
		log.Debugf(c, "no split")
		propertyRanges = []*propertyRange{pr}
	case pr.empty():
		// no property range, shard on key space only
		log.Debugf(c, "shard on keyspace")
		propertyRanges, err = q.getScatterSplitPoints(c, shards, oversampling, equality)
	default:
		log.Debugf(c, "shard on property range")
		if pr.lower == nil {
			// set lower filter predicate from datastore so we work out a meaningful range
			v, err := q.getExtremePropertyValue(c, equality, pr.upper.FieldName, Ascending)
			if err != nil {
				return nil, err
			}
			pr.lower = &filter{pr.upper.FieldName, greaterEq, v}
			log.Debugf(c, "got lower %s", pr.lower)
		}
		// set upper filter predicate from datastore so we work out a meaningful range
		if pr.upper == nil {
			v, err := q.getExtremePropertyValue(c, equality, pr.upper.FieldName, Descending)
			if err != nil {
				return nil, err
			}
			pr.upper = &filter{pr.upper.FieldName, lessEq, v}
			log.Debugf(c, "got upper %s", pr.upper)
		}

		propertyRanges = pr.split(shards)
	}

	// build query for the property ranges
	results := []*Query{}
	for _, pr := range propertyRanges {
		query := NewQuery(q.kind)
		query.namespaces = q.namespaces
		for _, f := range equality {
			query.filter = append(query.filter, f)
		}
		if pr.lower != nil {
			query.filter = append(query.filter, *pr.lower)
		}
		if pr.upper != nil {
			query.filter = append(query.filter, *pr.upper)
		}

		results = append(results, query)
	}

	return results, nil
}

// get the first or last value from the range by querying the datastore
// so we can split the range effectively
func (q *Query) getExtremePropertyValue(c context.Context, equality []filter, property string, dir Direction) (interface{}, error) {
	// note, we're already working against a specific namespace at this point
	// so our context will already be for that (because this happens within the
	// namespace task)
	dq := datastore.NewQuery(q.kind)
	for _, f := range equality {
		dq = dq.Filter(f.FieldName+" "+operatorToString[f.Op], f.Value)
	}
	switch dir {
	case Ascending:
		dq = dq.Order(property)
	case Descending:
		dq = dq.Order("-" + property)
	}
	dq = dq.Limit(1)

	pl := make([]*datastore.PropertyList, 1)
	if _, err := dq.GetAll(c, pl); err != nil {
		return nil, err
	}
	for _, p := range *pl[0] {
		if p.Name == property {
			return p.Value, nil
		}
	}
	return nil, fmt.Errorf("property %s not found", property)
}

/*
Uses the scatter property to distribute ranges to segments.

A random scatter property is added to 1 out of every 128 entities see:
https://github.com/GoogleCloudPlatform/appengine-mapreduce/wiki/ScatterPropertyImplementation

Retrieving the entities with the highest scatter values provides a random sample of entities.
Because they are randomly selected, their distribution in keyspace should be the same as other
entities.

Looking at Keyspace, It looks something like this:
|---*------*------*---*--------*-----*--------*--|
Where "*" is a scatter entity and "-" is some other entity.

So once sample entities are obtained them by key allows them to serve as boundaries between
ranges of keyspace.
*/
func (q *Query) getScatterSplitPoints(c context.Context, shards, oversampling int, equality []filter) ([]*propertyRange, error) {
	results := []*propertyRange{}

	// number of samples to take
	// TODO: decide if this should be limited (1024 should be about the max ...)
	limit := shards * oversampling

	// first attempt to include equality filters
	dq := datastore.NewQuery(q.kind)
	for _, f := range equality {
		dq = dq.Filter(f.FieldName+" "+operatorToString[f.Op], f.Value)
	}
	dq = dq.Order("__scatter__")
	dq = dq.Limit(limit)
	dq = dq.KeysOnly()

	randomKeys, err := dq.GetAll(c, nil)
	if err != nil {
		return nil, err
	}

	log.Debugf(c, "got %d random keys", len(randomKeys))

	if len(randomKeys) == 0 && len(equality) > 0 {
		// no keys could be due to a missing index so fallback to just using the scatter
		// and hope that the distribution of keys is good enough, not idea but 'results'
		dq = datastore.NewQuery(q.kind)
		dq = dq.Order("__scatter__")
		dq = dq.Limit(limit)
		dq = dq.KeysOnly()

		randomKeys, err = dq.GetAll(c, nil)
		if err != nil {
			return nil, err
		}

		log.Debugf(c, "got %d random keys", len(randomKeys))
	}

	if len(randomKeys) == 0 {
		log.Debugf(c, "no keys, process range")
		// we're going to process the entire range
		pr, err := newPropertyRange(nil, nil)
		if err != nil {
			return nil, err
		}
		results = append(results, pr)
		return results, nil
	}

	// this assumes that keys in the table are either all int or all string which
	// seems like a reasonable assumption (and how would it work if they weren't?)
	if randomKeys[0].IntID() > 0 {
		sort.Sort(byIntKey(randomKeys))
	} else {
		sort.Sort(byStringKey(randomKeys))
	}

	// use the number of random keys returned to do a rough approximation of the
	// minimum size of the table. A scatter property is added roughly once every
	// 512 records (although the wiki says 0.78% chance which suggests it's 128)
	// When the number of random keys is below some threshold, reduce the number
	// of shards accordingly so we're not burning tasks for trivial numbers of
	// records - some namespaces may only need a single task to process them all

	// i.e. if we asked for 256 random keys (8 shards * 32 oversampling) but only
	// get back 64 then we estimate that the table has roughly 8192 entities in it
	// so we'd want to only split that into 2 shards.

	const scatterRatio = 128

	// TODO: this may be a specific job config setting (like shards and oversampling)
	minEntitiesPerShard := oversampling * scatterRatio
	minEntitiesEstimate := len(randomKeys) * scatterRatio

	if minEntitiesEstimate < minEntitiesPerShard*shards {
		shards = minEntitiesEstimate / minEntitiesPerShard
		if shards == 0 {
			shards = 1
		}
	}

	log.Debugf(c, "minEntitiesPerShard %d minEntitiesEstimate %d shards %d", minEntitiesPerShard, minEntitiesEstimate, shards)

	if len(randomKeys) > shards {
		randomKeys = q.chooseSplitPoints(randomKeys, shards)
	}

	for i := 0; i <= len(randomKeys); i++ {
		switch i {
		case 0:
			// first
			results = append(results, &propertyRange{
				lower: nil,
				upper: &filter{"__key__", lessThan, randomKeys[i]},
			})
		case len(randomKeys):
			// last
			results = append(results, &propertyRange{
				lower: &filter{"__key__", greaterEq, randomKeys[i-1]},
				upper: nil,
			})
		default:
			// others
			results = append(results, &propertyRange{
				lower: &filter{"__key__", greaterEq, randomKeys[i-1]},
				upper: &filter{"__key__", lessThan, randomKeys[i]},
			})
		}
	}

	return results, nil
}

// Reduces the oversampled list of keys into the desired number of shard split points
// If we have 64 keys (represented by '-'):
// |----------------------------------------------------------------|
//
// and we want 8 shards, we want to find the *7* points to divide that into 8 pieces
// |-------*-------*-------*-------*-------*-------*-------*--------|

func (q *Query) chooseSplitPoints(keys []*datastore.Key, shards int) []*datastore.Key {
	stride := float64(len(keys)) / float64(shards)
	results := make([]*datastore.Key, 0, shards)
	for i := 1; i < shards; i++ {
		idx := int(math.Floor(stride * float64(i)))
		results = append(results, keys[idx])
	}
	return results
}
