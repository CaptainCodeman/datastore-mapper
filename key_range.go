package mapper

import (
	"fmt"

	"google.golang.org/appengine/datastore"
)

type (
	// KeyRange represents a range of datastore keys
	KeyRange struct {
		Namespace string
		KeyStart  *datastore.Key
		KeyEnd    *datastore.Key
		Direction Direction
		Inclusive bool
	}

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

func newKeyRange(namespace string, keyStart, keyEnd *datastore.Key, direction Direction, inclusive bool) *KeyRange {
	return &KeyRange{
		Namespace: namespace,
		KeyStart:  keyStart,
		KeyEnd:    keyEnd,
		Direction: direction,
		Inclusive: inclusive,
	}
}

func (k *KeyRange) String() string {
	var dir string
	if k.Direction == Ascending {
		dir = "ASC"
	} else {
		dir = "DESC"
	}
	var leftSide string
	if k.KeyStart == nil {
		leftSide = "[first]"
	} else {
		if k.Inclusive {
			leftSide = ">="
		} else {
			leftSide = ">"
		}
	}
	var rightSide string
	if k.KeyEnd == nil {
		rightSide = "[last]"
	} else {
		rightSide = "<"
	}
	return fmt.Sprintf("%s%s to %s%s (%s)", leftSide, k.KeyStart.String(), rightSide, k.KeyEnd.String(), dir)
}

func (k *KeyRange) Advance(key *datastore.Key) {
	k.Inclusive = false
	k.KeyStart = key
}

func (k *KeyRange) FilterQuery(query *datastore.Query) *datastore.Query {
	var startComparator string
	if k.Inclusive {
		startComparator = ">="
	} else {
		startComparator = ">"
	}
	var endComparator string
	endComparator = "<"
	if k.KeyStart != nil {
		query = query.Filter("__key__ "+startComparator, k.KeyStart)
	}
	if k.KeyEnd != nil {
		query = query.Filter("__key__ "+endComparator, k.KeyEnd)
	}
	return query
}

func (k *KeyRange) MakeDirectedQuery(kind string, keysOnly bool) *datastore.Query {
	q := datastore.NewQuery(kind)
	q = k.FilterQuery(q)
	switch k.Direction {
	case Ascending:
		q = q.Order("__key__")
	case Descending:
		q = q.Order("-__key__")
	}
	if keysOnly {
		q = q.KeysOnly()
	}
	return q
}

func (k *KeyRange) MakeAscendingQuery(kind string, keysOnly bool) *datastore.Query {
	q := datastore.NewQuery(kind)
	q = k.FilterQuery(q)
	q = q.Order("__key__")
	if keysOnly {
		q = q.KeysOnly()
	}
	return q
}
