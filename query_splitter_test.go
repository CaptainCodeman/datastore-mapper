package mapper

import (
	"testing"
)

func validateSplits(t *testing.T, q *Query, expected []*Query) {
	results, err := q.split(nil, 5)
	if err != nil {
		t.Errorf("error splitting query %s %s", q.String(), err.Error())
	}
	if len(results) != len(expected) {
		t.Errorf("expected %d results, got %d", len(expected), len(results))
	}
	//for i, r := range results {
	// TODO: assert that query is the same
	//}
}

func TestSplit(t *testing.T) {
	tests := []struct {
		// query  *Query
		// shards int
		// values []filter{}
	}{
	/*
		// {NewQuery("foo").Filter("a >=", 1).Filter("a <=", 4).Filter("b =", 1), 10}
		{"byte", NewQuery("foo").Filter("bar >=", byte(0)).Filter("bar <", byte(100)), []interface{}{byte(0), byte(20), byte(40), byte(60), byte(80), byte(100)}},
		{"int", NewQuery("foo").Filter("bar >=", int(0)).Filter("bar <", int(100)), []interface{}{int(0), int(20), int(40), int(60), int(80), int(100)}},
		{"int32", NewQuery("foo").Filter("bar >=", int32(0)).Filter("bar <", int32(100)), []interface{}{int32(0), int32(20), int32(40), int32(60), int32(80), int32(100)}},
		{"int64", NewQuery("foo").Filter("bar >=", int64(0)).Filter("bar <", int64(100)), []interface{}{int64(0), int64(20), int64(40), int64(60), int64(80), int64(100)}},
		{"date", NewQuery("foo").Filter("bar >=", time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Filter("bar <", time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)), []interface{}{time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2002, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2004, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2006, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2008, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)}},
	*/
	}
	//	NewQuery("foo").Filter("bar >=", byte(0)).Filter("bar <", byte(100))
	for i, test := range tests {
		t.Errorf("%d not implemented %#v", i, test)
	}
	// validateSplits(t, q, expected)
}
