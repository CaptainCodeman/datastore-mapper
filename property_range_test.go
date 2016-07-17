package mapper

import (
	"testing"
	"time"
)

// TODO: refactor this into smaller, more focused tests (it just grew ...)
func TestPropertyRange(t *testing.T) {
	now := time.Now().UTC()
	old := time.Date(now.Year()-1, now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond(), time.UTC)

	tests := []struct {
		query    *Query
		fail     bool
		property string
		lower    *filter
		upper    *filter
		equal    []filter
	}{
		// success filters
		{NewQuery("foo").Filter("a =", 1).Filter("b =", 2), false, "", nil, nil, []filter{{"a", equal, 1}, {"b", equal, 2}}},
		{NewQuery("foo").Filter("a >", 1).Filter("a <", 2), false, "a", &filter{"a", greaterThan, 1}, &filter{"a", lessThan, 2}, []filter{}},
		{NewQuery("foo").Filter("a >", old).Filter("a <=", now), false, "a", &filter{"a", greaterThan, old}, &filter{"a", lessEq, now}, []filter{}},
		// expect the same property
		{NewQuery("foo").Filter("a >", 1).Filter("b <", 2), true, "", nil, nil, nil},
		// expect a closed range
		{NewQuery("foo").Filter("a >", 10).Filter("a <=", 2), true, "", nil, nil, nil},
		{NewQuery("foo").Filter("a >", now).Filter("a <=", old), true, "", nil, nil, nil},
	}
	for i, test := range tests {
		equal, pr, err := test.query.toEqualityListAndRange()
		/*
			// dump output
			if err == nil {
				if result.lower != nil {
					t.Logf("%d lower %s %s %v", i, result.lower.FieldName, operatorToString[result.lower.Op], result.lower.Value)
				}
				if result.upper != nil {
					t.Logf("%d upper %s %s %v", i, result.upper.FieldName, operatorToString[result.upper.Op], result.upper.Value)
				}
				for j, f := range result.equal {
					t.Logf("%d.%d equal %s %s %v", i, j, f.FieldName, operatorToString[f.Op], f.Value)
				}
			}
		*/

		if err != nil && !test.fail {
			t.Errorf("%d unexpected error %s", i, err.Error())
			continue
		}
		if err == nil && test.fail {
			t.Errorf("%d unexpected success", i)
			continue
		}
		if err != nil && test.fail {
			continue
		}

		if pr.empty() {
			if test.lower != nil {
				t.Errorf("%d expected lower %s %s %v", i, test.lower.FieldName, operatorToString[test.lower.Op], test.lower.Value)
			}
			if test.upper != nil {
				t.Errorf("%d expected upper %s %s %v", i, test.upper.FieldName, operatorToString[test.upper.Op], test.upper.Value)
			}
		} else {
			if test.lower == nil {
				t.Errorf("%d unexpected lower %s %s %v", i, pr.lower.FieldName, operatorToString[pr.lower.Op], pr.lower.Value)
			} else {
				exp := test.lower
				got := pr.lower
				if got.FieldName != exp.FieldName || got.Op != exp.Op || got.Value != exp.Value {
					t.Errorf("%d expected lower %s %s %v, got %s %s %v", i, exp.FieldName, operatorToString[exp.Op], exp.Value, got.FieldName, operatorToString[got.Op], got.Value)
				}
				exp = test.upper
				got = pr.upper
				if got.FieldName != exp.FieldName || got.Op != exp.Op || got.Value != exp.Value {
					t.Errorf("%d expected upper %s %s %v, got %s %s %v", i, exp.FieldName, operatorToString[exp.Op], exp.Value, got.FieldName, operatorToString[got.Op], got.Value)
				}
			}
			if test.upper == nil {
				t.Errorf("%d unexpected upper %s %s %v", i, pr.upper.FieldName, operatorToString[pr.upper.Op], pr.upper.Value)
			} else {
				exp := test.lower
				got := pr.lower
				if got.FieldName != exp.FieldName || got.Op != exp.Op || got.Value != exp.Value {
					t.Errorf("%d expected lower %s %s %v, got %s %s %v", i, exp.FieldName, operatorToString[exp.Op], exp.Value, got.FieldName, operatorToString[got.Op], got.Value)
				}
				exp = test.upper
				got = pr.upper
				if got.FieldName != exp.FieldName || got.Op != exp.Op || got.Value != exp.Value {
					t.Errorf("%d expected upper %s %s %v, got %s %s %v", i, exp.FieldName, operatorToString[exp.Op], exp.Value, got.FieldName, operatorToString[got.Op], got.Value)
				}
			}
		}

		if pr.propertyName() != test.property {
			t.Errorf("%d expected property %s got %s", i, test.property, pr.propertyName())
		}

		if len(equal) != len(test.equal) {
			t.Errorf("%d expected %d equal, got %d", i, len(test.equal), len(equal))
			for j, f := range test.equal {
				t.Errorf("%d.%d expected %s %s %v", i, j, f.FieldName, operatorToString[f.Op], f.Value)
			}
			for j, f := range equal {
				t.Errorf("%d.%d result %s %s %v", i, j, f.FieldName, operatorToString[f.Op], f.Value)
			}
			continue
		}

		// compare equal values
		for j, f := range equal {
			exp := test.equal[j]
			if f.FieldName != exp.FieldName || f.Op != exp.Op || f.Value != exp.Value {
				t.Errorf("%d.%d expected %s %s %v, got %s %s %v", i, j, exp.FieldName, operatorToString[exp.Op], exp.Value, f.FieldName, operatorToString[f.Op], f.Value)
			}
		}
	}
}

/*
func TestPropertyRangeSplit(t *testing.T) {
	tests := []struct {
		query  *Query
		shards int
		values [][]filter
	}{
		{
			// integers
			NewQuery("foo").Filter("a >=", 1).Filter("a <=", 4).Filter("b =", 1), 10,
			[][]filter{
				[]filter{{"a", greaterEq, 1}, {"a", lessThan, 2}, {"b", equal, 1}},
				[]filter{{"a", greaterEq, 2}, {"a", lessThan, 3}, {"b", equal, 1}},
				[]filter{{"a", greaterEq, 3}, {"a", lessEq, 4}, {"b", equal, 1}},
			},
		},
		{
			// floating points
			NewQuery("foo").Filter("a >=", 1.0).Filter("a <=", 4.0).Filter("b =", 1), 4,
			[][]filter{
				[]filter{{"a", greaterEq, 1}, {"a", lessThan, 1.75}, {"b", equal, 1}},
				[]filter{{"a", greaterEq, 1.75}, {"a", lessThan, 2.5}, {"b", equal, 1}},
				[]filter{{"a", greaterEq, 2.5}, {"a", lessThan, 3.25}, {"b", equal, 1}},
				[]filter{{"a", greaterEq, 3.25}, {"a", lessEq, 4}, {"b", equal, 1}},
			},
		},
		// TODO: string, datetime
	}
	for i, test := range tests {
		equal, pr, err := test.query.toEqualityListAndRange()
		if err != nil {
			t.Errorf("%d error %s", i, err.Error())
			continue
		}
		split := pr.split(test.shards)
		if split == nil {
			t.Errorf("%d no split", i)
		}
	}
}
*/
