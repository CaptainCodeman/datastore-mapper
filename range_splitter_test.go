package mapper

import (
	"testing"
	"time"

	"math/big"
)

func TestSplitInt64s(t *testing.T) {
	tests := []struct {
		start    int64
		end      int64
		splits   int
		expected []int64
	}{
		{0, 1, 2, []int64{0, 1}},
		{0, 4, 8, []int64{0, 1, 2, 3, 4}},
		{0, 100, 2, []int64{0, 50, 100}},
		{0, 100, 3, []int64{0, 33, 67, 100}},
	}
	for i, test := range tests {
		results := splitRangeInt64(test.start, test.end, test.splits)
		if len(results) != len(test.expected) {
			t.Errorf("%d expect %d, got %d", i, len(test.expected), len(results))
			continue
		}
		for j, v := range results {
			if v != test.expected[j] {
				t.Errorf("%d.%d expect %d, got %d", i, j, test.expected[j], v)
			}
		}
	}
}

func TestSplitFloat64s(t *testing.T) {
	tests := []struct {
		start    float64
		end      float64
		splits   int
		expected []float64
	}{
		{0, 1, 2, []float64{0, 0.5, 1}},
		{0, 4, 8, []float64{0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4}},
		{0, 120, 2, []float64{0, 60, 120}},
		{0, 120, 3, []float64{0, 40, 80, 120}},
	}
	for i, test := range tests {
		results := splitRangeFloat64(test.start, test.end, test.splits)
		if len(results) != len(test.expected) {
			t.Errorf("%d expect %d, got %d", i, len(test.expected), len(results))
			continue
		}
		for j, v := range results {
			if v != test.expected[j] {
				t.Errorf("%d.%d expect %f, got %f", i, j, test.expected[j], v)
			}
		}
	}
}

func TestSplitTime(t *testing.T) {
	tests := []struct {
		start    time.Time
		end      time.Time
		splits   int
		expected []time.Time
	}{
		{ // years
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2011, 1, 1, 0, 0, 0, 9, time.UTC),
			2,
			[]time.Time{
				time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2005, 7, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2011, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		{ // quarters
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2001, 1, 1, 0, 0, 0, 9, time.UTC),
			4,
			[]time.Time{
				time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2000, 4, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2000, 7, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2000, 10, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		{ // months
			time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2001, 1, 1, 0, 0, 0, 9, time.UTC),
			12,
			[]time.Time{
				time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2000, 1, 31, 12, 0, 0, 0, time.UTC),
				time.Date(2000, 3, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2000, 4, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2000, 5, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2000, 6, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2000, 7, 2, 0, 0, 0, 0, time.UTC),
				time.Date(2000, 8, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2000, 9, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2000, 10, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2000, 11, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2000, 12, 1, 12, 0, 0, 0, time.UTC),
				time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		{ // days
			time.Date(2016, 7, 11, 0, 0, 0, 0, time.UTC),
			time.Date(2016, 7, 18, 0, 0, 0, 9, time.UTC),
			7,
			[]time.Time{
				time.Date(2016, 7, 11, 0, 0, 0, 0, time.UTC),
				time.Date(2016, 7, 12, 0, 0, 0, 0, time.UTC),
				time.Date(2016, 7, 13, 0, 0, 0, 0, time.UTC),
				time.Date(2016, 7, 14, 0, 0, 0, 0, time.UTC),
				time.Date(2016, 7, 15, 0, 0, 0, 0, time.UTC),
				time.Date(2016, 7, 16, 0, 0, 0, 0, time.UTC),
				time.Date(2016, 7, 17, 0, 0, 0, 0, time.UTC),
				time.Date(2016, 7, 18, 0, 0, 0, 0, time.UTC),
			},
		},
	}
	for i, test := range tests {
		results := splitRangeTime(test.start, test.end, test.splits)
		if len(results) != len(test.expected) {
			t.Errorf("%d expect %d, got %d", i, len(test.expected), len(results))
			continue
		}
		for j, v := range results {
			if v != test.expected[j] {
				t.Errorf("%d.%d expect %s, got %s", i, j, test.expected[j].String(), v.String())
			}
		}
	}
}

func TestSplitString(t *testing.T) {
	setupConstants("abc", 3)
	/*
		The limited character set and length helps make the ordinal string ordering clearer,
		there are 40 different strings in the set - 1 (empty) + 3 x 13 combinations:

		"",
		"a", "aa", "aaa", "aab", "aac", "ab", "aba", "abb", "abc", "ac", "aca", "acb", "acc",
		"b", "ba", "baa", "bab", "bac", "bb", "bba", "bbb", "bbc", "bc", "bca", "bcb", "bcc",
		"c", "ca", "caa", "cab", "cac", "cb", "cba", "cbb", "cbc", "cc", "cca", "ccb", "ccc"

		... so divide 40 by the number of shards and pick every nth entry for the result.
		e.g. for 4 shards we would pick every 10th entry: "", "abc", "bb", "caa", "ccc"
	*/
	tests := []struct {
		start    string
		end      string
		splits   int
		expected []string
	}{
		{"", "ccc", 2, []string{"", "bb", "ccc"}},
		{"aaa", "aac", 2, []string{"aaa", "aab", "aac"}},
		{"", "ccc", 8, []string{"", "aab", "abc", "b", "bb", "bca", "caa", "cbb", "ccc"}},
		{"", "ccc", 4, []string{"", "abc", "bb", "caa", "ccc"}},
		{"aba", "abc", 2, []string{"aba", "abb", "abc"}},
	}

	for i, test := range tests {
		results := splitRangeString(test.start, test.end, test.splits)
		if len(results) != len(test.expected) {
			t.Errorf("%d expect %d, got %d", i, len(test.expected), len(results))
			continue
		}
		for j, v := range results {
			if v != test.expected[j] {
				t.Errorf("%d.%d expect %s, got %s", i, j, test.expected[j], v)
			}
		}
	}
}

func TestOrdinalization(t *testing.T) {
	setupConstants("ab", 2)
	tests := []struct {
		ordinal *big.Int
		value   string
	}{
		{big.NewInt(0), ""},
		{big.NewInt(1), "a"},
		{big.NewInt(2), "aa"},
		{big.NewInt(3), "ab"},
		{big.NewInt(4), "b"},
		{big.NewInt(5), "ba"},
		{big.NewInt(6), "bb"},
	}
	for i, test := range tests {
		if ns := ordToString(test.ordinal, 0); ns != test.value {
			t.Errorf("%d undefined %s failed - expected %s got %s", i, test.ordinal, test.value, ns)
		}

		if ord := stringToOrd(test.value); ord.Cmp(test.ordinal) != 0 {
			t.Errorf("%d stringToOrd %s failed - expected %s got %s", i, test.value, test.ordinal, ord)
		}
	}
}
