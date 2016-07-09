package mapper

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"

	"encoding/gob"
	"encoding/json"

	"google.golang.org/appengine/datastore"
)

type (
	// Query is a gob encodable specification of a datastore query
	// with only the mapper supported query features provided and
	// the addition ability to specify namespaces
	Query struct {
		namespace []string
		kind      string
		filter    []filter
		keysOnly  bool
		limit     int32
		err       error
	}

	// filter is a conditional filter on query results.
	filter struct {
		FieldName string
		Op        operator
		Value     interface{}
	}

	operator int
)

const (
	lessThan operator = iota
	lessEq
	equal
	greaterEq
	greaterThan
)

func init() {
	gob.Register(&Query{})
}

// NewQuery created a new Query
func NewQuery(kind string) *Query {
	return &Query{
		kind: kind,
	}
}

// Namespace returns a derivative query with a namespace-based filter.
func (q *Query) Namespace(namespace string) *Query {
	q = q.clone()
	q.namespace = append(q.namespace, namespace)
	return q
}

// KeysOnly returns a derivative query that yields only keys, not keys and
// entities. It cannot be used with projection queries.
func (q *Query) KeysOnly() *Query {
	q = q.clone()
	q.keysOnly = true
	return q
}

// Filter returns a derivative query with a field-based filter.
// The filterStr argument must be a field name followed by optional space,
// followed by an operator, one of ">", "<", ">=", "<=", or "=".
// Fields are compared against the provided value using the operator.
// Multiple filters are AND'ed together.
func (q *Query) Filter(filterStr string, value interface{}) *Query {
	q = q.clone()
	filterStr = strings.TrimSpace(filterStr)
	if len(filterStr) < 1 {
		q.err = errors.New("datastore: invalid filter: " + filterStr)
		return q
	}
	f := filter{
		FieldName: strings.TrimRight(filterStr, " ><=!"),
		Value:     value,
	}
	switch op := strings.TrimSpace(filterStr[len(f.FieldName):]); op {
	case "<=":
		f.Op = lessEq
	case ">=":
		f.Op = greaterEq
	case "<":
		f.Op = lessThan
	case ">":
		f.Op = greaterThan
	case "=":
		f.Op = equal
	default:
		q.err = fmt.Errorf("datastore: invalid operator %q in filter %q", op, filterStr)
		return q
	}
	q.filter = append(q.filter, f)
	return q
}

// Limit returns a derivative query that has a limit on the number of results
// returned. A negative value means unlimited.
func (q *Query) Limit(limit int) *Query {
	q = q.clone()
	if limit < math.MinInt32 || limit > math.MaxInt32 {
		q.err = errors.New("datastore: query limit overflow")
		return q
	}
	q.limit = int32(limit)
	return q
}

func (q *Query) clone() *Query {
	x := *q
	// Copy the contents of the slice-typed fields to a new backing store.
	if len(q.namespace) > 0 {
		x.namespace = make([]string, len(q.namespace))
		copy(x.namespace, q.namespace)
	}
	if len(q.filter) > 0 {
		x.filter = make([]filter, len(q.filter))
		copy(x.filter, q.filter)
	}
	return &x
}

func (q *Query) toDatastoreQuery() *datastore.Query {
	dq := datastore.NewQuery(q.kind)
	for _, f := range q.filter {
		var op string
		switch f.Op {
		case lessEq:
			op = "<="
		case greaterEq:
			op = ">="
		case lessThan:
			op = "<"
		case greaterThan:
			op = ">"
		case equal:
			op = "="
		default:
			op = "?"
		}
		dq = dq.Filter(f.FieldName+" "+op, f.Value)
	}
	if q.keysOnly {
		dq = dq.KeysOnly()
	}
	return dq
}

func (q *Query) GobEncode() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(q.namespace); err != nil {
		return nil, err
	}
	if err := enc.Encode(q.kind); err != nil {
		return nil, err
	}
	if err := enc.Encode(q.filter); err != nil {
		return nil, err
	}
	if err := enc.Encode(q.keysOnly); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (q *Query) GobDecode(b []byte) error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&q.namespace); err != nil {
		return err
	}
	if err := dec.Decode(&q.kind); err != nil {
		return err
	}
	if err := dec.Decode(&q.filter); err != nil {
		return err
	}
	if err := dec.Decode(&q.keysOnly); err != nil {
		return err
	}
	return nil
}

func (q *Query) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Namespace []string `json:"namespace"`
		Kind      string   `json:"kind"`
		Filter    []filter `json:"filter"`
		KeysOnly  bool     `json:"keys_only"`
	}{
		Namespace: q.namespace,
		Kind:      q.kind,
		Filter:    q.filter,
		KeysOnly:  q.keysOnly,
	})
}

func (f *filter) MarshalJSON() ([]byte, error) {
	var op string
	switch f.Op {
	case lessEq:
		op = "<="
	case greaterEq:
		op = ">="
	case lessThan:
		op = "<"
	case greaterThan:
		op = ">"
	case equal:
		op = "="
	default:
		op = "?"
	}
	return json.Marshal(&struct {
		Field string      `json:"field"`
		Op    string      `json:"operator"`
		Value interface{} `json:"value"`
	}{
		Field: f.FieldName,
		Op:    op,
		Value: f.Value,
	})
}
