package mapper

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"encoding/gob"
	"encoding/json"
)

type (
	// Query is a gob encodable specification of a datastore query
	// with only the mapper supported query features provided and
	// the addition ability to specify namespaces
	Query struct {
		namespaces []string
		kind       string
		filter     []filter
		keysOnly   bool
		err        error
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

var (
	operatorToString = map[operator]string{
		lessThan:    "<",
		lessEq:      "<=",
		equal:       "=",
		greaterEq:   ">=",
		greaterThan: ">",
	}
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

// Namespace returns a derivative query with a single namespace
func (q *Query) Namespace(namespace string) *Query {
	q = q.clone()
	q.namespaces = []string{namespace}
	return q
}

// Namespaces returns a derivative query with a collection of namespaces
func (q *Query) Namespaces(namespaces []string) *Query {
	q = q.clone()
	q.namespaces = append(q.namespaces, namespaces...)
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

func (q *Query) clone() *Query {
	x := *q
	// Copy the contents of the slice-typed fields to a new backing store.
	if len(q.namespaces) > 0 {
		x.namespaces = make([]string, len(q.namespaces))
		copy(x.namespaces, q.namespaces)
	}
	if len(q.filter) > 0 {
		x.filter = make([]filter, len(q.filter))
		copy(x.filter, q.filter)
	}
	return &x
}

// String returns a string representation of the query for display purposes only
func (q *Query) String() string {
	str := fmt.Sprintf("kind:%s namespace(s):%s (%d)", q.kind, strings.Join(q.namespaces, ","), len(q.namespaces))
	for _, f := range q.filter {
		str += fmt.Sprintf(" filter:%s", f.String())
	}
	return str
}

func (f *filter) String() string {
	return fmt.Sprintf("%s %s %v", f.FieldName, operatorToString[f.Op], f.Value)
}

func (q *Query) GobEncode() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(q.namespaces); err != nil {
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
	if err := dec.Decode(&q.namespaces); err != nil {
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
		Namespaces []string `json:"namespaces"`
		Kind       string   `json:"kind"`
		Filter     []filter `json:"filter"`
		KeysOnly   bool     `json:"keys_only"`
	}{
		Namespaces: q.namespaces,
		Kind:       q.kind,
		Filter:     q.filter,
		KeysOnly:   q.keysOnly,
	})
}

func (f *filter) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Field string      `json:"field"`
		Op    string      `json:"operator"`
		Value interface{} `json:"value"`
	}{
		Field: f.FieldName,
		Op:    operatorToString[f.Op],
		Value: f.Value,
	})
}
