package mapper

import (
	"bytes"
	"errors"
	"fmt"
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
		selection  selection
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

	operator  int
	selection int
)

const (
	lessThan operator = iota
	lessEq
	equal
	greaterEq
	greaterThan
)

const (
	all selection = iota
	empty
	named
	selected
)

func init() {
	gob.Register(&datastore.Key{})
	gob.Register(&Query{})
}

func (op operator) String() string {
	switch op {
	case lessThan:
		return "<"
	case lessEq:
		return "<="
	case equal:
		return "="
	case greaterEq:
		return ">="
	case greaterThan:
		return ">"
	default:
		return "unknown"
	}
}

func (s selection) String() string {
	switch s {
	case all:
		return "all"
	case empty:
		return "empty"
	case named:
		return "named"
	case selected:
		return "selected"
	default:
		return "unknown"
	}
}

// NewQuery created a new Query
func NewQuery(kind string) *Query {
	return &Query{
		kind: kind,
	}
}

// NamespaceAll returns a derivative query specifying all namespaces
func (q *Query) NamespaceAll() *Query {
	q = q.clone()
	q.selection = all
	q.namespaces = []string{}
	return q
}

// NamespaceEmpty returns a derivative query specifying the empty namespace
func (q *Query) NamespaceEmpty() *Query {
	q = q.clone()
	q.selection = empty
	q.namespaces = []string{}
	return q
}

// NamespaceNamed returns a derivative query specifying the none-empty namespaces
func (q *Query) NamespaceNamed() *Query {
	q = q.clone()
	q.selection = named
	q.namespaces = []string{}
	return q
}

// Namespace returns a derivative query with a selection of namespaces
func (q *Query) Namespace(namespaces ...string) *Query {
	q = q.clone()
	q.selection = selected
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
	x.selection = q.selection
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
	str := fmt.Sprintf("kind:%s namespace(s):%s %s (%d)", q.kind, q.selection, strings.Join(q.namespaces, ","), len(q.namespaces))
	for _, f := range q.filter {
		str += fmt.Sprintf(" filter:%s", f.String())
	}
	return str
}

func (f *filter) String() string {
	return fmt.Sprintf("%s %s %v", f.FieldName, f.Op, f.Value)
}

func (q *Query) GobEncode() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(q.selection); err != nil {
		return nil, err
	}
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
	if err := dec.Decode(&q.selection); err != nil {
		return err
	}
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
		Selection  string   `json:"selection"`
		Namespaces []string `json:"namespaces"`
		Kind       string   `json:"kind"`
		Filter     []filter `json:"filter"`
		KeysOnly   bool     `json:"keys_only"`
	}{
		Selection:  q.selection.String(),
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
		Op:    f.Op.String(),
		Value: f.Value,
	})
}
