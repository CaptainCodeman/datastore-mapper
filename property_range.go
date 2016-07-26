package mapper

import (
	"fmt"
	"reflect"
	"time"
)

// TODO: this assumes that index keys are always ascending, it's possible that
// an index exists but it's in descending order so the range logic would need
// to take that into account - but that's a story for a future episode ...

type (
	// propertyRange represents a filter range for a single property
	propertyRange struct {
		lower *filter
		upper *filter
	}
)

// newPropertyRange creates a property range
func newPropertyRange(lower, upper *filter) (*propertyRange, error) {
	pr := &propertyRange{lower, upper}
	return pr, pr.validate()
}

func (pr *propertyRange) empty() bool {
	if pr.lower == nil && pr.upper == nil {
		return true
	}
	return false
}

// validate checks that the property range is valid
func (pr *propertyRange) validate() error {
	// no range is valid
	if pr.lower == nil && pr.upper == nil {
		return nil
	}

	// lower must use a lower range operator
	if pr.lower != nil && (pr.lower.Op != greaterEq && pr.lower.Op != greaterThan) {
		return fmt.Errorf("invalid lower range op %s", pr.lower.Op)
	}

	// upper must use an upper range operator
	if pr.upper != nil && (pr.upper.Op != lessEq && pr.upper.Op != lessThan) {
		return fmt.Errorf("invalid upper range op %s", pr.upper.Op)
	}

	// a single ended range is valid
	if pr.lower == nil || pr.upper == nil {
		return nil
	}

	// both defined must be for same property name
	if pr.lower.FieldName != pr.upper.FieldName {
		return fmt.Errorf("range must be on a single property, found %s and %s", pr.lower.FieldName, pr.upper.FieldName)
	}

	// and have the same value type
	if reflect.TypeOf(pr.lower.Value) != reflect.TypeOf(pr.upper.Value) {
		return fmt.Errorf("property range types must be the same")
	}

	// and make sense (lower value must be lower than the upper value)
	isLower := false
	lowerValue := reflect.ValueOf(pr.lower.Value)
	upperValue := reflect.ValueOf(pr.upper.Value)
	switch lowerValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if lowerValue.Int() < upperValue.Int() {
			isLower = true
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if lowerValue.Uint() < upperValue.Uint() {
			isLower = true
		}
	case reflect.Float32, reflect.Float64:
		if lowerValue.Float() < upperValue.Float() {
			isLower = true
		}
	case reflect.String:
		if lowerValue.String() < upperValue.String() {
			isLower = true
		}
	case reflect.Struct:
		// TODO: handle datastore geo, key, blobkey etc...
		switch lowerValue.Type() {
		case reflect.TypeOf(time.Time{}):
			lv := lowerValue.Interface().(time.Time)
			uv := upperValue.Interface().(time.Time)
			if lv.Before(uv) {
				isLower = true
			}
		}
	}
	if !isLower {
		return fmt.Errorf("lower value should be smaller than upper value, found %v %v", lowerValue, upperValue)
	}

	return nil
}

func (pr *propertyRange) propertyName() string {
	if pr.lower != nil {
		return pr.lower.FieldName
	}
	if pr.upper != nil {
		return pr.upper.FieldName
	}
	return ""
}

// toEqualityListAndRange separates the equality filters from
// any property range and validates that there is at most a single
// closed range on a single property
func (q *Query) toEqualityListAndRange() ([]filter, *propertyRange, error) {
	pr, _ := newPropertyRange(nil, nil)
	equality := []filter{}

	for _, f := range q.filter {
		switch f.Op {
		case greaterThan:
			fallthrough
		case greaterEq:
			if pr.lower != nil {
				return nil, nil, fmt.Errorf("only one lower property range allowed")
			}
			pr.lower = &filter{f.FieldName, f.Op, f.Value}
		case lessThan:
			fallthrough
		case lessEq:
			if pr.upper != nil {
				return nil, nil, fmt.Errorf("only one upper property range allowed")
			}
			pr.upper = &filter{f.FieldName, f.Op, f.Value}
		case equal:
			equality = append(equality, f)
		default:
			return nil, nil, fmt.Errorf("unsupported operator %s", f.Op)
		}
	}

	return equality, pr, pr.validate() // and empty property range will be valid
}

// split divides the property range into n splits
func (pr *propertyRange) split(splits int) []*propertyRange {
	lowerValue := reflect.ValueOf(pr.lower.Value)
	upperValue := reflect.ValueOf(pr.upper.Value)

	var values []interface{}
	// um, yeah ... should maybe have some array of functions for types or something clever
	switch lowerValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		values = pr.splitInt64(splitRangeInt64(lowerValue.Int(), upperValue.Int(), splits))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		values = pr.splitUint64(splitRangeUint64(lowerValue.Uint(), upperValue.Uint(), splits))
	case reflect.Float32, reflect.Float64:
		values = pr.splitFloat64(splitRangeFloat64(lowerValue.Float(), upperValue.Float(), splits))
	case reflect.String:
		values = pr.splitString(splitRangeString(lowerValue.String(), upperValue.String(), splits))
	case reflect.Struct:
		// TODO: handle datastore geo, key, blobkey etc...
		switch lowerValue.Type() {
		case reflect.TypeOf(time.Time{}):
			lv := lowerValue.Interface().(time.Time)
			uv := upperValue.Interface().(time.Time)
			values = pr.splitTime(splitRangeTime(lv, uv, splits))
		}
	}
	return pr.boundariesToRanges(values)
}

// boundariesToRanges converts the list of boundaries into a list
// of propertyRange, each with their own lower and upper bound
func (pr *propertyRange) boundariesToRanges(values []interface{}) []*propertyRange {
	results := make([]*propertyRange, 0, len(values))
	fieldName := pr.lower.FieldName
	for i := 1; i < len(values); i++ {
		switch i {
		case 0:
			// first
			results = append(results, &propertyRange{
				lower: pr.lower,
				upper: &filter{fieldName, lessThan, values[i]},
			})
		case len(values) - 1:
			// last
			results = append(results, &propertyRange{
				lower: &filter{fieldName, greaterEq, values[i-1]},
				upper: pr.upper,
			})
		default:
			// others
			results = append(results, &propertyRange{
				lower: &filter{fieldName, greaterEq, values[i-1]},
				upper: &filter{fieldName, lessThan, values[i]},
			})
		}
	}

	return results
}

// these methods convert the typed boundary ranges to slices of interface{}
// for use with the boundariesToRanges function above so we don't duplicate
// that logic ... nope, I'd still rather do this than have generics
func (pr *propertyRange) splitInt64(values []int64) []interface{} {
	new := make([]interface{}, len(values))
	for i, v := range values {
		new[i] = v
	}
	return new
}

func (pr *propertyRange) splitUint64(values []uint64) []interface{} {
	new := make([]interface{}, len(values))
	for i, v := range values {
		new[i] = v
	}
	return new
}

func (pr *propertyRange) splitFloat64(values []float64) []interface{} {
	new := make([]interface{}, len(values))
	for i, v := range values {
		new[i] = v
	}
	return new
}

func (pr *propertyRange) splitString(values []string) []interface{} {
	new := make([]interface{}, len(values))
	for i, v := range values {
		new[i] = v
	}
	return new
}

func (pr *propertyRange) splitTime(values []time.Time) []interface{} {
	new := make([]interface{}, len(values))
	for i, v := range values {
		new[i] = v
	}
	return new
}
