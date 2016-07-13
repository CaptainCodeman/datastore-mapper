package mapper

import (
	"fmt"
	"reflect"
	"time"
)

type (
	// propertyRange is used to figure out how to split queries
	propertyRange struct {
		propertyName string
		propertyType reflect.Type
		lower        *filter
		upper        *filter
		equal        []filter
	}
)

// newPropertyRange gets the property range from query filters
// and validates that there is only one closed range on a single
// property
func newPropertyRange(q *Query) (*propertyRange, error) {
	pr := new(propertyRange)
	pr.equal = []filter{}

	for _, f := range q.filter {
		switch f.Op {
		case greaterThan:
			fallthrough
		case greaterEq:
			if pr.lower != nil {
				return nil, fmt.Errorf("only one lower property range allowed")
			}
			pr.lower = &filter{f.FieldName, f.Op, f.Value}
		case lessThan:
			fallthrough
		case lessEq:
			if pr.upper != nil {
				return nil, fmt.Errorf("only one upper property range allowed")
			}
			pr.upper = &filter{f.FieldName, f.Op, f.Value}
		case equal:
			pr.equal = append(pr.equal, f)
		}
	}

	// no range
	if pr.lower == nil && pr.upper == nil {
		return pr, nil
	}

	// only one end of range defined
	if pr.lower == nil || pr.upper == nil {
		return nil, fmt.Errorf("filter should contain a complete range")
	}

	// different property range
	if pr.lower.FieldName != pr.upper.FieldName {
		return nil, fmt.Errorf("property range must be for a single property")
	}

	if pr.lower.Value == nil || pr.upper.Value == nil {
		return nil, fmt.Errorf("property range values must not be nil")
	}

	lowerType := reflect.TypeOf(pr.lower.Value)
	upperType := reflect.TypeOf(pr.upper.Value)
	if lowerType != upperType {
		return nil, fmt.Errorf("property range types must be the same")
	}

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
		// TODO: handle geo point (?)
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
		return nil, fmt.Errorf("start value should be smaller than end value")
	}

	pr.propertyName = pr.lower.FieldName
	pr.propertyType = lowerType

	return pr, nil
}

func (pr *propertyRange) split(splits int) []*propertyRange {
	/*
		lowerValue := reflect.ValueOf(pr.lower.Value)
		upperValue := reflect.ValueOf(pr.upper.Value)

		switch lowerValue.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			splitPoints := splitRangeInt64(lowerValue.Int(), upperValue.Int(), splits)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			splitPoints := splitRangeUint64(lowerValue.Uint(), upperValue.Uint(), splits)
		case reflect.Float32, reflect.Float64:
			splitPoints := splitRangeFloat64(lowerValue.Float(), upperValue.Float(), splits)
		case reflect.String:
			splitPoints := splitRangeString(lowerValue.String(), upperValue.String(), splits)
		case reflect.Struct:
			switch lowerValue.Type() {
			case reflect.TypeOf(time.Time{}):
				lv := lowerValue.Interface().(time.Time)
				uv := upperValue.Interface().(time.Time)
				splitPoints := splitRangeTime(lv, uv, splits)
			}
		}
	*/
	return nil
}
