package gocassa

import (
	"fmt"
	"reflect"
	"time"
)

// Comparator represents a comparison operand
type Comparator int

const (
	// These comparison types represent the comparison types supported
	// when generating a relation between a field and it's terms
	CmpEquality            Comparator = iota // direct equality (foo = bar)
	CmpIn                                    // membership (foo IN (bar, bing, baz))
	CmpGreaterThan                           // larger than (foo > 1)
	CmpGreaterThanOrEquals                   // larger than or equal (foo >= 1)
	CmpLesserThan                            // less than (foo < 1)
	CmpLesserThanOrEquals                    // less than or equal (foo <= 1)
)

// Relation describes the comparison of a field against a list of terms
// that need to satisfy a comparator
type Relation struct {
	cmp   Comparator
	field string
	// terms represents the list of terms on the right hand side to match
	// against. It is expected that all comparators except the CmpIn have
	// exactly one term.
	terms []interface{}
}

// Field provides the field name for this relation
func (r Relation) Field() string {
	return r.field
}

// Comparator provides the comparator for this relation
func (r Relation) Comparator() Comparator {
	return r.cmp
}

// Terms provides a list of values to compare against. A valid relation
// will always have at least one term present
func (r Relation) Terms() []interface{} {
	return r.terms
}

func anyEquals(value interface{}, terms []interface{}) bool {
	primVal := convertToPrimitive(value)
	for _, term := range terms {
		if primVal == convertToPrimitive(term) {
			return true
		}
	}
	return false
}

func convertToPrimitive(i interface{}) interface{} {
	switch v := i.(type) {
	case time.Time:
		return v.UnixNano()
	case time.Duration:
		return v.Nanoseconds()
	case []byte:
		// This case works as strings in Go are simply defined as the following:
		// "A string value is a (possibly empty) sequence of bytes" (from the go lang spec)
		// and
		// "Converting a slice of bytes to a string type yields a string whose successive bytes are the elements of the slice."
		// Finally:
		// "String values are comparable and ordered, lexically byte-wise."
		// We mostly want this to allow comparisons of blob types in the primary key of a table,
		// since []byte are not `==` comparable in go, but strings are
		return string(v)
	default:
		// If the underlying type is a string, we want to represent this value
		// as a string for comparison across proxy types.
		if reflect.ValueOf(i).Kind() == reflect.String {
			return fmt.Sprintf("%v", i)
		}
		return i
	}
}

func (r Relation) accept(i interface{}) bool {
	var result bool
	var err error

	if r.Comparator() == CmpEquality || r.Comparator() == CmpIn {
		return anyEquals(i, r.Terms())
	}

	a, b := convertToPrimitive(i), convertToPrimitive(r.Terms()[0])

	switch r.Comparator() {
	case CmpGreaterThan:
		result, err = builtinGreaterThan(a, b)
	case CmpGreaterThanOrEquals:
		result, err = builtinGreaterThan(a, b)
		result = result || a == b
	case CmpLesserThanOrEquals:
		result, err = builtinLessThan(a, b)
		result = result || a == b
	case CmpLesserThan:
		result, err = builtinLessThan(a, b)
	}

	return err == nil && result
}

func toI(i interface{}) []interface{} {
	return []interface{}{i}
}

func Eq(field string, term interface{}) Relation {
	return Relation{
		cmp:   CmpEquality,
		field: field,
		terms: toI(term),
	}
}

// In allows a field to be queried with multiple terms simultaneously
// Note: In should only be used for Primary Key columns. Usage for
// clustering key columns may result in an error depending on backing
// storage implementation
func In(field string, terms ...interface{}) Relation {
	return Relation{
		cmp:   CmpIn,
		field: field,
		terms: terms,
	}
}

func GT(field string, term interface{}) Relation {
	return Relation{
		cmp:   CmpGreaterThan,
		field: field,
		terms: toI(term),
	}
}

func GTE(field string, term interface{}) Relation {
	return Relation{
		cmp:   CmpGreaterThanOrEquals,
		field: field,
		terms: toI(term),
	}
}

func LT(field string, term interface{}) Relation {
	return Relation{
		cmp:   CmpLesserThan,
		field: field,
		terms: toI(term),
	}
}

func LTE(field string, term interface{}) Relation {
	return Relation{
		cmp:   CmpLesserThanOrEquals,
		field: field,
		terms: toI(term),
	}
}
