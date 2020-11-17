package gocassa

import (
	"bytes"
	"fmt"
)

// Modifiers are used with update statements.

// ModifierOp represents a modifier operand
type ModifierOp int

const (
	// These modifier types represent the field modification operations
	// on list/map types such as append/remove/map set to be used with
	// UPDATE CQL statements
	ModifierListPrepend      ModifierOp = iota // prepend to beginning of a list
	ModifierListAppend                         // append to the end of a list
	ModifierListSetAtIndex                     //	set a value for a specific list index
	ModifierListRemove                         // remove an item from the list
	ModifierMapSetFields                       // set values from the provided map
	ModifierMapSetField                        // update a value for a specific key
	ModifierCounterIncrement                   // increment a counter
)

type Modifier struct {
	op   ModifierOp
	args []interface{}
}

// Operation returns the operation this modifier represents
func (m Modifier) Operation() ModifierOp {
	return m.op
}

// Args provides the arguments for this operation when generating the CQL statement,
// the actual arguments will depend on the Operation that this modifier represents
//   - ModifierListPrepend returns 1 element with the value (interface{})
//     to be prepended
//	 - ModifierListAppend returns 1 element with the value (interface{})
//	   to be appended
// 	 - ModifierListSetAtIndex returns two elements, the index (int) and
//	   value (interface{}) to be set
// 	 - ModifierListRemove returns 1 element with the value (interface{})
//	   to be removed
//   - ModifierMapSetFields returns 1 element with a map (map[string]interface{})
//     with the keys and values to be set
//   - MapSetField returns 2 elements, the key (string) and value (interface{})
//     to be set in the underlying map
//   - ModifierCounterIncrement returns 1 element (int) with how much the value
//     should be incremented by (or decremented if the value is negative)
func (m Modifier) Args() []interface{} {
	return m.args
}

// ListPrepend prepends a value to the front of the list
func ListPrepend(value interface{}) Modifier {
	return Modifier{
		op:   ModifierListPrepend,
		args: []interface{}{value},
	}
}

// ListAppend appends a value to the end of the list
func ListAppend(value interface{}) Modifier {
	return Modifier{
		op:   ModifierListAppend,
		args: []interface{}{value},
	}
}

// ListSetAtIndex sets the list element at a given index to a given value
func ListSetAtIndex(index int, value interface{}) Modifier {
	return Modifier{
		op:   ModifierListSetAtIndex,
		args: []interface{}{index, value},
	}
}

// ListRemove removes all elements from a list having a particular value
func ListRemove(value interface{}) Modifier {
	return Modifier{
		op:   ModifierListRemove,
		args: []interface{}{value},
	}
}

// MapSetFields updates the map with keys and values in the given map
func MapSetFields(fields map[string]interface{}) Modifier {
	return Modifier{
		op:   ModifierMapSetFields,
		args: []interface{}{fields},
	}
}

// MapSetField updates the map with the given key and value
func MapSetField(key, value interface{}) Modifier {
	return Modifier{
		op:   ModifierMapSetField,
		args: []interface{}{key, value},
	}
}

// CounterIncrement increments the value of the counter with the given value.
// Negative value results in decrementing.
func CounterIncrement(value int) Modifier {
	return Modifier{
		op:   ModifierCounterIncrement,
		args: []interface{}{value},
	}
}

func (m Modifier) cql(name string) (string, []interface{}) {
	str := ""
	vals := []interface{}{}
	switch m.op {
	case ModifierListPrepend:
		str = fmt.Sprintf("%s = ? + %s", name, name)
		vals = append(vals, []interface{}{m.args[0]})
	case ModifierListAppend:
		str = fmt.Sprintf("%s = %s + ?", name, name)
		vals = append(vals, []interface{}{m.args[0]})
	case ModifierListSetAtIndex:
		str = fmt.Sprintf("%s[?] = ?", name)
		vals = append(vals, m.args[0], m.args[1])
	case ModifierListRemove:
		str = fmt.Sprintf("%s = %s - ?", name, name)
		vals = append(vals, []interface{}{m.args[0]})
	case ModifierMapSetFields:
		fields, ok := m.args[0].(map[string]interface{})
		if !ok {
			panic(fmt.Sprintf("Argument for MapSetFields is not a map: %v", m.args[0]))
		}

		buf := new(bytes.Buffer)
		i := 0
		for k, v := range fields {
			if i > 0 {
				buf.WriteString(", ")
			}

			fieldStmt, fieldVals := MapSetField(k, v).cql(name)
			buf.WriteString(fieldStmt)
			vals = append(vals, fieldVals...)

			i++
		}
		str = buf.String()
	case ModifierMapSetField:
		str = fmt.Sprintf("%s[?] = ?", name)
		vals = append(vals, m.args[0], m.args[1])
	case ModifierCounterIncrement:
		val := m.args[0].(int)
		if val > 0 {
			str = fmt.Sprintf("%s = %s + ?", name, name)
			vals = append(vals, val)
		} else {
			str = fmt.Sprintf("%s = %s - ?", name, name)
			vals = append(vals, -val)
		}
	}
	return str, vals
}
