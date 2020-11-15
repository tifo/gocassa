package gocassa

import (
	"fmt"
	"strings"
	"time"
)

type statement struct {
	fieldNames []string

	values []interface{}
	query  string
}

// FieldNames contains the column names which will be selected
// This will only be populated for SELECT queries
func (s statement) FieldNames() []string {
	return s.fieldNames
}

// Values encapsulates binding values to be set within the CQL
// query string as binding parameters. If there are no binding
// parameters in the query, this will be the empty slice
func (s statement) Values() []interface{} {
	return s.values
}

// Query returns the CQL query for this statement
func (s statement) Query() string {
	return s.query
}

func newStatement(query string, values []interface{}) statement {
	return statement{
		query:  query,
		values: values,
	}
}

func newSelectStatement(query string, values []interface{}, fieldNames []string) statement {
	return statement{
		query:      query,
		values:     values,
		fieldNames: fieldNames,
	}
}

// SelectStatement represents a read (SELECT) query for some data in C*
// It satisfies the Statement interface
type SelectStatement struct {
	keyspace       string                  // name of the keyspace
	table          string                  // name of the table
	fields         []string                // list of fields we want to select
	where          []Relation              // where filter clauses
	order          []ClusteringOrderColumn // order by clauses
	limit          int                     // limit count, 0 means no limit
	allowFiltering bool                    // whether we should allow filtering
}

// InsertStatement represents an INSERT query to write some data in C*
// It satisfies the Statement interface
type InsertStatement struct {
	keyspace string                 // name of the keyspace
	table    string                 // name of the table
	fieldMap map[string]interface{} // fields to be inserted
	ttl      time.Duration          // ttl of the row
}

// Query provides the CQL query string for an UPDATE query
func (s InsertStatement) Query() string {
	query, _ := s.queryAndValues()
	return query
}

// Values provide the binding values for an UPDATE query
func (s InsertStatement) Values() []interface{} {
	_, values := s.queryAndValues()
	return values
}

func (s InsertStatement) queryAndValues() (string, []interface{}) {
	query := []string{"INSERT INTO", fmt.Sprintf("%s.%s", s.keyspace, s.table)}

	fieldNames := make([]string, 0, len(s.fieldMap))
	placeholders := make([]string, 0, len(s.fieldMap))
	values := make([]interface{}, 0, len(s.fieldMap))
	for _, field := range sortedKeys(s.fieldMap) {
		fieldNames = append(fieldNames, strings.ToLower(field))
		placeholders = append(placeholders, "?")
		values = append(values, s.fieldMap[field])
	}

	query = append(query, "("+strings.Join(fieldNames, ", ")+")")
	query = append(query, "VALUES ("+strings.Join(placeholders, ", ")+")")

	// Determine if we need to set a TTL
	if s.ttl > 0 {
		query = append(query, "USING TTL ?")
		values = append(values, int(s.ttl.Seconds()))
	}

	return strings.Join(query, " "), values
}

// UpdateStatement represents an UPDATE query to update some data in C*
// It satisfies the Statement interface
type UpdateStatement struct {
	keyspace string                 // name of the keyspace
	table    string                 // name of the table
	fieldMap map[string]interface{} // fields to be updated
	where    []Relation             // where filter clauses
	ttl      time.Duration          // ttl of the row
}

// Query provides the CQL query string for an UPDATE query
func (s UpdateStatement) Query() string {
	query, _ := s.queryAndValues()
	return query
}

// Values provide the binding values for an UPDATE query
func (s UpdateStatement) Values() []interface{} {
	_, values := s.queryAndValues()
	return values
}

func (s UpdateStatement) queryAndValues() (string, []interface{}) {
	values := make([]interface{}, 0)
	query := []string{"UPDATE", fmt.Sprintf("%s.%s", s.keyspace, s.table)}

	// Determine if we need to set a TTL
	if s.ttl > 0 {
		query = append(query, "USING TTL ?")
		values = append(values, int(s.ttl.Seconds()))
	}

	setCQL, setValues := generateUpdateSetCQL(s.fieldMap)
	query = append(query, "SET", setCQL)
	values = append(values, setValues...)

	whereCQL, whereValues := generateWhereCQL(s.where)
	if whereCQL != "" {
		query = append(query, "WHERE", whereCQL)
		values = append(values, whereValues...)
	}
	return strings.Join(query, " "), values
}

// DeleteStatement represents a DELETE query to delete some data in C*
// It satisfies the Statement interface
type DeleteStatement struct {
	keyspace string     // name of the keyspace
	table    string     // name of the table
	where    []Relation // where filter clauses
}

// Query provides the CQL query string for a DELETE query
func (s DeleteStatement) Query() string {
	query, _ := s.queryAndValues()
	return query
}

// Values provide the binding values for a DELETE query
func (s DeleteStatement) Values() []interface{} {
	_, values := s.queryAndValues()
	return values
}

func (s DeleteStatement) queryAndValues() (string, []interface{}) {
	whereCQL, whereValues := generateWhereCQL(s.where)
	query := fmt.Sprintf("DELETE FROM %s.%s", s.keyspace, s.table)
	if whereCQL != "" {
		query += " WHERE " + whereCQL
	}
	return query, whereValues
}

// noOpStatement represents a statement that doesn't perform any specific
// query. It's used internally for testing, satisfies the Statement interface
type noOpStatement struct{}

func (_ noOpStatement) Query() string { return "" }

func (_ noOpStatement) Values() []interface{} { return []interface{}{} }

// generateUpdateSetCQL takes in a field map and generates the comma separated
// SET syntax. An expected output may be something like:
// 	- "foo = ?", {1}
// 	- "foo = ?, bar = ?", {1, 2}
func generateUpdateSetCQL(fm map[string]interface{}) (string, []interface{}) {
	clauses, values := make([]string, 0, len(fm)), make([]interface{}, 0, len(fm))
	for _, fieldName := range sortedKeys(fm) {
		value := fm[fieldName]
		if modifier, ok := value.(Modifier); ok {
			stmt, vals := modifier.cql(fieldName)
			clauses = append(clauses, stmt)
			values = append(values, vals...)
			continue
		}
		clauses = append(clauses, fieldName+" = ?")
		values = append(values, value)
	}
	return strings.Join(clauses, ", "), values
}

// generateWhereCQL takes a list of relations and generates the CQL for
// a WHERE clause. An expected output may be something like:
//	- "foo = ?", {1}
//	- "foo = ? AND bar IN ?", {1, {"a", "b", "c"}}
func generateWhereCQL(rs []Relation) (string, []interface{}) {
	clauses, values := make([]string, 0, len(rs)), make([]interface{}, 0, len(rs))
	for _, relation := range rs {
		clause, bindValue := generateRelationCQL(relation)
		clauses = append(clauses, clause)
		values = append(values, bindValue)
	}
	return strings.Join(clauses, " AND "), values
}

func generateRelationCQL(rel Relation) (string, interface{}) {
	field := strings.ToLower(rel.Field())
	switch rel.Comparator() {
	case CmpEquality:
		return field + " = ?", rel.Terms()[0]
	case CmpIn:
		return field + " IN ?", rel.Terms()
	case CmpGreaterThan:
		return field + " > ?", rel.Terms()[0]
	case CmpGreaterThanOrEquals:
		return field + " >= ?", rel.Terms()[0]
	case CmpLesserThan:
		return field + " < ?", rel.Terms()[0]
	case CmpLesserThanOrEquals:
		return field + " <= ?", rel.Terms()[0]
	default:
		// This represents an invalid Comparator and would only manifest
		// if we've initialised a Relation incorrectly within this package
		panic(fmt.Sprintf("unknown comparator %v", rel.Comparator()))
	}
}
