package gocassa

import "time"

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
	keyspace string        // name of the keyspace
	table    string        // name of the table
	fields   []string      // fields to be inserted
	values   []string      // values to be inserted
	ttl      time.Duration // ttl of the row
}

// UpdateStatement represents an UPDATE query to update some data in C*
// It satisfies the Statement interface
type UpdateStatement struct {
	keyspace string        // name of the keyspace
	table    string        // name of the table
	fields   []string      // fields to be updated
	values   []string      // values to be updated
	where    []Relation    // where filter clauses
	ttl      time.Duration // ttl of the row
}

// DeleteStatement represents a DELETE query to delete some data in C*
// It satisfies the Statement interface
type DeleteStatement struct {
	keyspace string     // name of the keyspace
	table    string     // name of the table
	where    []Relation // where filter clauses
}

// noOpStatement represents a statement that doesn't perform any specific
// query. It's used internally for testing, satisfies the Statement interface
type noOpStatement struct{}

func (_ noOpStatement) Query() string { return "" }

func (_ noOpStatement) Values() []interface{} { return []interface{}{} }
