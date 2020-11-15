package gocassa

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStatement(t *testing.T) {
	query1 := "DROP KEYSPACE IF EXISTS fakekeyspace"
	stmtPlain := newStatement(query1, []interface{}{})
	if stmtPlain.Query() != query1 {
		t.Fatalf("expected query '%s', got '%s'", query1, stmtPlain.Query())
	}
	if len(stmtPlain.FieldNames()) != 0 {
		t.Fatalf("expected 0 fields, got %d fields", len(stmtPlain.FieldNames()))
	}
	if len(stmtPlain.Values()) != 0 {
		t.Fatalf("expected 0 values, got %d values", len(stmtPlain.FieldNames()))
	}

	query2 := "DELETE FROM fakekeyspace.faketable WHERE id = ?"
	stmtWithValues := newStatement(query2, []interface{}{"id_abcd1234"})
	if stmtWithValues.Query() != query2 {
		t.Fatalf("expected query '%s', got '%s'", query2, stmtWithValues.Query())
	}
	if len(stmtWithValues.FieldNames()) != 0 {
		t.Fatalf("expected 0 fields, got %d fields", len(stmtWithValues.FieldNames()))
	}
	if len(stmtWithValues.Values()) != 1 {
		t.Fatalf("expected 1 value, got %d values", len(stmtWithValues.FieldNames()))
	}

	query3 := "SELECT id, name, created FROM fakekeyspace.faketable WHERE id = ?"
	stmtSelect := newSelectStatement(query3, []interface{}{"id_abcd1234"}, []string{"id", "name", "created"})
	if stmtSelect.Query() != query3 {
		t.Fatalf("expected query '%s', got '%s'", query3, stmtSelect.Query())
	}
	if len(stmtSelect.FieldNames()) != 3 {
		t.Fatalf("expected 3 fields, got %d fields", len(stmtSelect.FieldNames()))
	}
	if len(stmtSelect.Values()) != 1 {
		t.Fatalf("expected 1 value, got %d values", len(stmtSelect.FieldNames()))
	}
}

func TestUpdateStatement(t *testing.T) {
	stmt := UpdateStatement{keyspace: "ks1", table: "tbl1"}
	stmt.fieldMap = map[string]interface{}{"a": "b"}
	assert.Equal(t, "UPDATE ks1.tbl1 SET a = ?", stmt.Query())
	assert.Equal(t, []interface{}{"b"}, stmt.Values())

	stmt.fieldMap = map[string]interface{}{"a": "b", "c": ListAppend("d")}
	assert.Equal(t, "UPDATE ks1.tbl1 SET a = ?, c = c + ?", stmt.Query())
	assert.Equal(t, []interface{}{"b", []interface{}{"d"}}, stmt.Values())

	stmt.fieldMap = map[string]interface{}{"a": "b", "c": "d"}
	assert.Equal(t, "UPDATE ks1.tbl1 SET a = ?, c = ?", stmt.Query())
	assert.Equal(t, []interface{}{"b", "d"}, stmt.Values())

	stmt.where = []Relation{
		Eq("foo", "bar"),
		In("baz", "a", "b", "c"),
	}
	assert.Equal(t, "UPDATE ks1.tbl1 SET a = ?, c = ? WHERE foo = ? AND baz IN ?", stmt.Query())
	assert.Equal(t, []interface{}{"b", "d", "bar", []interface{}{"a", "b", "c"}}, stmt.Values())

	stmt.ttl = 1 * time.Hour
	assert.Equal(t, "UPDATE ks1.tbl1 USING TTL ? SET a = ?, c = ? WHERE foo = ? AND baz IN ?", stmt.Query())
	assert.Equal(t, []interface{}{3600, "b", "d", "bar", []interface{}{"a", "b", "c"}}, stmt.Values())
}

func TestDeleteStatement(t *testing.T) {
	stmt := DeleteStatement{keyspace: "ks1", table: "tbl1"}
	assert.Equal(t, "DELETE FROM ks1.tbl1", stmt.Query())
	assert.Equal(t, []interface{}{}, stmt.Values())

	stmt.where = []Relation{
		Eq("foo", "bar"),
	}
	assert.Equal(t, "DELETE FROM ks1.tbl1 WHERE foo = ?", stmt.Query())
	assert.Equal(t, []interface{}{"bar"}, stmt.Values())

	stmt.where = []Relation{
		Eq("foo", "bar"),
		In("baz", "a", "b", "c"),
	}
	assert.Equal(t, "DELETE FROM ks1.tbl1 WHERE foo = ? AND baz IN ?", stmt.Query())
	assert.Equal(t, []interface{}{"bar", []interface{}{"a", "b", "c"}}, stmt.Values())
}

func TestGenerateWhereCQL(t *testing.T) {
	stmt, values := generateWhereCQL([]Relation{
		Eq("foo", "bar"),
	})
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, []interface{}{"bar"}, values)

	stmt, values = generateWhereCQL([]Relation{
		Eq("foo", "bar"),
		In("baz", "a", "b", "c"),
	})
	assert.Equal(t, "foo = ? AND baz IN ?", stmt)
	assert.Equal(t, []interface{}{"bar", []interface{}{"a", "b", "c"}}, values)
}

func TestGenerateRelationCQL(t *testing.T) {
	stmt, value := generateRelationCQL(Eq("foo", "bar"))
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, "bar", value)

	stmt, value = generateRelationCQL(Eq("FoO", "BAR"))
	assert.Equal(t, "foo = ?", stmt)
	assert.Equal(t, "BAR", value)

	stmt, value = generateRelationCQL(In("foo", "a", "b", "c"))
	assert.Equal(t, "foo IN ?", stmt)
	assert.Equal(t, []interface{}{"a", "b", "c"}, value)

	stmt, value = generateRelationCQL(GT("foo", 1))
	assert.Equal(t, "foo > ?", stmt)
	assert.Equal(t, 1, value)

	stmt, value = generateRelationCQL(GTE("foo", 1))
	assert.Equal(t, "foo >= ?", stmt)
	assert.Equal(t, 1, value)

	stmt, value = generateRelationCQL(LT("foo", 1))
	assert.Equal(t, "foo < ?", stmt)
	assert.Equal(t, 1, value)

	stmt, value = generateRelationCQL(LTE("foo", 1))
	assert.Equal(t, "foo <= ?", stmt)
	assert.Equal(t, 1, value)

	assert.PanicsWithValue(t, "unknown comparator -1", func() {
		stmt, value = generateRelationCQL(Relation{cmp: -1})
	})
}
