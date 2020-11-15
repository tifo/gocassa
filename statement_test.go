package gocassa

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSelectStatement(t *testing.T) {
	stmt := SelectStatement{keyspace: "ks1", table: "tbl1"}
	stmt.fields = []string{"a", "b", "c"}
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1", stmt.Query())
	assert.Equal(t, []interface{}{}, stmt.Values())

	stmt.limit = 10
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 LIMIT ?", stmt.Query())
	assert.Equal(t, []interface{}{10}, stmt.Values())

	stmt.order = []ClusteringOrderColumn{
		{Column: "a", Direction: ASC},
	}
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 ORDER BY a ASC LIMIT ?", stmt.Query())
	assert.Equal(t, []interface{}{10}, stmt.Values())

	stmt.where = []Relation{
		Eq("foo", "bar"),
		In("baz", "bing"),
	}
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 WHERE foo = ? AND baz IN ? ORDER BY a ASC LIMIT ?", stmt.Query())
	assert.Equal(t, []interface{}{"bar", []interface{}{"bing"}, 10}, stmt.Values())

	stmt.allowFiltering = true
	assert.Equal(t, "SELECT a, b, c FROM ks1.tbl1 WHERE foo = ? AND baz IN ? ORDER BY a ASC LIMIT ? ALLOW FILTERING", stmt.Query())
	assert.Equal(t, []interface{}{"bar", []interface{}{"bing"}, 10}, stmt.Values())
}

func TestInsertStatement(t *testing.T) {
	stmt := InsertStatement{keyspace: "ks1", table: "tbl1"}
	stmt.fieldMap = map[string]interface{}{"a": "b"}
	assert.Equal(t, "INSERT INTO ks1.tbl1 (a) VALUES (?)", stmt.Query())
	assert.Equal(t, []interface{}{"b"}, stmt.Values())

	stmt.fieldMap = map[string]interface{}{"a": "b", "c": "d"}
	assert.Equal(t, "INSERT INTO ks1.tbl1 (a, c) VALUES (?, ?)", stmt.Query())
	assert.Equal(t, []interface{}{"b", "d"}, stmt.Values())

	stmt.ttl = 1 * time.Hour
	assert.Equal(t, "INSERT INTO ks1.tbl1 (a, c) VALUES (?, ?) USING TTL ?", stmt.Query())
	assert.Equal(t, []interface{}{"b", "d", 3600}, stmt.Values())
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

func TestGenerateOrderByCQL(t *testing.T) {
	stmt := generateOrderByCQL([]ClusteringOrderColumn{})
	assert.Equal(t, "", stmt)

	stmt = generateOrderByCQL([]ClusteringOrderColumn{
		{Column: "foo", Direction: ASC},
	})
	assert.Equal(t, "foo ASC", stmt)

	stmt = generateOrderByCQL([]ClusteringOrderColumn{
		{Column: "foo", Direction: ASC},
		{Column: "bar", Direction: DESC},
	})
	assert.Equal(t, "foo ASC, bar DESC", stmt)
}
