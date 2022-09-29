package gocassa

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/monzo/gocassa/reflect"
)

type customType struct {
	foo string
}

func (ct customType) CQLType() gocql.Type {
	return gocql.TypeVarchar
}

// TestGoCassaType_CustomType tests that cassaType supports identifying the
// C* type for a custom type that implements the CQLTyper interface
func TestGoCassaType_CustomType(t *testing.T) {
	var testRow = struct {
		Field customType
	}{}

	m, ok := reflect.StructToMap(testRow)
	require.True(t, ok)

	typ := cassaType(m["Field"])
	assert.Equal(t, gocql.TypeVarchar, typ)
}
