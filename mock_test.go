package gocassa

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type user struct {
	Pk1  int
	Pk2  int
	Ck1  int
	Ck2  int
	Name string
}

type UserWithMap struct {
	Id       string
	Map      map[string]interface{}
	OtherMap map[int]interface{}
}

type point struct {
	Time time.Time
	Id   int
	User string
	X    float64
	Y    float64
}

type PostalCode string

type address struct {
	Time            time.Time
	Id              string
	County          string
	LocationPrice   map[string]int       // json compatible map
	LocationHistory map[time.Time]string // not json compatible map
	PostCode        PostalCode           // embedded type
	TownID          string
}

func TestRunMockSuite(t *testing.T) {
	suite.Run(t, new(MockSuite))
}

type MockSuite struct {
	suite.Suite
	*require.Assertions
	tbl                  Table
	ks                   KeySpace
	mapTbl               MapTable
	mmapTbl              MultimapTable
	tsTbl                TimeSeriesTable
	mtsTbl               MultiTimeSeriesTable
	mkTsTbl              MultiKeyTimeSeriesTable
	embMapTbl            MapTable
	embTsTbl             TimeSeriesTable
	addressByCountyMmTbl MultimapTable
	mmMkTable            MultimapMkTable
}

func (s *MockSuite) SetupTest() {
	s.ks = NewMockKeySpace()
	s.Assertions = require.New(s.T())
	s.tbl = s.ks.Table("users", user{}, Keys{
		PartitionKeys:     []string{"Pk1", "Pk2"},
		ClusteringColumns: []string{"Ck1", "Ck2"},
	})

	s.mapTbl = s.ks.MapTable("users", "Pk1", user{})
	s.mmapTbl = s.ks.MultimapTable("users", "Pk1", "Pk2", user{})
	s.tsTbl = s.ks.TimeSeriesTable("points", "Time", "Id", 1*time.Minute, point{})
	s.mtsTbl = s.ks.MultiTimeSeriesTable("points", "User", "Time", "Id", 1*time.Minute, point{})
	s.mkTsTbl = s.ks.MultiKeyTimeSeriesTable("points", []string{"X", "Y"}, "Time", []string{"Id"}, 1*time.Minute, point{})

	s.embMapTbl = s.ks.MapTable("addresses", "Id", address{})
	s.embTsTbl = s.ks.TimeSeriesTable("addresses", "Time", "Id", 1*time.Minute, address{})
	s.addressByCountyMmTbl = s.ks.MultimapTable("address_by_county", "County", "Id", address{})
	s.mmMkTable = s.ks.MultimapMultiKeyTable("addresses_by_id_and_town", []string{"Id", "TownID"}, []string{"County"}, address{})
}

// Table tests
func (s *MockSuite) TestTableEmpty() {
	var result []user
	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).Read(&result).Run())
	s.Equal(0, len(result))
}

// TestEmptyPrimaryKey asserts that gocassa mock will return an error for a row
// with an empty primary
func (s *MockSuite) TestEmptyPrimaryKey() {
	add := address{
		Id:              "",
		County:          "",
		Time:            s.parseTime("2015-01-01 00:00:00"),
		LocationPrice:   map[string]int{"A": 1},
		LocationHistory: map[time.Time]string{time.Now().UTC(): "A"},
		PostCode:        "ABC",
	}

	// embMapTbl has Id in the partition key
	s.Error(s.embMapTbl.Set(add).Run())
	// addressByCountyMmTbl has County in the partition key
	s.Error(s.addressByCountyMmTbl.Set(add).Run())

	add.County = "London"
	s.Error(s.embMapTbl.Set(add).Run())
	// address can be written successfully, now that the partiton key -
	// County is not empty anymore; Id is still empty
	s.NoError(s.addressByCountyMmTbl.Set(add).Run())

	add.Id = "someID"
	// both Id and County are not empty, writing to the tables should be
	// suucessful
	s.NoError(s.embMapTbl.Set(add).Run())
	s.NoError(s.addressByCountyMmTbl.Set(add).Run())

	add = address{
		Id:              "",
		TownID:          "",
		County:          "",
		Time:            s.parseTime("2015-01-01 00:00:00"),
		LocationPrice:   map[string]int{"A": 1},
		LocationHistory: map[time.Time]string{time.Now().UTC(): "A"},
		PostCode:        "ABC",
	}
	// no error in writing all empty values to a table with a composite
	// primary key
	s.NoError(s.mmMkTable.Set(add).Run())
}

func (s *MockSuite) TestTableRead() {
	u1, u2, u3, u4 := s.insertUsers()

	var users []user
	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1)).Read(&users).Run())
	s.Equal([]user{u1, u4, u3}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 2)).Read(&users).Run())
	s.Equal([]user{u2}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), In("Pk2", 1, 2)).Read(&users).Run())
	s.Equal([]user{u1, u4, u3, u2}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1)).Read(&users).Run())
	s.Equal([]user{u1, u4}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).Read(&users).Run())
	s.Equal([]user{u1}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), GT("Ck1", 1)).Read(&users).Run())
	s.Equal([]user{u3}, users)

	s.NoError(s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), LT("Ck2", 2)).Read(&users).Run())
	s.Equal([]user{u1}, users)

	var u user
	op1 := s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 1)).ReadOne(&u)
	s.NoError(op1.Run())
	s.Equal(u1, u)

	op2 := s.tbl.Where(Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)).ReadOne(&u)
	s.NoError(op2.Run())
	s.Equal(u4, u)

	s.NoError(op1.Add(op2).Run())
	s.NoError(op1.Add(op2).RunLoggedBatchWithContext(context.Background()))
}

func (s *MockSuite) TestTableUpdate() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)}

	s.NoError(s.tbl.Where(relations...).Update(map[string]interface{}{
		"Name": "x",
	}).Run())

	var u user
	s.NoError(s.tbl.Where(relations...).ReadOne(&u).Run())
	s.Equal("x", u.Name)

	relations = []Relation{Eq("Pk1", 1), In("Pk2", 1, 2), Eq("Ck1", 1), Eq("Ck2", 1)}

	s.NoError(s.tbl.Where(relations...).Update(map[string]interface{}{
		"Name": "y",
	}).Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Read(&users).Run())
	for _, u := range users {
		s.Equal("y", u.Name)
	}
}

func (s *MockSuite) TestTableDeleteOne() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), Eq("Pk2", 1), Eq("Ck1", 1), Eq("Ck2", 2)}
	s.NoError(s.tbl.Where(relations...).Delete().Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Read(&users).Run())
	s.Empty(users)
}

func (s *MockSuite) TestTableDeleteWithIn() {
	s.insertUsers()

	relations := []Relation{Eq("Pk1", 1), In("Pk2", 1, 2), Eq("Ck1", 1), Eq("Ck2", 1)}
	s.NoError(s.tbl.Where(relations...).Delete().Run())

	var users []user
	s.NoError(s.tbl.Where(relations...).Read(&users).Run())
	s.Empty(users)
}

// MapTable tests
func (s *MockSuite) TestMapTableRead() {
	s.insertUsers()
	var u user
	s.NoError(s.mapTbl.Read(1, &u).Run())
	s.Equal("Jane", u.Name)
	s.Error(s.mapTbl.Read(42, &u).Run())
}

func (s *MockSuite) TestMapTableMultiRead() {
	s.insertUsers()
	var users []user
	s.NoError(s.mapTbl.MultiRead([]interface{}{1, 2}, &users).Run())
	s.Len(users, 2)
	s.Equal("Jane", users[0].Name)
	s.Equal("Jill", users[1].Name)
}

func (s *MockSuite) TestMapTableUpdate() {
	s.insertUsers()
	s.NoError(s.mapTbl.Update(1, map[string]interface{}{
		"Name": "foo",
	}).Run())
	var u user
	s.NoError(s.mapTbl.Read(1, &u).Run())
	s.Equal("foo", u.Name)
}

func (s *MockSuite) TestMapTableDelete() {
	s.insertUsers()
	s.NoError(s.mapTbl.Delete(1).Run())
	var user user
	s.Equal(RowNotFoundError{}, s.mapTbl.Read(1, &user).Run())
}

func (s *MockSuite) TestMapModifiers() {
	tbl := s.ks.MapTable("user342135", "Id", UserWithMap{})
	createIf(tbl.(TableChanger), s.T())
	c := UserWithMap{
		Id: "1",
		Map: map[string]interface{}{
			"3": "Is Odd",
			"6": "Is Even",
		},
		OtherMap: nil,
	}
	if err := tbl.Set(c).Run(); err != nil {
		s.T().Fatal(err)
	}

	// MapSetField
	if err := tbl.Update("1", map[string]interface{}{
		"OtherMap": MapSetField(1, "One"),
	}).Run(); err != nil {
		s.T().Fatal(err)
	}

	if err := tbl.Update("1", map[string]interface{}{
		"OtherMap": MapSetField(2, "Two"),
	}).Run(); err != nil {
		s.T().Fatal(err)
	}

	// MapSetFields
	if err := tbl.Update("1", map[string]interface{}{
		"Map": MapSetFields(map[string]interface{}{
			"2": "Two",
			"4": "Four",
		}),
	}).Run(); err != nil {
		s.T().Fatal(err)
	}

	// Read back into a new struct (see #83)
	var c2 UserWithMap
	if err := tbl.Read("1", &c2).Run(); err != nil {
		s.T().Fatal(err)
	}
	if !reflect.DeepEqual(c2, UserWithMap{
		Id: "1",
		Map: map[string]interface{}{
			"2": "Two",
			"3": "Is Odd",
			"4": "Four",
			"6": "Is Even",
		},
		OtherMap: map[int]interface{}{
			1: "One",
			2: "Two",
		},
	}) {
		s.T().Fatal(c2)
	}
}

// MultiMapTable tests
func (s *MockSuite) TestMultiMapTableRead() {
	s.insertUsers()

	var u user
	s.NoError(s.mmapTbl.Read(1, 1, &u).Run())
	s.Equal("Jane", u.Name)
	s.NoError(s.mmapTbl.Read(1, 2, &u).Run())
	s.Equal("Joe", u.Name)
}

func (s *MockSuite) TestMultiMapTableList() {
	s.insertUsers()
	var users []user

	// Offset 0, limit 10
	s.NoError(s.mmapTbl.List(1, 0, 10, &users).Run())
	s.Len(users, 2)
	s.Equal("Jane", users[0].Name)
	s.Equal("Joe", users[1].Name)

	// Offset 1, limit 1
	s.NoError(s.mmapTbl.List(1, 1, 1, &users).Run())
	s.Len(users, 1)
	s.Equal("Jane", users[0].Name)

	// Offset 2, limit 1
	s.NoError(s.mmapTbl.List(1, 2, 1, &users).Run())
	s.Len(users, 1)
	s.Equal("Joe", users[0].Name)
}

func (s *MockSuite) TestMultiMapTableUpdate() {
	s.insertUsers()

	s.NoError(s.mmapTbl.Update(1, 2, map[string]interface{}{
		"Name": "foo",
	}).Run())
	var u user
	s.NoError(s.mmapTbl.Read(1, 2, &u).Run())
	s.Equal("foo", u.Name)
}

func (s *MockSuite) TestMultiMapTableDelete() {
	s.insertUsers()
	s.NoError(s.mmapTbl.Delete(1, 2).Run())
	var u user
	s.Equal(RowNotFoundError{}, s.mmapTbl.Read(1, 2, &u).Run())
}

func (s *MockSuite) TestMultiMapTableDeleteAll() {
	s.insertUsers()
	s.NoError(s.mmapTbl.DeleteAll(1).Run())
	var users []user
	s.Empty(users)
}

// TimeSeriesTable tests
func (s *MockSuite) TestTimeSeriesTableRead() {
	points := s.insertPoints()

	var p point
	s.NoError(s.tsTbl.Read(points[0].Time, points[0].Id, &p).Run())
	s.Equal(points[0], p)
}

func (s *MockSuite) TestTimeSeriesTableList() {
	points := s.insertPoints()

	// First two points
	var ps []point
	s.NoError(s.tsTbl.List(points[0].Time, points[1].Time, &ps).Run())
	s.Len(ps, 2)
	s.Equal(points[0], ps[0])
	s.Equal(points[1], ps[1])

	// Last two points
	s.NoError(s.tsTbl.List(points[1].Time, points[2].Time, &ps).Run())
	s.Len(ps, 2)
	s.Equal(points[1], ps[0])
	s.Equal(points[2], ps[1])
}

func (s *MockSuite) TestWithOptions() {
	points := s.insertPoints()
	var ps []point

	// First two points, but with a limit of one
	s.NoError(s.tsTbl.List(points[0].Time, points[1].Time, &ps).
		WithOptions(Options{Limit: 1}).Run())
	s.Len(ps, 1)
	s.Equal(points[0], ps[0])

	// First two points, but with a limit of one, but RunWithContext
	s.NoError(s.tsTbl.List(points[0].Time, points[1].Time, &ps).
		WithOptions(Options{Limit: 1}).RunWithContext(context.Background()))
	s.Len(ps, 1)
	s.Equal(points[0], ps[0])
}

func (s *MockSuite) TestTimeSeriesTableUpdate() {
	points := s.insertPoints()

	s.NoError(s.tsTbl.Update(points[0].Time, points[0].Id, map[string]interface{}{
		"X": 42.0,
		"Y": 43.0,
	}).Run())
	var p point
	s.NoError(s.tsTbl.Read(points[0].Time, points[0].Id, &p).Run())
	s.Equal(42.0, p.X)
	s.Equal(43.0, p.Y)
}

func (s *MockSuite) TestTimeSeriesTableDelete() {
	points := s.insertPoints()

	var p point
	s.NoError(s.tsTbl.Delete(points[0].Time, points[0].Id).Run())
	s.Equal(RowNotFoundError{}, s.tsTbl.Read(points[0].Time, points[0].Id, &p).Run())
}

// MultiTimeSeriesTable tests
func (s *MockSuite) TestMultiTimeSeriesTableRead() {
	points := s.insertPoints()

	var p point
	s.NoError(s.mtsTbl.Read("John", points[0].Time, points[0].Id, &p).Run())
	s.Equal(points[0], p)
}

func (s *MockSuite) TestMultiTimeSeriesTableList() {
	points := s.insertPoints()

	var ps []point
	s.NoError(s.mtsTbl.List("John", points[0].Time, points[2].Time, &ps).Run())
	s.Len(ps, 2)
	s.Equal(points[0], ps[0])
	s.Equal(points[2], ps[1])

	s.NoError(s.mtsTbl.List("Jane", points[0].Time, points[2].Time, &ps).Run())
	s.Len(ps, 1)
	s.Equal(points[1], ps[0])
}

func (s *MockSuite) TestMultiTimeSeriesTableUpdate() {
	points := s.insertPoints()

	s.NoError(s.mtsTbl.Update("John", points[0].Time, points[0].Id, map[string]interface{}{
		"X": 42.0,
	}).Run())

	var p point
	s.NoError(s.mtsTbl.Read("John", points[0].Time, points[0].Id, &p).Run())
	s.Equal(42.0, p.X)
}

func (s *MockSuite) TestMultiTimeSeriesTableDelete() {
	points := s.insertPoints()

	s.NoError(s.mtsTbl.Delete("John", points[0].Time, points[0].Id).Run())

	var p point
	s.Equal(RowNotFoundError{}, s.mtsTbl.Read("John", points[0].Time, points[0].Id, &p).Run())
}

func (s *MockSuite) TestMultiKeyTimeSeriesTableRead() {
	points := s.insertPoints()

	var p point
	s.NoError(s.mkTsTbl.Read(map[string]interface{}{"X": points[0].X, "Y": points[0].Y}, points[0].Time, map[string]interface{}{"User": points[0].User}, &p).Run())
	s.Equal(points[0], p)
}

func (s *MockSuite) TestMultiKeyTimeSeriesTableList() {
	points := s.insertPoints()

	var ps []point
	s.NoError(s.mkTsTbl.List(map[string]interface{}{"X": 1.1, "Y": 1.2}, points[0].Time, points[2].Time, &ps).Run())
	s.Len(ps, 1)
	s.Equal(points[0], ps[0])

	s.NoError(s.mkTsTbl.List(map[string]interface{}{"X": 5.1, "Y": 5.2}, points[0].Time, points[2].Time, &ps).Run())
	s.Len(ps, 1)
	s.Equal(points[1], ps[0])
}

func (s *MockSuite) TestNoop() {
	s.insertUsers()
	var users []user
	op := Noop()
	op = op.Add(s.mapTbl.MultiRead([]interface{}{1, 2}, &users))
	s.NoError(op.Run())
	s.Len(users, 2)
	s.Equal("Jane", users[0].Name)
	s.Equal("Jill", users[1].Name)
}

// Don't panic when adding an empty Noop to an existing op chain
func (s *MockSuite) TestEmptyNoop() {
	addresses := s.insertAddresses()
	addressToUpdate := addresses[0]
	addressToUpdate.PostCode = "XYZ"
	emptyNoop := Noop()
	op := s.embMapTbl.Set(addressToUpdate)
	op = op.Add(s.embTsTbl.Set(addressToUpdate))
	s.NotPanics(func() {
		op = op.Add(emptyNoop)
	})
	s.NoError(op.Run())
}

func (s *MockSuite) TestEmbedMapRead() {
	expectedAddresses := s.insertAddresses()

	var actualAddress address
	s.NoError(s.embMapTbl.Read("1", &actualAddress).Run())
	s.Equal(expectedAddresses[0], actualAddress)

	s.NoError(s.embMapTbl.Read("2", &actualAddress).Run())
	s.Equal(expectedAddresses[1], actualAddress)
}

// Helper functions
func (s *MockSuite) insertPoints() []point {
	points := []point{
		point{
			Time: s.parseTime("2015-04-01 15:41:00"),
			Id:   1,
			User: "John",
			X:    1.1,
			Y:    1.2,
		},
		point{
			Time: s.parseTime("2015-04-01 15:41:05"),
			Id:   2,
			User: "Jane",
			X:    5.1,
			Y:    5.2,
		},
		point{
			Time: s.parseTime("2015-04-01 15:41:10"),
			Id:   3,
			User: "John",
			X:    1.1,
			Y:    1.3,
		},
	}

	for _, p := range points {
		s.NoError(s.tsTbl.Set(p).Run())
		s.NoError(s.mtsTbl.Set(p).Run())
		s.NoError(s.mkTsTbl.Set(p).Run())
	}

	return points
}

func (s *MockSuite) insertUsers() (user, user, user, user) {
	u1 := user{
		Pk1:  1,
		Pk2:  1,
		Ck1:  1,
		Ck2:  1,
		Name: "John",
	}
	u2 := user{
		Pk1:  1,
		Pk2:  2,
		Ck1:  1,
		Ck2:  1,
		Name: "Joe",
	}
	u3 := user{
		Pk1:  1,
		Pk2:  1,
		Ck1:  2,
		Ck2:  1,
		Name: "Josh",
	}
	u4 := user{
		Pk1:  1,
		Pk2:  1,
		Ck1:  1,
		Ck2:  2,
		Name: "Jane",
	}
	u5 := user{
		Pk1:  2,
		Pk2:  1,
		Ck1:  1,
		Ck2:  1,
		Name: "Jill",
	}

	for _, u := range []user{u1, u2, u3, u4, u5} {
		s.NoError(s.tbl.Set(u).Run())
		s.NoError(s.mapTbl.Set(u).Run())
		s.NoError(s.mmapTbl.Set(u).Run())
	}

	return u1, u2, u3, u4
}

func (s *MockSuite) insertAddresses() []address {
	addresses := []address{
		address{
			Id:              "1",
			Time:            s.parseTime("2015-01-01 00:00:00"),
			LocationPrice:   map[string]int{"A": 1},
			LocationHistory: map[time.Time]string{time.Now().UTC(): "A"},
			PostCode:        "ABC",
		},
		address{
			Id:              "2",
			Time:            s.parseTime("2015-01-02 00:00:00"),
			LocationPrice:   map[string]int{"F": 1},
			LocationHistory: map[time.Time]string{time.Now().UTC(): "F"},
			PostCode:        "FGH",
		},
	}

	for _, addr := range addresses {
		s.NoError(s.embMapTbl.Set(addr).Run())
		s.NoError(s.embTsTbl.Set(addr).Run())
	}

	return addresses
}

func (s *MockSuite) parseTime(value string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", value)
	s.NoError(err)
	return t
}

func TestRunMockIteratorSuite(t *testing.T) {
	suite.Run(t, new(MockIteratorSuite))
}

type MockIteratorSuite struct {
	suite.Suite
	*require.Assertions
}

func (s *MockIteratorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *MockIteratorSuite) TestBaseBehaviour() {
	// Test with no results
	iter := newMockIterator([]map[string]interface{}{}, []string{})
	s.False(iter.Next(), "expected next to fail as there were no results")

	// Test with results but no fields
	result := map[string]interface{}{"a": "1"}
	iter = newMockIterator([]map[string]interface{}{result}, []string{})
	s.True(iter.Next(), "expected next to succeed as there was one result")
	s.False(iter.Next(), "expected next to fail as there were no more results")
	s.Equal(0, iter.currRowIndex)

	// Test with mismatched fields vs destination ptrs
	iter = newMockIterator([]map[string]interface{}{result}, []string{"a"})
	s.True(iter.Next())
	s.Error(iter.Scan())

	// Test for passing a struct as a value
	iter = newMockIterator([]map[string]interface{}{result}, []string{"a"})
	s.True(iter.Next())
	s.Error(iter.Scan())

	// Sanity check the happy case, we unmarshal
	var res string
	iter = newMockIterator([]map[string]interface{}{result}, []string{"a"})
	s.True(iter.Next())
	s.NoError(iter.Scan(&res))
	s.Equal(0, iter.currRowIndex)
	s.Equal("1", res)
}

func (s *MockIteratorSuite) TestIgnorableFields() {
	result := map[string]interface{}{"a": "1", "b": "2", "c": "3"}
	iter := newMockIterator([]map[string]interface{}{result}, []string{"a", "b"})

	// Test reading all the things
	var a1, b1 string
	s.True(iter.Next())
	s.NoError(iter.Scan(&a1, &b1))
	s.Equal("1", a1)
	s.Equal("2", b1)
	iter.Reset()

	// Test ignorable things
	var a2, b2 string
	s.True(iter.Next())
	s.NoError(iter.Scan(&a2, &IgnoreFieldType{}))
	s.Equal("1", a2)
	s.Equal("", b2)
	iter.Reset()
	s.True(iter.Next())
	s.NoError(iter.Scan(&IgnoreFieldType{}, &b2))
	s.Equal("2", b2)
	iter.Reset()

	// Test ignoring everything
	s.True(iter.Next())
	s.NoError(iter.Scan(&IgnoreFieldType{}, &IgnoreFieldType{}))
	iter.Reset()

	result = map[string]interface{}{"a": "1", "b": "2", "c": "3"}
	iter = newMockIterator([]map[string]interface{}{result}, []string{"e", "f"})

	// Test fields not present in results
	var e3, f3 string
	s.True(iter.Next())
	s.NoError(iter.Scan(&e3, &f3))
	s.Equal("", e3)
	s.Equal("", f3)
	iter.Reset()
}

func (s *MockIteratorSuite) TestMapConversionTypes() {
	result := map[string]interface{}{
		"a": map[string]interface{}{"a1": "1", "a2": "2"},
		"b": map[string]interface{}{"b1": 1, "b2": 2},
		"c": map[string]interface{}{"c1": float32(1.0), "c2": float32(2.0)},
	}

	iter := newMockIterator([]map[string]interface{}{result}, []string{"a", "b", "c"})
	var a map[string]string
	var b map[string]int
	var c map[string]float32
	s.True(iter.Next())
	s.NoError(iter.Scan(&a, &b, &c))
	s.Equal("1", a["a1"])
	s.Equal(2, b["b2"])
	s.Equal(float32(1.0), c["c1"])
}

func (s *MockIteratorSuite) TestConvertableTypes() {
	type Status string

	result := map[string]interface{}{"a": "1", "b": "2", "c": "3"}
	iter := newMockIterator([]map[string]interface{}{result}, []string{"a", "b"})

	// Test we can convert between sensible types
	var a1, b1 Status
	s.True(iter.Next())
	s.NoError(iter.Scan(&a1, &b1))
	s.Equal(Status("1"), a1)
	s.Equal(Status("2"), b1)
	iter.Reset()

	var a2, b2 []byte
	s.True(iter.Next())
	s.NoError(iter.Scan(&a2, &b2))
	s.Equal("1", string(a2))
	s.Equal("2", string(b2))
	iter.Reset()

	// Test we can't convert into a nonsense type
	var a3, b3 int
	s.True(iter.Next())
	s.Error(iter.Scan(&a3, &b3))
	iter.Reset()
}

func (s *MockIteratorSuite) TestValidValues() {
	t, _ := time.Parse("2006-01-02 15:04:05-0700", "2018-11-13 14:05:36+0000")
	result := map[string]interface{}{"a": t, "b": nil}
	iter := newMockIterator([]map[string]interface{}{result}, []string{"a", "b"})

	var a1, b1 time.Time
	s.True(iter.Next())
	s.NoError(iter.Scan(&a1, &b1))
	s.Equal(t, a1)
	s.Equal(time.Time{}, b1)
}

func TestErrorInjectors(t *testing.T) {
	type Thing struct {
		ID    string
		Field string
	}
	things := []Thing{
		{ID: "1", Field: "one"},
		{ID: "2", Field: "two"},
		{ID: "3", Field: "three"},
	}
	errToInject := fmt.Errorf("injected error")

	t.Run("NeverFail", func(t *testing.T) {
		ks := NewMockKeySpace()
		table := ks.MapTable("table_name", "ID", Thing{})

		op := Noop()
		for _, thing := range things {
			op = op.Add(table.Set(thing))
		}
		ctx := ErrorInjectorContext(context.Background(), &neverFail{})
		err := op.RunWithContext(ctx)
		assert.NoError(t, err)

		for _, thing := range things {
			readThing := Thing{}
			err := table.Read(thing.ID, &readThing).RunWithContext(context.Background())
			require.NoError(t, err)
			assert.Equal(t, thing, readThing)
		}
	})

	t.Run("FailOnNthOperation", func(t *testing.T) {
		ks := NewMockKeySpace()
		table := ks.MapTable("table_name", "ID", Thing{})

		op := Noop()
		for _, thing := range things {
			op = op.Add(table.Set(thing))
		}
		ctx := ErrorInjectorContext(context.Background(), FailOnNthOperation(2, errToInject))
		err := op.RunWithContext(ctx)
		assert.Equal(t, errToInject, err)
	})

	t.Run("FailOnEachOperation", func(t *testing.T) {
		ks := NewMockKeySpace()
		table := ks.MapTable("table_name", "ID", Thing{})

		op := Noop()
		for _, thing := range things {
			op = op.Add(table.Set(thing))
		}
		errorInjector := FailOnEachOperation(errToInject)
		ctx := ErrorInjectorContext(context.Background(), errorInjector)

		ops, ok := op.(mockMultiOp)
		require.True(t, ok)

		for i := 0; i < len(ops); i++ {
			err := op.RunWithContext(ctx)
			assert.Equal(t, errToInject, err)
			assert.True(t, errorInjector.ShouldContinue())
			assert.Equal(t, i, errorInjector.LastErrorInjectedAtIdx())
		}

		// After we've failed on all the operations, the error injector should
		// stop injecting errors, allowing the operation to succeed
		err := op.RunWithContext(ctx)
		assert.NoError(t, err)
		assert.False(t, errorInjector.ShouldContinue())
		assert.Equal(t, -1, errorInjector.LastErrorInjectedAtIdx())

		for _, thing := range things {
			readThing := Thing{}
			err := table.Read(thing.ID, &readThing).RunWithContext(context.Background())
			require.NoError(t, err)
			assert.Equal(t, thing, readThing)
		}
	})
}

func TestMockClusteringOrder(t *testing.T) {
	type Thing struct {
		ID      string
		Created time.Time
		Count   int
	}

	ctx := context.Background()

	ks := NewMockKeySpace()
	table := ks.Table("thing_table", Thing{}, Keys{
		PartitionKeys: []string{
			"ID",
		},
		ClusteringColumns: []string{
			"Created", "Count",
		},
	}).WithOptions(Options{
		ClusteringOrder: []ClusteringOrderColumn{
			{
				Direction: DESC,
				Column:    "Created",
			},
			{
				Direction: ASC,
				Column:    "Count",
			},
		},
	})

	id := "1"
	// things is in the correct order.
	things := []Thing{
		{
			ID:      id,
			Created: time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
			Count:   1,
		},
		{
			ID:      id,
			Created: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			Count:   1,
		},
		{
			ID:      id,
			Created: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			Count:   1,
		},
		{
			ID:      id,
			Created: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			Count:   2,
		},
		{
			ID:      id,
			Created: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC),
			Count:   3,
		},
	}

	for _, thing := range things {
		err := table.Set(thing).RunWithContext(ctx)
		require.NoError(t, err)
	}

	readThings := []Thing{}
	err := table.Where(Eq("ID", id)).Read(&readThings).RunWithContext(ctx)
	require.NoError(t, err)
	require.Equal(t, things, readThings)
}
