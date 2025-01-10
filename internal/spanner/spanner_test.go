package spanner

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/google/go-cmp/cmp"

	"github.com/kauche/splanter/internal/model"
)

type foo struct {
	FooID string `spanner:"FooID"`
	Name  string `spanner:"Name"`
}

type bar struct {
	FooID string `spanner:"FooID"`
	BarID string `spanner:"BarID"`
	Name  string `spanner:"Name"`
}

type baz struct {
	FooID string `spanner:"FooID"`
	BarID string `spanner:"BarID"`
	BazID string `spanner:"BazID"`
	Name  string `spanner:"Name"`
}

type allTypes struct {
	ID             string             `spanner:"ID"`
	StringValue    string             `spanner:"StringValue"`
	BoolValue      bool               `spanner:"BoolValue"`
	Int64Value     int64              `spanner:"Int64Value"`
	Float64Value   float64            `spanner:"Float64Value"`
	JSONValue      spanner.NullJSON   `spanner:"JSONValue"`
	BytesValue     []byte             `spanner:"BytesValue"`
	TimestampValue time.Time          `spanner:"TimestampValue"`
	NumericValue   *big.Rat           `spanner:"NumericValue"`
	DateValue      civil.Date         `spanner:"DateValue"`
	StringArray    []string           `spanner:"StringArray"`
	BoolArray      []bool             `spanner:"BoolArray"`
	Int64Array     []int64            `spanner:"Int64Array"`
	Float64Array   []float64          `spanner:"Float64Array"`
	JSONArray      []spanner.NullJSON `spanner:"JSONArray"`
	BytesArray     [][]byte           `spanner:"BytesArray"`
	TimestampArray []time.Time        `spanner:"TimestampArray"`
	NumericArray   []*big.Rat         `spanner:"NumericArray"`
	DateArray      []civil.Date       `spanner:"DateArray"`
}

func TestSave(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	db := testDB(t, ctx)

	err := db.Save(ctx, []*model.Table{
		{
			Name: "Foo",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"FooID": "993fcece-25b1-48e6-8618-d494eb4a0817",
						"Name":  "foo1",
					},
				},
				{
					Values: map[string]interface{}{
						"FooID": "cc2fadaa-c12e-4c77-a84a-c838d16c1cf7",
						"Name":  "foo2",
					},
				},
			},
		},
		{
			Name: "Bar",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"FooID": "993fcece-25b1-48e6-8618-d494eb4a0817",
						"BarID": "fae34ede-e66b-4e9b-835f-abb5b36faca5",
						"Name":  "bar1",
					},
				},
				{
					Values: map[string]interface{}{
						"FooID": "cc2fadaa-c12e-4c77-a84a-c838d16c1cf7",
						"BarID": "b0401495-ee10-4abe-9b66-8ec88640349d",
						"Name":  "bar2",
					},
				},
			},
		},
		{
			Name: "Baz",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"FooID": "993fcece-25b1-48e6-8618-d494eb4a0817",
						"BarID": "fae34ede-e66b-4e9b-835f-abb5b36faca5",
						"BazID": "b8f18085-7fa7-4845-a88a-7fbc7468489c",
						"Name":  "baz1",
					},
				},
				{
					Values: map[string]interface{}{
						"FooID": "cc2fadaa-c12e-4c77-a84a-c838d16c1cf7",
						"BarID": "b0401495-ee10-4abe-9b66-8ec88640349d",
						"BazID": "17291658-8f9b-4166-8599-988451228493",
						"Name":  "baz2",
					},
				},
			},
		},
		{
			Name: "Boo",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"BazID": "b8f18085-7fa7-4845-a88a-7fbc7468489c",
						"BooID": "25746814-9347-4766-b27a-07cdd92bbd4a",
						"Name":  "boo1",
					},
				},
				{
					Values: map[string]interface{}{
						"BazID": "17291658-8f9b-4166-8599-988451228493",
						"BooID": "2ede531d-6965-46d8-bd94-2d392d944f70",
						"Name":  "boo2",
					},
				},
			},
		},
		{
			Name: "AllTypes",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"DateValue":      "2022-04-01",
						"Float64Value":   float64(3.14159),
						"ID":             "All_Type_Values",
						"Int64Value":     int64(42),
						"JSONValue":      `{"test": 1}`,
						"NumericValue":   "-12345678901234567890123456789.123456789",
						"StringValue":    "FooBar",
						"TimestampValue": "2022-04-01T00:00:00Z",
						"BoolValue":      true,
						"BytesValue":     "aG9nZQ==",
						"StringArray":    []string{"Foo", "Bar"},
						"BoolArray":      []bool{true, false},
						"Int64Array":     []int64{12, 34},
						"Float64Array":   []float64{12.34, 56.789},
						"JSONArray":      []string{`{"test": 1}`, `{"test": 2}`},
						"BytesArray":     []string{"aG9nZQ==", "aG9nZQ=="},
						"TimestampArray": []string{"2022-04-01T00:00:00Z", "2022-04-02T00:00:00Z"},
						"NumericArray":   []string{"-12345678901234567890123456789.123456789", "-12345678901234567890123456789.123456789"},
						"DateArray":      []string{"2022-04-01", "2022-04-02"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Errorf("failed to save: %s", err)
		return
	}

	var actualFoos []*foo
	err = db.client.ReadOnlyTransaction().Query(ctx, spanner.Statement{
		SQL: "SELECT * FROM Foo ORDER BY Name",
	}).Do(func(row *spanner.Row) error {
		f := new(foo)
		if err = row.ToStruct(f); err != nil {
			return fmt.Errorf("failed to populate struct foo by rows: %w", err)
		}
		actualFoos = append(actualFoos, f)

		return nil
	})
	if err != nil {
		t.Errorf("failed to select Foo: %s", err)
		return
	}

	expectedFoos := []*foo{
		{
			FooID: "993fcece-25b1-48e6-8618-d494eb4a0817",
			Name:  "foo1",
		},
		{
			FooID: "cc2fadaa-c12e-4c77-a84a-c838d16c1cf7",
			Name:  "foo2",
		},
	}
	if diff := cmp.Diff(actualFoos, expectedFoos); diff != "" {
		t.Errorf("\n(-actual, +expected)\n%s", diff)
	}

	var actualBars []*bar
	err = db.client.ReadOnlyTransaction().Query(ctx, spanner.Statement{
		SQL: "SELECT * FROM Bar ORDER BY Name",
	}).Do(func(row *spanner.Row) error {
		b := new(bar)
		if err = row.ToStruct(b); err != nil {
			return fmt.Errorf("failed to populate struct bar by rows: %w", err)
		}
		actualBars = append(actualBars, b)

		return nil
	})
	if err != nil {
		t.Errorf("failed to select Bar: %s", err)
		return
	}

	expectedBars := []*bar{
		{
			FooID: "993fcece-25b1-48e6-8618-d494eb4a0817",
			BarID: "fae34ede-e66b-4e9b-835f-abb5b36faca5",
			Name:  "bar1",
		},
		{
			FooID: "cc2fadaa-c12e-4c77-a84a-c838d16c1cf7",
			BarID: "b0401495-ee10-4abe-9b66-8ec88640349d",
			Name:  "bar2",
		},
	}
	if diff := cmp.Diff(actualBars, expectedBars); diff != "" {
		t.Errorf("\n(-actual, +expected)\n%s", diff)
	}

	var actualBazs []*baz
	err = db.client.ReadOnlyTransaction().Query(ctx, spanner.Statement{
		SQL: "SELECT * FROM Baz ORDER BY Name",
	}).Do(func(row *spanner.Row) error {
		b := new(baz)
		if err = row.ToStruct(b); err != nil {
			return fmt.Errorf("failed to populate struct baz by rows: %w", err)
		}
		actualBazs = append(actualBazs, b)

		return nil
	})
	if err != nil {
		t.Errorf("failed to select Baz: %s", err)
		return
	}

	expectedBazs := []*baz{
		{
			FooID: "993fcece-25b1-48e6-8618-d494eb4a0817",
			BarID: "fae34ede-e66b-4e9b-835f-abb5b36faca5",
			BazID: "b8f18085-7fa7-4845-a88a-7fbc7468489c",
			Name:  "baz1",
		},
		{
			FooID: "cc2fadaa-c12e-4c77-a84a-c838d16c1cf7",
			BarID: "b0401495-ee10-4abe-9b66-8ec88640349d",
			BazID: "17291658-8f9b-4166-8599-988451228493",
			Name:  "baz2",
		},
	}
	if diff := cmp.Diff(actualBazs, expectedBazs); diff != "" {
		t.Errorf("\n(-actual, +expected)\n%s", diff)
	}

	var actualAllTypes []*allTypes
	err = db.client.ReadOnlyTransaction().Query(ctx, spanner.Statement{
		SQL: "SELECT * FROM AllTypes ORDER BY ID",
	}).Do(func(row *spanner.Row) error {
		b := new(allTypes)
		if err = row.ToStruct(b); err != nil {
			return fmt.Errorf("failed to populate allTypes by rows: %w", err)
		}
		actualAllTypes = append(actualAllTypes, b)

		return nil
	})
	if err != nil {
		t.Errorf("failed to select AllTypes: %s", err)
		return
	}
	expectedNumeric, _ := new(big.Rat).SetString("-12345678901234567890123456789.123456789")
	expectedAllTypes := []*allTypes{
		{
			ID:             "All_Type_Values",
			StringValue:    "FooBar",
			BoolValue:      true,
			Int64Value:     42,
			Float64Value:   3.14159,
			JSONValue:      spanner.NullJSON{Value: map[string]interface{}{"test": float64(1)}, Valid: true},
			BytesValue:     []byte{'h', 'o', 'g', 'e'},
			TimestampValue: time.Date(2022, time.April, 1, 0, 0, 0, 0, time.UTC),
			NumericValue:   expectedNumeric,
			DateValue:      civil.Date{Year: 2022, Month: time.April, Day: 1},
			StringArray:    []string{"Foo", "Bar"},
			BoolArray:      []bool{true, false},
			Float64Array:   []float64{12.34, 56.789},
			Int64Array:     []int64{12, 34},
			JSONArray: []spanner.NullJSON{
				{Value: map[string]interface{}{"test": float64(1)}, Valid: true},
				{Value: map[string]interface{}{"test": float64(2)}, Valid: true},
			},
			BytesArray: [][]byte{{'h', 'o', 'g', 'e'}, {'h', 'o', 'g', 'e'}},
			TimestampArray: []time.Time{
				time.Date(2022, time.April, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2022, time.April, 2, 0, 0, 0, 0, time.UTC),
			},
			NumericArray: []*big.Rat{expectedNumeric, expectedNumeric},
			DateArray: []civil.Date{
				{Year: 2022, Month: time.April, Day: 1},
				{Year: 2022, Month: time.April, Day: 2},
			},
		},
	}
	if diff := cmp.Diff(actualAllTypes, expectedAllTypes, cmp.Comparer(func(x, y *big.Rat) bool {
		if x == nil || y == nil {
			return false
		}
		return x.Cmp(y) == 0
	})); diff != "" {
		t.Errorf("\n(-actual, +expected)\n%s", diff)
	}
}

func TestSortTablesByDependencies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	db := testDB(t, ctx)

	actual := []*model.Table{
		{
			Name: "Bar",
		},
		{
			Name: "Baz",
		},
		{
			Name: "Foo",
		},
	}

	if err := db.sortTablesByDependencies(ctx, actual); err != nil {
		t.Errorf("failed to sort: %s", err)
		return
	}

	expected := []*model.Table{
		{
			Name: "Foo",
		},
		{
			Name: "Bar",
		},
		{
			Name: "Baz",
		},
	}

	if diff := cmp.Diff(actual, expected); diff != "" {
		t.Errorf("\n(-actual, +expected)\n%s", diff)
	}
}

func testDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()

	project := os.Getenv("SPANNER_PROJECT")
	if project == "" {
		t.Fatal("must specify SPANNER_PROJECT")
	}

	instance := os.Getenv("SPANNER_INSTANCE")
	if instance == "" {
		t.Fatal("must specify SPANNER_INSTANCE")
	}

	database := os.Getenv("SPANNER_DATABASE")
	if database == "" {
		t.Fatal("must specify SPANNER_DATABASE")
	}

	db, err := NewDB(ctx, project, instance, database)
	if err != nil {
		t.Fatalf("failed to create DB: %s", err)
	}

	return db
}
