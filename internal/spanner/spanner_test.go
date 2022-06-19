package spanner

import (
	"context"
	"fmt"
	"os"
	"testing"

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
