package yaml

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/kauche/splanter/internal/model"
)

func TestLoad(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	loader := NewLoader()
	actual, err := loader.Load(ctx, "testdata/seeds")
	if err != nil {
		t.Errorf("failed to load seeds: %s", err)
		return
	}

	expected := []*model.Table{
		{
			Name: "Bar",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"FooID": "e70946a8-2fb8-4457-96b1-d64c0d8d124c",
						"BarID": "208b6571-c140-4c2b-a9d5-b581fb062a77",
						"Name":  "bar1",
					},
				},
				{
					Values: map[string]interface{}{
						"FooID": "0b64da62-5895-4a7d-97bd-928ac8aaa076",
						"BarID": "10bb9433-559a-4b03-9361-d9cce55ec17c",
						"Name":  "bar2",
					},
				},
				{
					Values: map[string]interface{}{
						"FooID": "e70946a8-2fb8-4457-96b1-d64c0d8d124c",
						"BarID": "bcc0d0df-81bf-41b5-9427-9deb5e8f76f4",
						"Name":  "bar3",
					},
				},
			},
		},
		{
			Name: "Baz",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"FooID": "e70946a8-2fb8-4457-96b1-d64c0d8d124c",
						"BarID": "208b6571-c140-4c2b-a9d5-b581fb062a77",
						"BazID": "748eb1a4-6c2b-44d2-a549-db725865d9d6",
						"Name":  "baz1",
					},
				},
				{
					Values: map[string]interface{}{
						"FooID": "0b64da62-5895-4a7d-97bd-928ac8aaa076",
						"BarID": "10bb9433-559a-4b03-9361-d9cce55ec17c",
						"BazID": "373388b5-a7e4-4112-a898-ac0818ceefa4",
						"Name":  "baz2",
					},
				},
				{
					Values: map[string]interface{}{
						"FooID": "e70946a8-2fb8-4457-96b1-d64c0d8d124c",
						"BarID": "bcc0d0df-81bf-41b5-9427-9deb5e8f76f4",
						"BazID": "8c8cafab-830c-4f12-a5d8-a6bda10e912f",
						"Name":  "baz3",
					},
				},
			},
		},
		{
			Name: "Foo",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"FooID": "e70946a8-2fb8-4457-96b1-d64c0d8d124c",
						"Name":  "foo1",
					},
				},
				{
					Values: map[string]interface{}{
						"FooID": "0b64da62-5895-4a7d-97bd-928ac8aaa076",
						"Name":  "foo2",
					},
				},
				{
					Values: map[string]interface{}{
						"FooID": "bcc0d0df-81bf-41b5-9427-9deb5e8f76f4",
						"Name":  "foo3",
					},
				},
			},
		},
	}

	if diff := cmp.Diff(actual, expected); diff != "" {
		t.Errorf("\n(-actual, +expected)\n%s", diff)
	}
}
