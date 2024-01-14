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
			Name: "AllTypes",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"BoolValue":      true,
						"BytesValue":     "aG9nZQ==",
						"DateValue":      "2022-04-01",
						"Float64Value":   float64(3.14159),
						"ID":             "All_Type_Values",
						"Int64Value":     int64(42),
						"JSONValue":      `{"test": 1}`,
						"NumericValue":   string("-12345678901234567890123456789.123456789"),
						"StringValue":    "FooBar",
						"TimestampValue": "2022-04-01T00:00:00Z",
						"BoolArray":      []bool{true, false},
						"BytesArray":     []string{"aG9nZQ==", "aG9nZQ=="},
						"DateArray":      []string{"2022-04-01", "2022-04-02"},
						"Fload64Array":   []float64{12.34, 56.789},
						"Int64Array":     []int64{12, 34},
						"JSONArray":      []string{`{"test": 1}`, `{"test": 2}`},
						"NumericArray":   []string{"-12345678901234567890123456789.123456789", "-12345678901234567890123456789.123456789"},
						"StringArray":    []string{"Foo", "Bar"},
						"TimestampArray": []string{"2022-04-01T00:00:00Z", "2022-04-02T00:00:00Z"},
					},
				},
			},
		},
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
			Name: "Boo",
			Records: []*model.Record{
				{
					Values: map[string]interface{}{
						"BazID": "748eb1a4-6c2b-44d2-a549-db725865d9d6",
						"BooID": "86e27352-3352-4415-be2f-2522cfbdfbcf",
						"Name":  "boo1",
					},
				},
				{
					Values: map[string]interface{}{
						"BazID": "373388b5-a7e4-4112-a898-ac0818ceefa4",
						"BooID": "b9c9bd23-1c0b-434c-a553-de7791870c79",
						"Name":  "boo2",
					},
				},
				{
					Values: map[string]interface{}{
						"BazID": "8c8cafab-830c-4f12-a5d8-a6bda10e912f",
						"BooID": "39150a7d-b9be-4fec-8447-921d8cc3dd51",
						"Name":  "boo3",
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
				{
					Values: map[string]interface{}{
						"FooID": int64(123),
						"Name":  "foo4",
					},
				},
			},
		},
	}

	if diff := cmp.Diff(actual, expected); diff != "" {
		t.Errorf("\n(-actual, +expected)\n%s", diff)
	}
}
