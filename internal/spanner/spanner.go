package spanner

import (
	"context"
	"fmt"
	"sort"

	"cloud.google.com/go/spanner"

	"github.com/kauche/splanter/internal/model"
)

type DB struct {
	client *spanner.Client
}

func NewDB(ctx context.Context, project, instance, database string) (*DB, error) {
	client, err := spanner.NewClient(ctx, fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, database))
	if err != nil {
		return nil, fmt.Errorf("failed to create spanner client: %w", err)
	}
	return &DB{client: client}, nil
}

func (d *DB) Close() {
	d.client.Close()
}

func (d *DB) Save(ctx context.Context, tables []*model.Table) error {
	if err := d.sortTablesByDependencies(ctx, tables); err != nil {
		return fmt.Errorf("failed to sort tables: %w", err)
	}

	var mutations []*spanner.Mutation
	for _, table := range tables {
		for _, records := range table.Records {
			mutations = append(mutations, spanner.InsertOrUpdateMap(table.Name, records.Values))
		}
	}

	if _, err := d.client.Apply(ctx, mutations); err != nil {
		return fmt.Errorf("failed to insert records: %w", err)
	}

	return nil
}

func (d *DB) sortTablesByDependencies(ctx context.Context, tables []*model.Table) error {
	tableNames := make([]string, len(tables))
	for i, t := range tables {
		tableNames[i] = t.Name
	}

	statement := spanner.Statement{
		// NOTE: `WHERE TABLE_TYPE = "BASE TABLE"` is enough to select user tables, but spanner-emulator doesn't support it for now.
		// SQL: `SELECT TABLE_NAME, PARENT_TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = "BASE TABLE"`,
		SQL: `SELECT TABLE_NAME, PARENT_TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME IN UNNEST (@tables)`,
		Params: map[string]interface{}{
			"tables": tableNames,
		},
	}

	tableMap := make(map[string]*informationSchemaTable)
	err := d.client.ReadOnlyTransaction().Query(ctx, statement).Do(func(row *spanner.Row) error {
		ist := new(informationSchemaTable)
		if err := row.ToStruct(ist); err != nil {
			return fmt.Errorf("failed to populate struct by rows: %w", err)
		}

		tableMap[ist.TableName.StringVal] = ist

		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to select INFORMATION_SCHEMA.TABLES: %w", err)
	}

	for _, ist := range tableMap {
		ist.numDependentParents = numDependentParents(ist, tableMap)
	}

	sort.SliceStable(tables, func(i, j int) bool {
		isti, ok := tableMap[tables[i].Name]
		if !ok {
			return false
		}

		istj, ok := tableMap[tables[j].Name]
		if !ok {
			return false
		}

		return isti.numDependentParents < istj.numDependentParents
	})

	return nil
}

func numDependentParents(ist *informationSchemaTable, tableMap map[string]*informationSchemaTable) uint {
	if !ist.ParentTableName.Valid {
		return 0
	}

	pist, ok := tableMap[ist.ParentTableName.StringVal]
	if !ok {
		return 0
	}

	return 1 + numDependentParents(pist, tableMap)
}
