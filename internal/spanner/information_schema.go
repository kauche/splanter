package spanner

import "cloud.google.com/go/spanner"

type informationSchemaTable struct {
	TableName       spanner.NullString `spanner:"TABLE_NAME"`
	ParentTableName spanner.NullString `spanner:"PARENT_TABLE_NAME"`

	numDependentParents uint
}
