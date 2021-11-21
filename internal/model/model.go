package model

type Table struct {
	Name    string
	Records []*Record
}

type Record struct {
	Values map[string]interface{}
}
