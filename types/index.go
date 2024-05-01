package types

type DBkeyType struct {
	Name  string
	Value string
}
type DBKeys struct {
	PartitionKey DBkeyType
	SortKey      *DBkeyType
}
