package service

type DeletePartitionInput struct {
	TableName      string
	PartitionValue string
}

type TableKeySchema struct {
	TableName    string
	PartitionKey string
	RangeKey     string
}
