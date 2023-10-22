package types

type RequestId string

type DeleteTablePartitionInput struct {
	TableName      string
	PartitionValue string
	EndpointUrl    string
	AwsRegion      string
}
