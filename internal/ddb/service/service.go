package service

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/client"
	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/constants"
	t "github.com/jittakal/dynamodb-partition-delete/internal/ddb/types"
	"golang.org/x/exp/slog"
)

type TablePartitionService struct {
	client *dynamodb.Client
}

func NewTablePartitionService(ctx context.Context, endPointURL, awsRegion string) (*TablePartitionService, error) {
	client, err := client.NewDynamoDBLocalClient(ctx, endPointURL, awsRegion)

	if err != nil {
		slog.Info("Got error while initializing DynamoDB connection",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			constants.LogErrorKey, err)
		return nil, err
	}

	return &TablePartitionService{
		client: client,
	}, nil
}

// DeleteTablePartition delets all the partioned items for specified table
func (s *TablePartitionService) DeleteTablePartition(ctx context.Context, dtpi t.DeleteTablePartitionInput) error {
	slog.Info("Delete Table Partition request recieved",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"layer", "service",
		"table-name", dtpi.TableName,
		"partition-value", dtpi.PartitionValue,
		"endpoint-url", dtpi.EndpointUrl,
		"region", dtpi.AwsRegion)

	// Is Table Exists?
	_, err := s.tableExists(ctx, dtpi.TableName)
	if err != nil {
		return err
	}

	// Initiate Delete Partition Operation
	paginator, err := s.tableQueryPaginator(ctx, dtpi.TableName, dtpi.PartitionValue)
	if err != nil {
		return err
	}

	// Iterate and process Table Query Paginator
	s.iterateTableQueryPaginator(ctx, paginator)

	return nil
}

// Iterate Table Query Paginator
// Use of GO concurrency primities - Goroutines and Channels
func (s *TablePartitionService) iterateTableQueryPaginator(ctx context.Context, paginator *dynamodb.QueryPaginator) error {
	var wg sync.WaitGroup

	batchWriteItemInputStream := make(chan dynamodb.BatchWriteItemInput, 2)
	batchWriteItemOutputStream := make(chan dynamodb.BatchWriteItemOutput, 2)

	var wgDeleteItemBatch sync.WaitGroup
	wgDeleteItemBatch.Add(1)
	slog.Info("Goroutine - deleteTablePageBatchItems - batchWriteItemInputStream - batchWriteItemOutputStream - Triggered",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
	go s.deleteTablePageBatchItems(ctx, &wgDeleteItemBatch, batchWriteItemInputStream, batchWriteItemOutputStream)

	var wgDeletePartitionSummary sync.WaitGroup
	wgDeletePartitionSummary.Add(1)
	slog.Info("Goroutine - logDeleteTablePartitionSummary - batchWriteItemOutputStream - Triggered",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
	go s.logDeleteTablePartitionSummary(ctx, &wgDeletePartitionSummary, batchWriteItemOutputStream)

	for paginator.HasMorePages() {
		queryOutput, err := paginator.NextPage(ctx)
		slog.Info("Number of items for page found",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			"Length", len(queryOutput.Items))

		if err != nil {
			return fmt.Errorf("failed to iterate over next pages, here it is why - %s", err)
		}

		wg.Add(1)
		slog.Info("Goroutine - deleteTablePageItems - Triggered",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
		go s.deleteTablePageItems(ctx, &wg, batchWriteItemInputStream, queryOutput)
	}

	slog.Info("Waiting for - Goroutine - deleteTablePageItems - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Waiting")
	wg.Wait()
	slog.Info("Waiting for - Goroutine - deleteTablePageItems - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Done")

	slog.Info("Closing batchWriteItemInputStream - channel",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
	close(batchWriteItemInputStream)

	slog.Info("Waiting for - deleteTablePageBatchItems - DeleteRquest - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Waiting")
	wgDeleteItemBatch.Wait()
	slog.Info("Waiting for - deleteTablePageBatchItems - DeleteRquest - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Done")

	slog.Info("Closing batchWriteItemOutputStream - channel",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
	close(batchWriteItemOutputStream)

	slog.Info("Waiting for - logDeleteTablePartitionSummary - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Waiting")
	wgDeletePartitionSummary.Wait()
	slog.Info("Waiting for - logDeleteTablePartitionSummary - WaitGroup",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Status", "Done")

	return nil
}

// tableExists determines whether a DynamoDB table exists.
func (s *TablePartitionService) tableExists(ctx context.Context, tableName string) (bool, error) {
	exists := true

	_, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		var notFoundEx *types.ResourceNotFoundException

		if errors.As(err, &notFoundEx) {
			slog.Error("Table does not exists",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
				"Table", tableName,
				constants.LogErrorKey, err)
		} else {
			slog.Error("Couldn't determine existence of table",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
				"Table", tableName,
				constants.LogErrorKey, err)
		}
		exists = false
	}

	return exists, err
}

// tableKeySchema return table schema elements like partition key and range key
func (s *TablePartitionService) tableKeySchema(ctx context.Context, tableName string) (*TableKeySchema, error) {

	describeTableInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}

	describeTableOutput, err := s.client.DescribeTable(ctx, describeTableInput)
	if err != nil {
		slog.Error("Couldn't found the Table information, here it is why",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			"Table", tableName,
			constants.LogErrorKey, err)
		return nil, err
	}

	if describeTableOutput == nil {
		return nil, fmt.Errorf("dynamodb table `%s` does not exists", tableName)
	}

	kse := &TableKeySchema{
		TableName: tableName,
	}
	keySchemaElements := describeTableOutput.Table.KeySchema
	for _, keySchemaElement := range keySchemaElements {
		if keySchemaElement.KeyType == types.KeyTypeHash {
			kse.PartitionKey = aws.ToString(keySchemaElement.AttributeName)
		} else if keySchemaElement.KeyType == types.KeyTypeRange {
			kse.RangeKey = aws.ToString(keySchemaElement.AttributeName)
		}
		// If both keys are found, exit the loop
		if kse.PartitionKey != "" && kse.RangeKey != "" {
			break
		}
	}
	return kse, nil
}

// queryInput prepares the QueryPaginatorInput with projection and expression based on table key schema
func (s *TablePartitionService) tableQueryInput(ctx context.Context, tableName, partitionValue string) (*dynamodb.QueryInput, error) {
	kse, err := s.tableKeySchema(ctx, tableName)
	if err != nil {
		return nil, err
	}

	qInput := &dynamodb.QueryInput{
		TableName:              aws.String(kse.TableName),
		ProjectionExpression:   aws.String(kse.PartitionKey),
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :%s", kse.PartitionKey, kse.PartitionKey)),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			fmt.Sprintf(":%s", kse.PartitionKey): &types.AttributeValueMemberS{Value: partitionValue},
		},
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	}

	if kse.RangeKey != "" {
		qInput.ProjectionExpression = aws.String(fmt.Sprintf("%s, #sortkey", kse.PartitionKey))
		qInput.ExpressionAttributeNames = map[string]string{"#sortkey": kse.RangeKey}
	}
	return qInput, nil
}

// tablePaginator returns DynamoDB Table Query Paginator Object having pages
func (s *TablePartitionService) tableQueryPaginator(ctx context.Context, tableName, partitionValue string) (*dynamodb.QueryPaginator, error) {
	queryInput, err := s.tableQueryInput(ctx, tableName, partitionValue)
	if err != nil {
		return nil, err
	}

	paginator := dynamodb.NewQueryPaginator(s.client, queryInput)
	return paginator, nil
}

// Split page items into max 25 elements
func (s *TablePartitionService) prepareBatchWriteItemInputs(ctx context.Context, page *dynamodb.QueryOutput) []dynamodb.BatchWriteItemInput {
	tableName := aws.ToString(page.ConsumedCapacity.TableName)
	slog.Info("Table Name from page object",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"TableName", tableName)

	var deleteRequests []types.WriteRequest
	for _, item := range page.Items {
		deleteRequest := types.WriteRequest{
			DeleteRequest: &types.DeleteRequest{
				Key: item,
			},
		}
		deleteRequests = append(deleteRequests, deleteRequest)
	}

	var batches []dynamodb.BatchWriteItemInput
	deleteRequestLen := len(deleteRequests)
	for i := 0; i < deleteRequestLen; i += constants.BatchWriteItemSize {
		end := i + constants.BatchWriteItemSize
		if end > deleteRequestLen {
			end = deleteRequestLen
		}
		batches = append(batches, dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: deleteRequests[i:end],
			},
			ReturnConsumedCapacity:      types.ReturnConsumedCapacityTotal,
			ReturnItemCollectionMetrics: types.ReturnItemCollectionMetricsSize,
		})
	}

	return batches
}

func (s *TablePartitionService) deleteTablePageItems(ctx context.Context, wg *sync.WaitGroup,
	batchWriteItemInputStream chan<- dynamodb.BatchWriteItemInput, page *dynamodb.QueryOutput) {
	batchWriteIteminputs := s.prepareBatchWriteItemInputs(ctx, page)

	slog.Info("Total number of batches for paginator page",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"Length", len(batchWriteIteminputs))

	for _, batchWriteItemInput := range batchWriteIteminputs {
		slog.Debug("Streaming table page batch for deletetion",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			"layer", "Service",
			"method", "deleteTablePageBatchItems")
		batchWriteItemInputStream <- batchWriteItemInput
	}
	wg.Done()
}

func (s *TablePartitionService) deleteTablePageBatchItems(ctx context.Context, wgDeleteItemBatch *sync.WaitGroup,
	batchWriteItemInputStream <-chan dynamodb.BatchWriteItemInput,
	batchWriteItemOutputStream chan<- dynamodb.BatchWriteItemOutput) {

	for batchWriteItemInput := range batchWriteItemInputStream {
		slog.Debug("Received table page batch delete request",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
			"layer", "Service",
			"method", "deleteTablePageBatchItems")
		batchWriteItemOutput, err := s.client.BatchWriteItem(ctx, &batchWriteItemInput)

		if err != nil {
			slog.Error("Failed to delete items batch",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
				"error", err)
			return
		}

		batchWriteItemOutputStream <- *batchWriteItemOutput
	}
	wgDeleteItemBatch.Done()
}

func (s *TablePartitionService) logDeleteTablePartitionSummary(ctx context.Context, wgDeletePartitionSummary *sync.WaitGroup,
	batchWriteItemOutputStream <-chan dynamodb.BatchWriteItemOutput) {

	totalItemsCount := 0
	totalCapacityUnits := 0.0
	for batchWriteItemOutput := range batchWriteItemOutputStream {
		slog.Debug("logDeleteTablePartitionSummary - request - received",
			constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
		for key := range batchWriteItemOutput.ItemCollectionMetrics {
			totalItemsCount += len(batchWriteItemOutput.ItemCollectionMetrics[key])
		}
		for _, consumedCapacity := range batchWriteItemOutput.ConsumedCapacity {
			totalCapacityUnits += aws.ToFloat64(consumedCapacity.CapacityUnits)
		}
	}
	slog.Info("Delete Partition Summary Report",
		constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
		"TotalDeletedItems", totalItemsCount,
		"TotalConsumedCapacityUnits", totalCapacityUnits)
	wgDeletePartitionSummary.Done()
}
