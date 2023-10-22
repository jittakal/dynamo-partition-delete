package main

import (
	"context"
	"fmt"
	"os"

	"log/slog"

	"github.com/google/uuid"
	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/constants"
	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/handler"
	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/types"
	"github.com/spf13/cobra"
)

func init() {
	// New Json Handler
	handler := slog.NewJSONHandler(os.Stdout, nil)
	// New Logger for JSON Hander
	logger := slog.New(handler)
	// Set it as a default logger
	slog.SetDefault(logger)
}

func main() {
	var tableName, partitionValue, endpointURL, awsRegion string
	var skipConfirmation bool

	var rootCmd = &cobra.Command{Use: "ddbctl"}

	var deleteCmd = &cobra.Command{
		Use:   "delete-partition",
		Short: "Delete DynamoDB Table Partition Data",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.WithValue(context.TODO(), constants.CliRequestId, uuid.New())

			if tableName == "" || partitionValue == "" {
				slog.Info("Both 'table-name' and 'partition-value' are mandatory parameters.",
					constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
				os.Exit(1)
			}

			if !skipConfirmation {
				fmt.Printf("Are you sure you want to delete the partition in table %s with partition value %s? (y/n): ", tableName, partitionValue)
				var confirmation string
				_, err := fmt.Scanln(&confirmation)
				if err != nil {
					slog.Error("Failed to read user input for confirmation.",
						constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
						"error", err)
					os.Exit(1)
				}

				if confirmation != "y" && confirmation != "Y" {
					slog.Info("delete-partition request canceled.",
						constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))
					return
				}
			}

			slog.Info("ddbctl delete-partition cli parameters",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
				"table-name", tableName,
				"partition-value", partitionValue,
				"endpoint-url", endpointURL,
				"region", awsRegion)

			deleteTablePartitionInput := types.DeleteTablePartitionInput{
				TableName:      tableName,
				PartitionValue: partitionValue,
				EndpointUrl:    endpointURL,
				AwsRegion:      awsRegion,
			}

			handler, err := handler.NewTablePartitionHandler(ctx, endpointURL, awsRegion)

			if err != nil {
				slog.Error("Error while initializing Handler object",
					constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
					"error", err)
				os.Exit(1)
			}

			err = handler.HandleTablePartitionDeletion(ctx, deleteTablePartitionInput)

			if err != nil {
				slog.Error("Failed to delete-partition",
					constants.LogRequestIdKey, ctx.Value(constants.CliRequestId),
					"error", err)
				os.Exit(1)
			}

			slog.Info("delete-partition request completed successfully.",
				constants.LogRequestIdKey, ctx.Value(constants.CliRequestId))

		},
	}

	deleteCmd.Flags().StringVarP(&tableName, "table-name", "t", "", "Name of the DynamoDB table (mandatory)")
	deleteCmd.Flags().StringVarP(&partitionValue, "partition-value", "p", "", "Value of the partition key (mandatory)")
	deleteCmd.Flags().StringVarP(&endpointURL, "endpoint-url", "e", "http://localhost:8000", "Endpoint URL - http://localhost:8000")
	deleteCmd.Flags().StringVarP(&awsRegion, "region", "r", "us-east-1", "AWS region (optional)")
	deleteCmd.Flags().BoolVarP(&skipConfirmation, "skip-confirmation", "s", false, "Skip confirmation prompt")

	deleteCmd.MarkFlagRequired("table-name")
	deleteCmd.MarkFlagRequired("partition-value")

	rootCmd.AddCommand(deleteCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
