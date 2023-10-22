package client

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/constants"
	"golang.org/x/exp/slog"
)

func NewDynamoDBLocalClient(ctx context.Context, endPointUrl, awsRegion string) (*dynamodb.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(awsRegion),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endPointUrl}, nil
			})),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID: "JITENDRA", SecretAccessKey: "TAKALKAR",
				Source: "Hard-coded credentials; values are irrelevant for local DynamoDB",
			},
		}),
	)
	if err != nil {
		slog.Error("Error while getting connection",
			constants.CliRequestId, ctx.Value(constants.CliRequestId),
			constants.LogErrorKey, err)
		return nil, err
	}

	return dynamodb.NewFromConfig(cfg), nil
}
