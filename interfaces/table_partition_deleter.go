package interfaces

import (
	"context"

	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/types"
)

type TablePartitionDeleter interface {
	DeleteTablePartition(ctx context.Context, dtpi types.DeleteTablePartitionInput) error
}
