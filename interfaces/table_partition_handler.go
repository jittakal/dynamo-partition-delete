package interfaces

import (
	"context"

	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/types"
)

type TablePartitionHandler interface {
	HandleTablePartitionDeletion(ctx context.Context, dtpi types.DeleteTablePartitionInput) error
}
