package handler

import (
	"context"

	"github.com/jittakal/dynamodb-partition-delete/interfaces"
	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/service"
	"github.com/jittakal/dynamodb-partition-delete/internal/ddb/types"
)

type TablePartitionHandler struct {
	svc interfaces.TablePartitionDeleter
}

func NewTablePartitionHandler(ctx context.Context, endPointURL, awsRegion string) (*TablePartitionHandler, error) {
	tablePartitionService, err := service.NewTablePartitionService(ctx, endPointURL, awsRegion)

	if err != nil {
		return nil, err
	}
	return &TablePartitionHandler{
		svc: tablePartitionService,
	}, nil
}

func (h *TablePartitionHandler) HandleTablePartitionDeletion(ctx context.Context, dtpi types.DeleteTablePartitionInput) error {
	return h.svc.DeleteTablePartition(ctx, dtpi)
}
