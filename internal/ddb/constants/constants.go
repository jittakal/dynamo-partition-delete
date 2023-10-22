package constants

import "github.com/jittakal/dynamodb-partition-delete/internal/ddb/types"

const CliRequestId types.RequestId = "request-id"
const LogRequestIdKey = "request-id"
const LogErrorKey = "error"

const BatchWriteItemSize = 25
