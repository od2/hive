package auth

import (
	"context"
	"fmt"
	"strconv"

	"google.golang.org/grpc/metadata"
)

// Context describes the auth context of a request.
type Context struct {
	WorkerID int64
}

// FromGRPCContext reads the auth context from gRPC headers.
func FromGRPCContext(ctx context.Context) (*Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("no gRPC metadata")
	}
	workerIDList := md.Get("worker-id")
	if len(workerIDList) != 1 {
		return nil, fmt.Errorf("got %d worker-id fields", len(workerIDList))
	}
	workerID, err := strconv.ParseInt(workerIDList[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid worker-id: %w", err)
	}
	return &Context{WorkerID: workerID}, nil
}
