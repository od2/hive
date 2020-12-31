package auth

import (
	"context"

	"google.golang.org/grpc"
)

// serverStream is a tiny wrapper around grpc.ServerStream
// for overriding the stream context.
type serverStream struct {
	grpc.ServerStream
	ctx context.Context
}

// WorkerContext returns the given context.
func (s *serverStream) Context() context.Context {
	return s.ctx
}
