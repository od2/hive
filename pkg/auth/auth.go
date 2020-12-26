package auth

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MDAuthorization is the authorization metadata key.
const MDAuthorization = "authorization"

// Auth represents client auth.
type Auth struct {
	Token string
}

// Unary returns an auth unary client interceptor.
func (a *Auth) Unary() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		ctx = metadata.AppendToOutgoingContext(ctx, MDAuthorization, a.Token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// Stream returns an auth stream client interceptor.
func (a *Auth) Stream() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, MDAuthorization, a.Token)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
