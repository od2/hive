package authgw

import (
	"context"
	"fmt"

	"go.od2.network/hive/pkg/token"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Interceptor is a gRPC server auth interceptor.
type Interceptor struct {
	Backend
	token.Signer
}

func (i *Interceptor) intercept(ctx context.Context) error {
	// Get the auth token from the gRPC request.
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return fmt.Errorf("missing metadata on request")
	}
	authVals := md.Get("authorization")
	if len(authVals) != 1 {
		return status.Error(codes.Unauthenticated, "missing auth header")
	}
	authToken := authVals[0]
	// Verify MAC signature.
	signedPayload := token.Unmarshal(authToken)
	if signedPayload == nil {
		return status.Error(codes.Unauthenticated, "invalid auth token")
	}
	if !i.VerifyTag(signedPayload) {
		return status.Error(codes.Unauthenticated, "invalid auth token")
	}
	// Lookup token in DB.
	tokenInfo, err := i.Backend.LookupToken(ctx, signedPayload.Payload.ID)
	if err == ErrUnknown {
		return status.Error(codes.Unauthenticated, "invalid auth token")
	} else if err != nil {
		// TODO Log error
		return status.Errorf(codes.Internal, "internal auth error")
	}
	if !tokenInfo.Valid {
		return status.Error(codes.Unauthenticated, "invalid auth token")
	}
	return nil
}

// UnaryInterceptor is a gRPC unary server interceptor for authentication.
func (i *Interceptor) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		if err := i.intercept(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor is a gRPC stream server interceptor for authentication.
func (i *Interceptor) Stream() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if err := i.intercept(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}
