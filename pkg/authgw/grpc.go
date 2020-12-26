package authgw

import (
	"context"
	"fmt"

	"go.od2.network/hive/pkg/auth"
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

func (i *Interceptor) intercept(ctx context.Context) (context.Context, error) {
	// Get the auth token from the gRPC request.
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, fmt.Errorf("missing metadata on request")
	}
	authVals := md.Get(auth.MDAuthorization)
	if len(authVals) != 1 {
		return ctx, status.Error(codes.Unauthenticated, "missing auth header")
	}
	authToken := authVals[0]
	// Verify MAC signature.
	signedPayload := token.Unmarshal(authToken)
	if signedPayload == nil {
		return ctx, status.Error(codes.Unauthenticated, "invalid auth token")
	}
	if !i.VerifyTag(signedPayload) {
		return ctx, status.Error(codes.Unauthenticated, "invalid auth token")
	}
	// Lookup token in DB.
	tokenInfo, err := i.Backend.LookupToken(ctx, signedPayload.Payload.ID)
	if err == ErrUnknown {
		return ctx, status.Error(codes.Unauthenticated, "invalid auth token")
	} else if err != nil {
		// TODO Log error
		return ctx, status.Errorf(codes.Internal, "internal auth error")
	}
	if !tokenInfo.Valid {
		return ctx, status.Error(codes.Unauthenticated, "invalid auth token")
	}
	// TODO Check if expired
	authCtx := &Context{WorkerID: tokenInfo.WorkerID}
	return WithContext(ctx, authCtx), nil
}

// Unary returns a gRPC unary server interceptor for authentication.
func (i *Interceptor) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		ctx, err = i.intercept(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// Stream returns a gRPC stream server interceptor for authentication.
func (i *Interceptor) Stream() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx, err := i.intercept(ss.Context())
		if err != nil {
			return err
		}
		wrappedStream := &serverStream{
			ServerStream: ss,
			ctx:          ctx,
		}
		return handler(srv, wrappedStream)
	}
}

type serverStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the embedded context.
func (s *serverStream) Context() context.Context {
	return s.ctx
}
