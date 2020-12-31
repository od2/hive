package auth

import (
	"context"
	"fmt"

	"go.od2.network/hive/pkg/authgw"
	"go.od2.network/hive/pkg/token"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MDAuthorization is the authorization metadata key.
const MDAuthorization = "authorization"

// WorkerCredentials represents client auth.
type WorkerCredentials struct {
	Token string
}

// RequireTransportSecurity returns false, because we slapped Envoy in front of it.
func (a *WorkerCredentials) RequireTransportSecurity() bool {
	return false
}

// GetRequestMetadata fetches the authentication gRPC metadata.
func (a *WorkerCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	meta := map[string]string{MDAuthorization: a.Token}
	return meta, nil
}

// WorkerAuthInterceptor is a gRPC server auth interceptor.
type WorkerAuthInterceptor struct {
	authgw.Backend
	token.Signer
}

func (w *WorkerAuthInterceptor) intercept(ctx context.Context) (context.Context, error) {
	// Get the auth token from the gRPC request.
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, fmt.Errorf("missing metadata on request")
	}
	authVals := md.Get(MDAuthorization)
	if len(authVals) != 1 {
		return ctx, status.Error(codes.Unauthenticated, "missing auth header")
	}
	authToken := authVals[0]
	// Verify MAC signature.
	signedPayload := token.Unmarshal(authToken)
	if signedPayload == nil {
		return ctx, status.Error(codes.Unauthenticated, "invalid auth token")
	}
	if !w.VerifyTag(signedPayload) {
		return ctx, status.Error(codes.Unauthenticated, "invalid auth token")
	}
	// Lookup token in DB.
	tokenInfo, err := w.Backend.LookupToken(ctx, signedPayload.Payload.ID)
	if err == authgw.ErrUnknown {
		return ctx, status.Error(codes.Unauthenticated, "invalid auth token")
	} else if err != nil {
		// TODO Log error
		return ctx, status.Errorf(codes.Internal, "internal auth error")
	}
	if !tokenInfo.Valid {
		return ctx, status.Error(codes.Unauthenticated, "invalid auth token")
	}
	// TODO Check if expired
	authCtx := &WorkerContext{WorkerID: tokenInfo.WorkerID}
	return WithWorkerContext(ctx, authCtx), nil
}

// Unary returns a gRPC unary server interceptor for authentication.
func (w *WorkerAuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		ctx, err = w.intercept(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// Stream returns a gRPC stream server interceptor for authentication.
func (w *WorkerAuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx, err := w.intercept(ss.Context())
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

// WorkerContext describes the auth context of a request.
type WorkerContext struct {
	WorkerID int64
}

type workerContextKey struct{}

// WithContext returns a Go context with added auth context.
func WithWorkerContext(ctx context.Context, authCtx *WorkerContext) context.Context {
	return context.WithValue(ctx, workerContextKey{}, authCtx)
}

// WorkerFromContext returns the auth context from the Go context.
func WorkerFromContext(ctx context.Context) (*WorkerContext, error) {
	authCtx, _ := ctx.Value(workerContextKey{}).(*WorkerContext)
	if authCtx == nil {
		return nil, fmt.Errorf("invalid auth context")
	}
	return authCtx, nil
}
