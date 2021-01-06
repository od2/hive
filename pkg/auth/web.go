package auth

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// WebIdentityInterceptor reads the identity of the web user
// issuing requests from trusted metadata set by Envoy.
type WebIdentityInterceptor struct{}

// MDUserIdentity is the user identity metadata key.
const MDUserIdentity = "x-od2-user-identity"

func (w *WebIdentityInterceptor) intercept(ctx context.Context) (context.Context, error) {
	// Get the auth token from the gRPC request.
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, fmt.Errorf("missing metadata on request")
	}
	identityVals := md.Get(MDUserIdentity)
	if len(identityVals) != 1 {
		return ctx, status.Error(codes.Unauthenticated, "missing or wrong number of identity metadata values")
	}
	authToken := identityVals[0]
	user := new(GitHubUser)
	if err := json.Unmarshal([]byte(authToken), user); err != nil {
		return nil, err
	}
	return WithWebContext(ctx, user), nil
}

// Unary returns a gRPC unary server interceptor for authentication.
func (w *WebIdentityInterceptor) Unary() grpc.UnaryServerInterceptor {
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
func (w *WebIdentityInterceptor) Stream() grpc.StreamServerInterceptor {
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
type WebContext struct {
	WorkerID int64
}

type webContextKey struct{}

// WithWebContext returns a Go context with added web context.
func WithWebContext(ctx context.Context, user *GitHubUser) context.Context {
	return context.WithValue(ctx, webContextKey{}, user)
}

// WebFromContext returns the web user from the Go context.
func WebFromContext(ctx context.Context) (*GitHubUser, error) {
	authCtx, _ := ctx.Value(webContextKey{}).(*GitHubUser)
	if authCtx == nil {
		return nil, fmt.Errorf("invalid auth context")
	}
	return authCtx, nil
}
