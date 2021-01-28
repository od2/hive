package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/go-github/v33/github"
	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// WebIdentity is the identity of a web user.
type WebIdentity struct {
	ID   int64
	Name string
}

// GitHubAuthInterceptor reads the identity of the GitHub user
// issuing requests from trusted metadata set by Envoy.
// It uses GitHub OAuth 2.0 tokens through Bearer auth.
type GitHubAuthInterceptor struct {
	Log *zap.Logger
}

func (w *GitHubAuthInterceptor) intercept(ctx context.Context) (context.Context, error) {
	// Get the auth token from the gRPC request.
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, fmt.Errorf("missing metadata on request")
	}
	identityVals := md.Get("Authorization")
	if len(identityVals) != 1 {
		return ctx, status.Error(codes.Unauthenticated, "missing or wrong number of identity metadata values")
	}
	authToken := oauth2.StaticTokenSource(&oauth2.Token{
		AccessToken: strings.TrimPrefix(identityVals[0], "Bearer "),
	})
	client := github.NewClient(oauth2.NewClient(ctx, authToken))
	githubUser, _, err := client.Users.Get(ctx, "")
	if err != nil {
		w.Log.Error("Invalid GitHub user", zap.Error(err))
		return nil, status.Error(codes.Unauthenticated, "invalid GitHub user")
	}
	identity := &WebIdentity{
		ID:   githubUser.GetID(),
		Name: githubUser.GetName(),
	}
	return WithWebContext(ctx, identity), nil
}

// Unary returns a gRPC unary server interceptor for authentication.
func (w *GitHubAuthInterceptor) Unary() grpc.UnaryServerInterceptor {
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
func (w *GitHubAuthInterceptor) Stream() grpc.StreamServerInterceptor {
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

type webContextKey struct{}

// WithWebContext returns a Go context with added web context.
func WithWebContext(ctx context.Context, user *WebIdentity) context.Context {
	return context.WithValue(ctx, webContextKey{}, user)
}

// WebFromContext returns the web user from the Go context.
func WebFromContext(ctx context.Context) (*WebIdentity, error) {
	authCtx, _ := ctx.Value(webContextKey{}).(*WebIdentity)
	if authCtx == nil {
		return nil, fmt.Errorf("invalid auth context")
	}
	return authCtx, nil
}
