package authgw

import (
	"context"
	"fmt"
)

// Context describes the auth context of a request.
type Context struct {
	WorkerID int64
}

type contextKey struct{}

// WithContext returns a Go context with added auth context.
func WithContext(ctx context.Context, authCtx *Context) context.Context {
	return context.WithValue(ctx, contextKey{}, authCtx)
}

// FromContext returns the auth context from the Go context.
func FromContext(ctx context.Context) (*Context, error) {
	authCtx, _ := ctx.Value(contextKey{}).(*Context)
	if authCtx == nil {
		return nil, fmt.Errorf("invalid auth context")
	}
	return authCtx, nil
}
