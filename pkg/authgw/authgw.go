package authgw

import (
	"context"
	"errors"
	"time"

	"go.od2.network/hive/pkg/token"
)

// Backend fetches information about tokens.
type Backend interface {
	LookupToken(ctx context.Context, id token.ID) (*TokenInfo, error)
}

// TokenInfo specifies whether a token authorizes the client for the required service.
type TokenInfo struct {
	ExpiresAt time.Time // the time when the token becomes invalid
	Valid     bool      // whether the last lookup granted authorization (from Policy)
	WorkerID  int64
}

// ErrUnknown is returned when a token is not present in the DB.
var ErrUnknown = errors.New("unknown token")
