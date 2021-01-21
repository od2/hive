package management

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/golang/protobuf/ptypes"
	"github.com/rs/xid"
	"go.od2.network/hive-api"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/token"
)

// Handler implements the management API.
type Handler struct {
	DB     *sql.DB
	Signer token.Signer

	hive.UnimplementedManagementServer
}

// CreateWorkerToken generates a new, authorized worker token.
func (h *Handler) CreateWorkerToken(ctx context.Context, req *hive.CreateWorkerTokenRequest) (*hive.WorkerToken, error) {
	user, err := auth.WebFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// Generate new globally unique auth token ID.
	id := xid.New()
	ts, err := ptypes.Timestamp(req.ExpireAt)
	if err != nil {
		return nil, fmt.Errorf("invalid expire_at: %w", err)
	}
	// Sign token.
	exp, err := token.TimeToExp(ts)
	if err != nil {
		return nil, fmt.Errorf("invalid token expire_at: %w", err)
	}
	payload := token.Payload{
		Exp: exp,
	}
	copy(payload.ID[:], id[:])
	signedPayload, err := h.Signer.Sign(payload)
	if err != nil {
		// TODO Confidential error
		return nil, fmt.Errorf("failed to sign payload: %w", err)
	}
	// Insert auth token into DB.
	// TODO This assumes that the GitHub user ID = worker ID.
	//      Would be nice to support multiple workers per user.
	_, err = h.DB.ExecContext(ctx, `
		INSERT INTO auth_tokens (id, worker_id, expires_at) VALUES (?, ?, ?);`,
		id[:], user.ID, ts)
	if err != nil {
		return nil, fmt.Errorf("failed to insert auth token to DB: %w", err)
	}
	// Return token.
	result := &hive.WorkerToken{Token: token.Marshal(signedPayload)}
	return result, nil
}
