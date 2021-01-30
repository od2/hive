package management

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/jmoiron/sqlx"
	"go.od2.network/hive-api/web"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/token"
	"go.od2.network/hive/pkg/topology"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TODO Limit the number of tokens per worker

// TODO Token cache invalidation

// Handler implements the management API.
type Handler struct {
	DB       *sqlx.DB
	Signer   token.Signer
	Log      *zap.Logger
	Topology *topology.Config
	Items    *items.Factory

	web.UnimplementedWorkerTokensServer
}

// CreateWorkerToken generates a new, authorized worker token.
func (h *Handler) CreateWorkerToken(ctx context.Context, req *web.CreateWorkerTokenRequest) (*web.CreateWorkerTokenResponse, error) {
	user, err := auth.WebFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// Generate new globally unique auth token ID.
	id := token.NewID()
	signedPayload, err := h.Signer.Sign(id)
	if err != nil {
		h.Log.Error("Failed to sign payload", zap.Error(err), zap.Binary("token.payload", id[:]))
		return nil, status.Error(codes.Internal, "Failed to sign payload")
	}
	tokenStr := token.Marshal(signedPayload)
	tokenBit := tokenStr[len(tokenStr)-4:]
	// Insert auth token into DB.
	_, err = h.DB.ExecContext(ctx,
		`INSERT INTO auth_tokens (id, worker_id, token_bit, description) VALUES (?, ?, ?, ?);`,
		id[:], user.ID, tokenBit, req.GetDescription())
	if err != nil {
		h.Log.Error("Failed to insert into tokens table", zap.Error(err))
		return nil, status.Error(codes.Internal, "Failed to create token")
	}
	// Return token.
	result := &web.CreateWorkerTokenResponse{
		Key: tokenStr,
		Token: &web.WorkerToken{
			Id:          base64.RawStdEncoding.EncodeToString(id[:]),
			Description: req.GetDescription(),
			TokenBit:    tokenBit,
			CreatedAt:   ptypes.TimestampNow(), // Not the real timestamp, but close enough.
			LastUsedAt:  &timestamp.Timestamp{Seconds: 0, Nanos: 0},
		},
	}
	return result, nil
}

// ListWorkerTokens returns a list of all worker tokens.
func (h *Handler) ListWorkerTokens(ctx context.Context, _ *web.ListWorkerTokensRequest) (*web.ListWorkerTokensResponse, error) {
	user, err := auth.WebFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// List all tokens from user.
	res := new(web.ListWorkerTokensResponse)
	scan, err := h.DB.QueryContext(ctx,
		`SELECT id, description, token_bit, created_at, last_used_at
         FROM auth_tokens WHERE worker_id = ?;`,
		user.ID)
	if err != nil {
		h.Log.Error("Failed to query tokens", zap.Error(err))
		return nil, status.Error(codes.Internal, "Failed to query tokens")
	}
	// Scan tokens.
	for scan.Next() {
		workerToken := new(web.WorkerToken)
		var id []byte
		var createdAt time.Time
		var lastUsedAt sql.NullTime
		err := scan.Scan(
			&id,
			&workerToken.Description,
			&workerToken.TokenBit,
			&createdAt,
			&lastUsedAt,
		)
		if err != nil {
			h.Log.Error("Failed to scan token", zap.Error(err))
			return nil, status.Error(codes.Internal, "Failed to query tokens")
		}
		workerToken.Id = base64.RawStdEncoding.EncodeToString(id[:])
		workerToken.CreatedAt, err = ptypes.TimestampProto(createdAt)
		if err != nil {
			h.Log.Error("Invalid timestamp returned by query", zap.Error(err),
				zap.Time("token.created_at", createdAt))
			return nil, status.Error(codes.Internal, "Failed to query tokens")
		}
		if lastUsedAt.Valid {
			workerToken.LastUsedAt, err = ptypes.TimestampProto(lastUsedAt.Time)
			if err != nil {
				h.Log.Error("Invalid timestamp returned by query", zap.Error(err),
					zap.Time("token.last_used_at", lastUsedAt.Time))
				return nil, status.Error(codes.Internal, "Failed to query tokens")
			}
		} else {
			workerToken.LastUsedAt = &timestamp.Timestamp{Seconds: 0, Nanos: 0}
		}
		res.Tokens = append(res.Tokens, workerToken)
	}
	return res, nil
}

// RevokeWorkerToken invalidates and deletes a worker token by ID.
func (h *Handler) RevokeWorkerToken(ctx context.Context, req *web.RevokeWorkerTokenRequest) (*web.RevokeWorkerTokenResponse, error) {
	user, err := auth.WebFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// Decode the provided ID.
	var id token.ID
	n, b64Err := base64.RawStdEncoding.Decode(id[:], []byte(req.TokenId))
	if b64Err != nil || n != len(id) {
		return &web.RevokeWorkerTokenResponse{Found: false}, nil
	}
	// Delete single token from user.
	execRes, err := h.DB.ExecContext(ctx,
		`DELETE FROM auth_tokens WHERE id = ? AND worker_id = ?;`, id[:], user.ID)
	if err != nil {
		h.Log.Error("Failed to delete token", zap.Error(err))
		return nil, status.Error(codes.Internal, "Failed to delete token")
	}
	affected, err := execRes.RowsAffected()
	if err != nil {
		h.Log.Error("Failed to check if deletion affected any rows", zap.Error(err))
		return nil, status.Error(codes.Internal, "Failed to delete token")
	}
	return &web.RevokeWorkerTokenResponse{Found: affected > 0}, nil
}

// RevokeAllWorkerTokens invalidates and deletes all tokens of a worker.
func (h *Handler) RevokeAllWorkerTokens(ctx context.Context, _ *web.RevokeAllWorkerTokensRequest) (*web.RevokeAllWorkerTokensResponse, error) {
	user, err := auth.WebFromContext(ctx)
	if err != nil {
		return nil, err
	}
	// Delete all tokens from user.
	_, err = h.DB.ExecContext(ctx, `DELETE FROM auth_tokens WHERE worker_id = ?;`, user.ID)
	if err != nil {
		h.Log.Error("Failed to delete tokens", zap.Error(err))
		return nil, status.Error(codes.Internal, "Failed to delete tokens")
	}
	return &web.RevokeAllWorkerTokensResponse{}, nil
}

// GetCollections returns the collections of the topology along with some stats.
func (h *Handler) GetCollections(ctx context.Context, _ *web.GetCollectionsRequest) (*web.GetCollectionsResponse, error) {
	// Determine current database name.
	var dbName string
	err := h.DB.
		QueryRowContext(ctx, "SELECT DATABASE() FROM DUAL;").
		Scan(&dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to determine database name: %w", err)
	}
	// Scan the approximate row counts of all collections.
	rowCountsScan, err := h.DB.QueryContext(ctx,
		`SELECT TABLE_NAME, TABLE_ROWS FROM information_schema.tables 
		WHERE TABLE_SCHEMA = ?;`, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to get row counts: %w", err)
	}
	rowCounts := make(map[string]int64)
	for rowCountsScan.Next() {
		var tableName string
		var rowCount int64
		if err := rowCountsScan.Scan(&tableName, &rowCount); err != nil {
			return nil, fmt.Errorf("failed to scan row count: %w", err)
		}
		rowCounts[tableName] = rowCount
	}
	// Pack into response.
	res := new(web.GetCollectionsResponse)
	res.Collections = make([]*web.Collection, len(h.Topology.Collections))
	for i, coll := range h.Topology.Collections {
		store, err := h.Items.GetStore(coll.Name)
		if err != nil {
			return nil, err
		}
		tableName := store.TableName
		res.Collections[i] = &web.Collection{
			Name:      coll.Name,
			ItemCount: rowCounts[tableName],
		}
	}
	return res, nil
}
