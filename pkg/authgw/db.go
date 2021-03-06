package authgw

import (
	"context"
	"database/sql"
	"errors"

	"go.od2.network/hive/pkg/token"
)

// Database interfaces with the auth DB.
type Database struct {
	DB *sql.DB
}

// LookupToken reads a token from SQL.
func (d *Database) LookupToken(ctx context.Context, id token.ID) (*TokenInfo, error) {
	const query = "SELECT worker_id FROM auth_tokens WHERE id = ?;"
	row := d.DB.QueryRowContext(ctx, query, id[:])
	var workerID int64
	if err := row.Scan(&workerID); errors.Is(err, sql.ErrNoRows) {
		return nil, ErrUnknown
	} else if err != nil {
		return nil, err
	}
	return &TokenInfo{
		Valid:    true,
		WorkerID: workerID,
	}, nil
}
