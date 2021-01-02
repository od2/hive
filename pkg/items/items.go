package items

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/jmoiron/sqlx"
	"go.od2.network/hive/pkg/types"
)

// Store stores a collection of items.
type Store struct {
	DB        *sqlx.DB
	TableName string
	PKType    string
}

// CreateTable creates an items table.
func (i *Store) CreateTable(ctx context.Context) error {
	// language=MariaDB
	const template = `CREATE TABLE %s (
	item_id %s PRIMARY KEY,
	found_t DATETIME NOT NULL,
	last_update DATETIME,
	updates BIGINT UNSIGNED DEFAULT 0 NOT NULL
);`
	stmt := fmt.Sprintf(template, i.TableName, i.PKType)
	_, err := i.DB.ExecContext(ctx, stmt)
	return err
}

type itemStoreRow struct {
	ItemID     string       `db:"item_id"`
	FoundT     time.Time    `db:"found_t"`
	LastUpdate sql.NullTime `db:"last_update"`
	Updates    uint64       `db:"updates"`
}

// InsertDiscovered inserts newly found items into the items table.
// If the items already exist, nothing is done.
func (i *Store) InsertDiscovered(ctx context.Context, pointers []*types.ItemPointer) error {
	tx, err := i.DB.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
	// language=MariaDB
	const stmt = `INSERT IGNORE INTO %s (item_id, found_t)
VALUES (:item_id, :found_t);`
	inserts := make([]itemStoreRow, len(pointers))
	for i, pointer := range pointers {
		t, err := ptypes.Timestamp(pointer.Timestamp)
		if err != nil {
			return fmt.Errorf("invalid timestamp: %w", err)
		}
		inserts[i] = itemStoreRow{
			ItemID: pointer.Dst.Id,
			FoundT: t,
		}
	}
	if _, err = tx.NamedExecContext(ctx, fmt.Sprintf(stmt, i.TableName), inserts); err != nil {
		return err
	}
	return tx.Commit()
}

// FilterNewPointers filters a batch of pointers, removing items that were already seen.
func (i *Store) FilterNewPointers(ctx context.Context, itemIDs []string) ([]string, error) {
	tx, err := i.DB.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, err
	}
	const stmt = `SELECT item_id FROM %s WHERE item_id NOT IN (?);`
	results, err := tx.QueryContext(ctx, fmt.Sprintf(stmt, i.TableName), itemIDs)
	if err != nil {
		return nil, err
	}
	deduped := make([]string, 0, len(itemIDs))
	for results.Next() {
		var itemID string
		if err := results.Scan(&itemID); err != nil {
			return nil, fmt.Errorf("failed to scan results: %w", err)
		}
		deduped = append(deduped, itemID)
	}
	if err := results.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan result set: %w", err)
	}
	return deduped, nil
}

// PushResults updates items with task results.
func (i *Store) PushTaskResults(ctx context.Context, results []*types.TaskResult) error {
	tx, err := i.DB.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
	// language=MariaDB
	const stmt = `INSERT INTO %s (item_id, found_t, last_update, updates)
VALUES (:item_id, :found_t, :last_update, :updates)
ON DUPLICATE KEY UPDATE last_update = VALUES(last_update), updates = updates + 1;`
	inserts := make([]itemStoreRow, len(results))
	for i, result := range results {
		finishTime, err := ptypes.Timestamp(result.FinishTime)
		if err != nil {
			return fmt.Errorf("invalid finish_time: %w", err)
		}
		inserts[i] = itemStoreRow{
			ItemID:     result.Item.Id,
			FoundT:     finishTime,
			LastUpdate: sql.NullTime{Valid: true, Time: finishTime},
			Updates:    1, // the initial value, otherwise it's added in the update
		}
	}
	if _, err = tx.NamedExecContext(ctx, fmt.Sprintf(stmt, i.TableName), inserts); err != nil {
		return err
	}
	return tx.Commit()
}
