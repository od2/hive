package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/jmoiron/sqlx"
	"go.od2.network/hive/pkg/types"
)

// ItemStore stores a collection of items.
type ItemStore struct {
	DB        *sqlx.DB
	TableName string
	PKType    string
}

func (i *ItemStore) CreateTable(ctx context.Context) error {
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

// InsertNewlyDiscovered inserts newly found items into the items table.
// If the items already exist, nothing is done.
func (i *ItemStore) InsertNewlyDiscovered(ctx context.Context, pointers []*types.ItemPointer) error {
	tx, err := i.DB.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
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

// PushResults updates items with task results.
func (i *ItemStore) PushTaskResults(ctx context.Context, results []*types.TaskResult) error {
	tx, err := i.DB.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return err
	}
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
