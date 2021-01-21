package items

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive-api"
	"go.od2.network/hive/pkg/mariadbtest"
)

func TestItemStore(t *testing.T) {
	db := predefinedDB
	if db == nil {
		t.Log("No pre-defined DB, using Docker")
		docker := mariadbtest.NewDocker(t)
		defer docker.Close(t)
		db = docker.DB
	}
	itemStore := &Store{
		DB:        db,
		TableName: "item_store_1",
		PKType:    "BIGINT UNSIGNED",
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Create table
	require.NoError(t, itemStore.CreateTable(ctx))
	t.Log("Created table", itemStore.TableName)
	// Insert items the first time
	require.NoError(t, itemStore.InsertDiscovered(ctx, []*hive.ItemPointer{
		{
			Dst:       &hive.ItemLocator{Id: "1"},
			Timestamp: &timestamp.Timestamp{Seconds: 3},
		},
		{
			Dst:       &hive.ItemLocator{Id: "2"},
			Timestamp: &timestamp.Timestamp{Seconds: 3},
		},
	}))
	t.Log("Newly discovered: 1, 2")
	// Insert items again
	require.NoError(t, itemStore.InsertDiscovered(ctx, []*hive.ItemPointer{
		{
			Dst:       &hive.ItemLocator{Id: "2"},
			Timestamp: &timestamp.Timestamp{Seconds: 5},
		},
		{
			Dst:       &hive.ItemLocator{Id: "3"},
			Timestamp: &timestamp.Timestamp{Seconds: 10},
		},
	}))
	t.Log("Newly discovered: 2, 3")
	// Scan items
	assert.Equal(t, []itemStoreRow{
		{ItemID: "1", FoundT: time.Unix(3, 0)},
		{ItemID: "2", FoundT: time.Unix(3, 0)},
		{ItemID: "3", FoundT: time.Unix(10, 0)},
	}, scanItemStore(t, itemStore))
	// Push a batch of task results.
	require.NoError(t, itemStore.PushAssignmentResults(ctx, []*hive.AssignmentResult{
		{
			Locator:    &hive.ItemLocator{Id: "2"},
			FinishTime: &timestamp.Timestamp{Seconds: 3},
		},
		{
			Locator:    &hive.ItemLocator{Id: "2"},
			FinishTime: &timestamp.Timestamp{Seconds: 4},
		},
		{
			Locator:    &hive.ItemLocator{Id: "3"},
			FinishTime: &timestamp.Timestamp{Seconds: 4},
		},
		{
			Locator:    &hive.ItemLocator{Id: "4"},
			FinishTime: &timestamp.Timestamp{Seconds: 23},
		},
	}))
	t.Log("Pushed task results: 2, 2, 3, 4")
	// Scan items
	assert.Equal(t, []itemStoreRow{
		{
			ItemID: "1",
			FoundT: time.Unix(3, 0),
		},
		{
			ItemID:     "2",
			FoundT:     time.Unix(3, 0),
			LastUpdate: sql.NullTime{Valid: true, Time: time.Unix(4, 0)},
			Updates:    2,
		},
		{
			ItemID:     "3",
			FoundT:     time.Unix(10, 0),
			LastUpdate: sql.NullTime{Valid: true, Time: time.Unix(4, 0)},
			Updates:    1,
		},
		{
			ItemID:     "4",
			FoundT:     time.Unix(23, 0),
			LastUpdate: sql.NullTime{Valid: true, Time: time.Unix(23, 0)},
			Updates:    1,
		},
	}, scanItemStore(t, itemStore))
}

func scanItemStore(t *testing.T, itemStore *Store) (rows []itemStoreRow) {
	require.NoError(t, itemStore.DB.Select(&rows,
		fmt.Sprintf("SELECT * FROM %s ORDER BY item_id ASC", itemStore.TableName)))
	return
}
