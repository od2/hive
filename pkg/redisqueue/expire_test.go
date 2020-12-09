package redisqueue

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive/pkg/redistest"
	"go.uber.org/zap/zaptest"
)

func TestExpirationWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := redistest.NewRedis(ctx, t)
	defer instance.Close()

	callbacks := make(chan claimedTask)
	expWorker := ExpirationWorker{
		Log:   zaptest.NewLogger(t),
		Redis: instance.Client,
		Callback: func(ctx context.Context, taskID string, claim string) error {
			require.NoError(t, ctx.Err())
			callbacks <- claimedTask{taskID, claim}
			return nil
		},
		Keys:         KeysForPrefix("Q"),
		EmptyBackoff: 100 * time.Millisecond,
		BatchSize:    2,
	}
	go func() {
		require.Equal(t, context.Canceled, expWorker.Run(ctx))
	}()

	var err error
	// Add items to pending
	require.NoError(t, err)
	// Make the test backoff with no items
	time.Sleep(300 * time.Millisecond)
	// Push 4 tasks to Redis, 2 expire instantly, 1 is already removed, 1 expires after 3 seconds.
	pushTime := time.Now().Unix()
	_, err = instance.Client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		claims := []string{
			"t1", "c1",
			// t2 was already removed (Consumers.DropClaims)
			"t3", "c3",
			"t4", "c4",
		}
		if err := instance.Client.HSet(ctx, expWorker.Keys.InflightHash, claims).Err(); err != nil {
			return err
		}
		expireList := []string{
			fmt.Sprintf("t1:%d", pushTime),
			fmt.Sprintf("t2:%d", pushTime),
			fmt.Sprintf("t3:%d", pushTime),
			fmt.Sprintf("t4:%d", pushTime+2),
		}
		if err := instance.Client.LPush(ctx, expWorker.Keys.ExpireList, expireList).Err(); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
	t.Log("Pushed 4 items to in-flight")
	// Wait for 2 expirations to come in.
	firstTimer := time.NewTimer(time.Second)
	var firstCallbacks []claimedTask
	defer firstTimer.Stop()
first3Collect:
	for {
		select {
		case <-firstTimer.C:
			break first3Collect
		case ct := <-callbacks:
			firstCallbacks = append(firstCallbacks, ct)
		}
	}
	// We should get exactly 2 tasks that expired.
	require.Equal(t, []claimedTask{
		{taskID: "t1", claim: "c1"},
		{taskID: "t3", claim: "c3"},
	}, firstCallbacks)
	t.Log("Got first 2 expired tasks")
	// Ensure the items were deleted.
	inflightH, err := instance.Client.HGetAll(ctx, expWorker.Keys.InflightHash).Result()
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"t4": "c4"}, inflightH)
	inflightL, err := instance.Client.LRange(ctx, expWorker.Keys.ExpireList, 0, -1).Result()
	require.NoError(t, err)
	assert.Equal(t, []string{fmt.Sprintf("t4:%d", pushTime+2)}, inflightL)
	// Wait another 2 seconds for the last one to come in.
	last1Timer := time.NewTimer(2 * time.Second)
	var last1Callbacks []claimedTask
	defer last1Timer.Stop()
last1Collect:
	for {
		select {
		case <-last1Timer.C:
			break last1Collect
		case ct := <-callbacks:
			last1Callbacks = append(last1Callbacks, ct)
		}
	}
	// We should get exactly 1 task that expired.
	require.Equal(t, []claimedTask{
		{taskID: "t4", claim: "c4"},
	}, last1Callbacks)
	t.Log("Got last expired task")
	// Ensure the items were deleted.
	inflightH, err = instance.Client.HGetAll(ctx, expWorker.Keys.InflightHash).Result()
	require.NoError(t, err)
	assert.Len(t, inflightH, 0)
	inflightL, err = instance.Client.LRange(ctx, expWorker.Keys.ExpireList, 0, -1).Result()
	require.NoError(t, err)
	assert.Len(t, inflightL, 0)
}

type claimedTask struct {
	taskID string
	claim  string
}
