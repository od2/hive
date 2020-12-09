package redisqueue

import (
	"context"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive/pkg/redistest"
)

func TestConsumers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := redistest.NewRedis(ctx, t)
	defer instance.Close()

	consumers := Consumers{
		Redis: instance.Client,
		Keys:  KeysForPrefix("Q"),
		TTL:   5,
	}
	// Add 4 pending tasks.
	require.NoError(t, instance.Client.SAdd(ctx, consumers.Keys.PendingSet, "t1", "t2", "t3", "t4").Err())
	// Claim 3 for w1.
	w1Tasks, err := consumers.ClaimTasks(ctx, "w1", 3)
	require.NoError(t, err)
	assert.Len(t, w1Tasks, 3)
	// Ensure claim and expiration entries got created.
	w1Claims, err := instance.Client.HGetAll(ctx, consumers.Keys.InflightHash).Result()
	require.NoError(t, err)
	assert.Len(t, w1Claims, 3)
	for _, v := range w1Claims {
		assert.Equal(t, v, "w1")
	}
	w1Expires, err := instance.Client.LRange(ctx, consumers.Keys.ExpireList, 0, -1).Result()
	require.NoError(t, err)
	matchClaim, err := regexp.Compile("^t\\d:(\\d+)$")
	require.NoError(t, err)
	unixEpoch := time.Now().Unix()
	for _, claimExp := range w1Expires {
		matches := matchClaim.FindStringSubmatch(claimExp)
		require.Len(t, matches, 2)
		expEpoch, err := strconv.ParseInt(matches[1], 10, 64)
		require.NoError(t, err)
		shouldEpoch := unixEpoch + 5
		timeDiff := shouldEpoch - expEpoch
		if timeDiff < -2 || timeDiff > 2 {
			t.Fatalf("Time too far apart: %d vs %d (%d)", shouldEpoch, expEpoch, timeDiff)
		}
	}
	// Ensure pending items got removed.
	afterW1SCard, err := instance.Client.SCard(ctx, consumers.Keys.PendingSet).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), afterW1SCard)
	// Claim 2 for w2.
	w2Tasks, err := consumers.ClaimTasks(ctx, "w2", 2)
	require.NoError(t, err)
	assert.Len(t, w2Tasks, 1)
	// Ensure pending items got removed.
	emptyW1SCard, err := instance.Client.SCard(ctx, consumers.Keys.PendingSet).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), emptyW1SCard)
	// Drop claims.
	require.NoError(t, consumers.DropClaims(ctx, "w1", []string{w1Tasks[0], w1Tasks[1]}))
	afterDropHLen1, err := instance.Client.HLen(ctx, consumers.Keys.InflightHash).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), afterDropHLen1)
	// Drop claim we don't own.
	require.Equal(t, ErrClaimedByOther, consumers.DropClaims(ctx, "w2", []string{w1Tasks[2]}))
	afterDropHLen2, err := instance.Client.HLen(ctx, consumers.Keys.InflightHash).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(2), afterDropHLen2)
	// Drop Rest
	require.NoError(t, consumers.DropClaims(ctx, "w1", []string{w1Tasks[2]}))
	require.NoError(t, consumers.DropClaims(ctx, "w2", []string{w2Tasks[0]}))
	afterDropHLen3, err := instance.Client.HLen(ctx, consumers.Keys.InflightHash).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(0), afterDropHLen3)
}
