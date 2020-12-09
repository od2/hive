package redisqueue

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive/pkg/redistest"
)

func TestProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := redistest.NewRedis(ctx, t)
	defer instance.Close()

	producer := Producer{
		Redis: instance.Client,
		Keys:  KeysForPrefix("Q"),
	}
	// Add tasks 1 through 4.
	require.NoError(t, producer.PushTasks(ctx, []string{"t1", "t2", "t3", "t4"}))
	members, err := instance.Client.SMembers(ctx, producer.Keys.PendingSet).Result()
	require.NoError(t, err)
	sort.Strings(members)
	assert.Equal(t, []string{"t1", "t2", "t3", "t4"}, members)
	// Add tasks 3 and 4.
	require.NoError(t, producer.PushTasks(ctx, []string{"t4", "t5"}))
	members, err = instance.Client.SMembers(ctx, producer.Keys.PendingSet).Result()
	require.NoError(t, err)
	sort.Strings(members)
	assert.Equal(t, []string{"t1", "t2", "t3", "t4", "t5"}, members)
}
