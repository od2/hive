package redisdedup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.od2.network/hive/pkg/redistest"
)

func TestBitMap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	instance := redistest.NewRedis(ctx, t)
	defer instance.Close()

	bitmap := BitMap{
		Redis:  instance.Client,
		Prefix: "BITMAP",
		Exp:    10, // 1024 keys per bitmap
	}

	// No-ops.
	require.NoError(t, bitmap.AddItems(ctx, nil))
	_, err := bitmap.DedupItems(ctx, nil)
	require.NoError(t, err)

	// Add some random items.
	assert.EqualError(t, bitmap.AddItems(ctx, []string{"1", "3000", "1023", "a"}), `item is not a number: "a"`)
	require.NoError(t, bitmap.AddItems(ctx, []string{"1", "3000", "1023"}))

	buf, err := bitmap.Redis.Get(ctx, "BITMAP-0").Bytes()
	require.NoError(t, err)
	bucket0Exp := make([]byte, 128)
	bucket0Exp[0] = 0x40
	bucket0Exp[127] = 0x01
	assert.Equal(t, bucket0Exp, buf)
	buf, err = bitmap.Redis.Get(ctx, "BITMAP-2").Bytes()
	require.NoError(t, err)
	bucket1Exp := make([]byte, 120)
	bucket1Exp[119] = 0x80
	assert.Equal(t, bucket1Exp, buf)

	// Try dedup.
	_, err = bitmap.DedupItems(ctx, []string{"2999", "d", "3", "3000", "c", "b", "1"})
	assert.EqualError(t, err, `item is not a number: "d"`)
	deduped, err := bitmap.DedupItems(ctx, []string{"2999", "2999", "3", "3000", "1"})
	require.NoError(t, err)
	assert.Equal(t, []string{"3", "2999"}, deduped)

	// Add the same items again.
	require.NoError(t, bitmap.AddItems(ctx, []string{"1", "3000", "1023"}))
}
