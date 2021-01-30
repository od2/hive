package dedup

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
	defer instance.Close(t)

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
	assert.EqualError(t, bitmap.AddItems(ctx, []Item{
		StringItem("1"),
		StringItem("3000"),
		StringItem("1023"),
		StringItem("a"),
	}), `item is not a number: "a"`)
	require.NoError(t, bitmap.AddItems(ctx, []Item{
		StringItem("1"),
		StringItem("3000"),
		StringItem("1023"),
	}))

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
	_, err = bitmap.DedupItems(ctx, []Item{
		StringItem("2999"),
		StringItem("d"),
		StringItem("3"),
		StringItem("3000"),
		StringItem("c"),
		StringItem("b"),
		StringItem("1"),
	})
	assert.EqualError(t, err, `item is not a number: "d"`)
	deduped, err := bitmap.DedupItems(ctx, []Item{
		StringItem("2999"),
		StringItem("2999"),
		StringItem("3"),
		StringItem("3000"),
		StringItem("1"),
	})
	require.NoError(t, err)
	assert.Equal(t, []Item{
		StringItem("3"),
		StringItem("2999"),
	}, deduped)

	// Add the same items again.
	require.NoError(t, bitmap.AddItems(ctx, []Item{
		StringItem("1"),
		StringItem("3000"),
		StringItem("1023"),
	}))
	cancel()
}
