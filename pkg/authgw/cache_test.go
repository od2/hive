package authgw

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.od2.network/hive/pkg/token"
)

func TestCache_Invalidate(t *testing.T) {
	backend := &mockBackend{
		setValid: true,
	}
	cache, err := NewCache(backend, 16, 30*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	// Write some random entries to the cache.
	for i := 0; i < 64; i++ {
		_, err := cache.LookupSlow(context.TODO(), token.NewID())
		if err != nil {
			t.Fatal(err)
		}
	}
	// Write 8 more recent entries.
	var sps [8]*token.SignedPayload
	for i := range sps {
		sp := randomSignedPayload()
		sps[i] = sp
		_, err := cache.LookupToken(context.TODO(), sp.ID)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Pretend all tokens were revoked.
	backend.setValid = false
	// Re-request the entries.
	// The cache should be stale, since there were no invalidation events yet.
	for _, sp := range sps {
		info, err := cache.LookupToken(context.TODO(), sp.ID)
		assert.NoError(t, err)
		assert.True(t, info.Valid)
	}
	// Invalidate every other entry.
	for i := 1; i < 8; i += 2 {
		cache.Cache.Remove(sps[i].ID)
	}
	// Re-request the entries, agane.
	// Every other token ceases to be valid now.
	for i, sp := range sps {
		info, err := cache.LookupToken(context.TODO(), sp.ID)
		assert.NoError(t, err)
		assert.Equal(t, i%2 == 0, info.Valid, i)
	}
}

// mockBackend always returns the specified result.
type mockBackend struct {
	setValid bool
}

// LookupToken always returns the result specified in mockBackend.
func (m *mockBackend) LookupToken(_ context.Context, _ token.ID) (*TokenInfo, error) {
	return &TokenInfo{
		Valid: m.setValid,
	}, nil
}

// Assert mockBackend implements Backend.
var _ Backend = (*mockBackend)(nil)

var emptySecret = [32]byte{}

func randomSignedPayload() *token.SignedPayload {
	signer := token.NewSimpleSigner(&emptySecret)
	sp := signer.SignNoErr(token.NewID())
	return &sp
}
