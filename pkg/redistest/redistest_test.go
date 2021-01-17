package redistest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRedis(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	rd := NewRedis(ctx, t)
	defer rd.Close()
	assert.NoError(t, rd.Client.Ping(ctx).Err())
	t.Log("Ping success")
	cancel()
}
