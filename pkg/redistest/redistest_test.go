package redistest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()
	rd := NewRedis(ctx, t)
	defer rd.Close(t)
	assert.NoError(t, rd.Client.Ping(ctx).Err())
	t.Log("Ping success")
	cancel()
}
