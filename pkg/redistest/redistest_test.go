package redistest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRedis(t *testing.T) {
	ctx := context.TODO()
	rd := NewRedis(ctx, t)
	defer rd.Close()
	assert.NoError(t, rd.Client.Ping(ctx).Err())
}
