package ratelimit

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRateLimit(t *testing.T) {
	rl := NewRateLimit(1.0, 5)
	assert.Equal(t, time.Duration(0), round(rl.Count(1000, 1)))
	assert.Equal(t, time.Duration(0), round(rl.Count(1000, 5)))
	assert.Equal(t, 5000*time.Millisecond, round(rl.Count(1000, 5)))
	assert.Equal(t, 10000*time.Millisecond, round(rl.Count(1000, 5)))
	assert.Equal(t, 10000*time.Millisecond, round(rl.Count(1001, 0)))
	assert.Equal(t, 10000*time.Millisecond, round(rl.Count(1002, 0)))
	assert.Equal(t, 10000*time.Millisecond, round(rl.Count(1003, 0)))
	assert.Equal(t, 10000*time.Millisecond, round(rl.Count(1004, 0)))
	assert.Equal(t, 10000*time.Millisecond, round(rl.Count(1005, 0)))
	assert.Equal(t, 7000*time.Millisecond, round(rl.Count(1006, 0)))
	assert.Equal(t, 3999*time.Millisecond, round(rl.Count(1007, 0)))
	assert.Equal(t, 3999*time.Millisecond, round(rl.Count(1008, 3)))
	assert.Equal(t, 1000*time.Millisecond, round(rl.Count(1009, 0)))
	assert.Equal(t, 0*time.Millisecond, round(rl.Count(1010, 0)))
}

func round(dur time.Duration) time.Duration {
	return dur - (dur % time.Millisecond)
}
