package redisqueue

import (
	"context"

	"github.com/go-redis/redis/v8"
)

// Producer adds events to the queue.
// It is safe to run multiple instances on the queue.
type Producer struct {
	// Required components
	Redis *redis.Client
	// Required config
	Keys Keys
}

// PushTasks adds task IDs to the queue if they don't already exist.
func (p *Producer) PushTasks(ctx context.Context, kvps []KeyValue) error {
	hmSetArgs := make([]interface{}, len(kvps)*2)
	for i, kvp := range kvps {
		hmSetArgs[i*2] = kvp.Key
		hmSetArgs[i*2+1] = kvp.Value
	}
	return p.Redis.HMSet(ctx, p.Keys.PendingHash, hmSetArgs...).Err()
}

// Binary key-value pair.
type KeyValue struct {
	Key   []byte
	Value []byte
}
