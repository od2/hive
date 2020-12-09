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
func (p *Producer) PushTasks(ctx context.Context, taskIDs []string) error {
	sAddArgs := make([]interface{}, len(taskIDs))
	for i, taskID := range taskIDs {
		sAddArgs[i] = taskID
	}
	return p.Redis.SAdd(ctx, p.Keys.PendingSet, sAddArgs).Err()
}
