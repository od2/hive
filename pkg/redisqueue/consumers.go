package redisqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// Consumers claim and process events on the queue.
type Consumers struct {
	// Required components
	Redis *redis.Client
	// Required config
	Keys Keys
	TTL  uint // inflight time-to-live in seconds (expire)
}

// ClaimTasks attaches random task IDs to a claimer and returns the task IDs.
func (c *Consumers) ClaimTasks(ctx context.Context, claimer string, n uint) ([]string, error) {
	// Script: Bulk move tasks from pending to claimed.
	// Argument 1: Claimer string
	// Argument 2: Task count
	// Key 1: Pending set
	// Key 2: In-flight hash
	// Key 3: Expire list
	// Returns list of task IDs.
	const claimScript = `
redis.replicate_commands()
local ret = {}
for i=1,ARGV[2],1 do
	local item = redis.call("SRANDMEMBER", KEYS[1])
	if not item then break end
	redis.call("SREM", KEYS[1], item)
	redis.call("HSET", KEYS[2], item, ARGV[1])
	redis.call("LPUSH", KEYS[3], string.format("%s:%d", item, ARGV[3]))
	table.insert(ret, item)
end
return ret
`
	now := time.Now().Unix()
	expTime := now + int64(c.TTL)
	res, err := c.Redis.Eval(ctx, claimScript,
		[]string{c.Keys.PendingSet, c.Keys.InflightHash, c.Keys.ExpireList},
		claimer, int64(n), expTime).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get claims via Lua: %w", err)
	}
	taskIDsI, ok := res.([]interface{})
	if !ok {
		return nil, fmt.Errorf("failed to get claims via Lua: invalid return %#v", res)
	}
	taskIDs := make([]string, len(taskIDsI))
	for i, taskIDI := range taskIDsI {
		taskID, ok := taskIDI.(string)
		if !ok {
			return nil, fmt.Errorf("invalid entry in claims batch: %#v", taskID)
		}
		taskIDs[i] = taskID
	}
	return taskIDs, nil
}

// ErrClaimedByOther gets raised when a consumer tries to access a claim it doesn't own.
var ErrClaimedByOther = errors.New("claimed by other")

// DropClaims removes any claims if they exist.
// Returns ErrClaimedByOther if any of the task IDs are claimed by another claimer.
func (c *Consumers) DropClaims(ctx context.Context, claimer string, taskIDs []string) error {
	// Script: Bulk remove claimed tasks.
	// Argument 1: Claimer string
	// Argument 2: List of task IDs
	// Key 1: In-flight hash
	// Returns whether claimer matches.
	const dropClaimsScript = `
redis.replicate_commands()
local ret = {}
local claims = redis.call("HMGET", KEYS[1], unpack(ARGV, 2))
for i=1,#claims,2 do
	if claims[i] ~= ARGV[1] then
		return 0
	end
end
redis.call("HDEL", KEYS[1], unpack(ARGV, 2))
return 1
`
	varargs := make([]interface{}, len(taskIDs)+1)
	varargs[0] = claimer
	for i, taskID := range taskIDs {
		varargs[i+1] = taskID
	}
	res, err := c.Redis.Eval(ctx, dropClaimsScript,
		[]string{c.Keys.InflightHash},
		varargs...).Result()
	if err != nil {
		return fmt.Errorf("failed to drop claims via Lua: %w", err)
	}
	authzed, ok := res.(int64)
	if !ok {
		return fmt.Errorf("invalid return from drop claims: %#v", res)
	}
	if authzed == 0 {
		return ErrClaimedByOther
	}
	return nil
}
