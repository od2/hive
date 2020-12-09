package redisqueue

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// Config holds the Redis configuration.
type Config struct {
	PendingSet string
}

// ExpirationWorker loops over the expiration event queue,
// and dead-letters any tasks that were not finished within the TTL using a callback.
// It is safe to run multiple instances on the same keys.
type ExpirationWorker struct {
	// Required components
	Log      *zap.Logger
	Redis    *redis.Client
	Callback ExpireCallback
	// Required config
	Keys         Keys
	EmptyBackoff time.Duration // time to sleep when the queue is empty
	BatchSize    uint          // max tasks to drop at once using Lua script
}

// ExpireCallback is called when a claim for a task expires.
type ExpireCallback func(ctx context.Context, taskID string, claim string) error

// Run function runs expiration worker until the context is canceled.
func (e *ExpirationWorker) Run(ctx context.Context) error {
	for {
		if err := e.step(ctx); err != nil {
			return err
		}
	}
}

// step runs the expiration Lua script once, processes callbacks,
// and sleeps the minimum time until the next expiration can occur.
func (e *ExpirationWorker) step(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Script: Drop and return all tasks that have expired.
	// Argument 1: Batch size (max script iterations)
	// Argument 2: Unix epoch
	// Key 1: In-flight list
	// Key 2: In-flight hash
	// Returns table with
	// - "exp" set to seconds until next expiration, or -1 if no elements
	// - other task IDs mapping to task claims
	const expireScript = `
local ret = {}
local sleep = 0
for i=1,ARGV[1],1 do
	local item = redis.call("LINDEX", KEYS[1], -1)
	if not item then break end
	local sep = string.find(item, ":")
	if not sep then error("invalid item: " .. item) end
	local task_id = string.sub(item, 0, sep-1)
	local exp = tonumber(string.sub(item, sep+1))
	local t = tonumber(ARGV[2])
	sleep = exp - t
	if exp > t then break end
	redis.call("LTRIM", KEYS[1], 0, -2)
	local claim = redis.call("HGET", KEYS[2], task_id)
	if claim then
		table.insert(ret, claim)
		table.insert(ret, task_id)
		redis.log(redis.LOG_VERBOSE, "redisqueue: Expire " .. KEYS[1] .. " " .. task_id .. " claim " .. claim)
		redis.call("HDEL", KEYS[2], task_id)
	end
end
table.insert(ret, sleep)
table.insert(ret, "sleep")
return ret
`
	preWait := time.Now()
	preWaitEpoch := preWait.Unix()
	res, err := e.Redis.Eval(ctx, expireScript,
		[]string{e.Keys.ExpireList, e.Keys.InflightHash},
		e.BatchSize, preWaitEpoch, // args
	).Result()
	if err != nil {
		return fmt.Errorf("failed to drop expired keys via Lua: %w", err)
	}
	resParts, ok := res.([]interface{})
	if !ok || len(resParts) < 2 || len(resParts)%2 != 0 {
		return fmt.Errorf("failed to drop expired keys via Lua: invalid return %#v", resParts)
	}
	var sleepSecs int64
	var gotSleepParam bool
	for i := 0; i < len(resParts); i += 2 {
		entry, ok := resParts[i+1].(string)
		if !ok {
			return fmt.Errorf("invalid entry on expired batch: %#v %#v", resParts[i], resParts[i+1])
		}
		if entry == "sleep" {
			var ok bool
			sleepSecs, ok = resParts[i].(int64)
			if !ok {
				return fmt.Errorf("invalid sleep on expired batch: %#v", resParts[i])
			}
			gotSleepParam = true
		} else {
			claimStr, ok := resParts[i].(string)
			if !ok {
				return fmt.Errorf("invalid claim in expired batch: %#v", resParts[i])
			}
			if err := e.Callback(ctx, entry, claimStr); err != nil {
				return fmt.Errorf("callback failed: %w", err)
			}
		}
	}
	if !gotSleepParam {
		return fmt.Errorf("missing sleep on expired batch")
	}
	// Sleep until next expiration.
	var sleepDur time.Duration
	if len(resParts) <= 2 {
		sleepDur = e.EmptyBackoff
	} else if sleepSecs >= 0 {
		sleepDur = time.Duration(sleepSecs) * time.Second
	} else {
		sleepDur = 0
	}
	sleepTimer := time.NewTimer(sleepDur)
	defer sleepTimer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-sleepTimer.C:
		return nil
	}
}
