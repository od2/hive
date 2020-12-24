package njobs

import (
	"context"
	"fmt"
	"time"
)

// Watchdog observes the in-flight tasks or worker sessions for timeouts.
//
// Internally it works off a Redis priority queue of future expiration events.
// Confirmed expiration events are written out to Redis Streams.
type Watchdog struct {
	*RedisClient
	Options *Options
}

// Run starts a loop that processes expirations.
func (w *Watchdog) Run(ctx context.Context) error {
	for {
		if err := w.step(ctx); err != nil {
			return err
		}
	}
}

func (w *Watchdog) step(ctx context.Context) error {
	maxSleep, err := w.evalExpire(ctx, w.Options.TaskExpireBatch)
	if err != nil {
		return fmt.Errorf("failed to run expiration algorithm: %w", err)
	}
	var sleep time.Duration
	if maxSleep > w.Options.TaskExpireInterval {
		sleep = w.Options.TaskExpireInterval
	} else {
		sleep = maxSleep
	}
	timer := time.NewTimer(sleep)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
