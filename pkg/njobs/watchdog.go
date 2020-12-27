package njobs

import (
	"context"
	"fmt"
	"time"
)

// Watchdog observes the worker sessions for timeouts.
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
	wantSleep, err := w.evalSessionExpire(ctx, time.Now().Unix(), int64(w.Options.SessionExpireBatch))
	if err != nil {
		return fmt.Errorf("failed to run session expire script: %w", err)
	}
	var sleep time.Duration
	if wantSleep > w.Options.SessionExpireInterval {
		sleep = w.Options.SessionExpireInterval
	} else {
		sleep = wantSleep
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
