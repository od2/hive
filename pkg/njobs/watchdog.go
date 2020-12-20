package njobs

import (
	"context"
	"fmt"
	"time"
)

// Watchdog observes the in-flight tasks for timeouts.
//
// It's vital to run a watchdog per Kafka partition,
// otherwise Redis will leak memory.
//
// Internally it works off a Redis priority queue of future expiration events.
type Watchdog struct {
	*RedisClient
	Options     *Options
	Expirations chan<- []Expiration
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
	expirations, maxSleep, err := w.evalExpire(ctx, w.Options.ExpireBatch)
	if err != nil {
		return fmt.Errorf("failed to run expiration algorithm: %w", err)
	}
	w.Expirations <- expirations
	var sleep time.Duration
	if maxSleep > w.Options.ExpireInterval {
		sleep = w.Options.ExpireInterval
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
