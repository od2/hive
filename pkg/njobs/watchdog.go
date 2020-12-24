package njobs

import (
	"context"
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
	panic("not implemented")
}
