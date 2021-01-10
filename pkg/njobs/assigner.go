package njobs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// Assigner implements a Kafka consumer to process tasks.
//
// The Assigner coordinates with Streamers via Redis for assigning tasks to currently active workers.
// Assignments get written to the respective Redis streams.
//
// It also runs an embedded Watchdog background routine for cleaning up stalled streams.
//
// Internally, it reads in a batches of items from a Kafka partition,
// then assigns them to as much Redis workers as possible.
// The offset is stored in Redis (not Kafka), starting from the earliest message.
type Assigner struct {
	RedisClient *RedisClient
	Log         *zap.Logger

	Metrics *AssignerMetrics
}

// Run starts streaming messages from Kafka in batches.
// The algorithm throttles Kafka consumption to match the speed at which nqueue workers consume.
func (a *Assigner) Run(msgs <-chan *sarama.ConsumerMessage) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Start watchdog background routine and listen for error.
	watchdog := Watchdog{
		RedisClient: a.RedisClient,
	}
	watchdogErrC := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(watchdogErrC)
		err := watchdog.Run(ctx)
		if ctx.Err() == nil {
			// Only if the context is still supposed to be alive,
			// then we respect watchdog failures.
			watchdogErrC <- err
		}
	}()
	// Build Redis nqueue assigner state.
	s := assignerState{
		Assigner: a,
		r:        a.RedisClient,
	}
	// Start consumer loop.
	ticker := time.NewTicker(a.RedisClient.AssignInterval)
	defer ticker.Stop()
loop:
	for {
		select {
		case err := <-watchdogErrC:
			if err != nil {
				return fmt.Errorf("error from watchdog: %w", err)
			}
		case <-ticker.C:
			if err := s.flush(ctx); err != nil {
				return err
			}
		case msg, ok := <-msgs:
			if !ok {
				break loop
			}
			s.window = append(s.window, msg)
			if uint(len(s.window)) >= s.r.Options.AssignBatch {
				if err := s.flush(ctx); err != nil {
					return err
				}
			}
		}
	}
	// Exit immediately without flushing batch.
	// Wait for Redis expiry agent though.
	cancel()
	wg.Wait()
	return nil
}

type assignerState struct {
	*Assigner
	r      *RedisClient
	window []*sarama.ConsumerMessage // unacknowledged messages
}

// flush loops doing flush attempts until all messages are assigned.
func (s *assignerState) flush(ctx context.Context) error {
	for {
		ok, err := s.flushStep(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if err := s.backOff(ctx); err != nil {
			return err
		}
	}
}

func (s *assignerState) backOff(ctx context.Context) error {
	timer := time.NewTimer(s.RedisClient.Options.AssignInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *assignerState) flushStep(ctx context.Context) (ok bool, err error) {
	if len(s.window) <= 0 {
		s.Log.Debug("Empty batch")
		return true, nil
	}
	lastOffset, count, assignErr := s.r.evalAssignTasks(ctx, s.window)
	if assignErr == ErrSeek {
		// Redis is ahead of Kafka.
		// This is weird, since we would expect Kafka to be more durable than Redis.
		// TODO Theoretically we can recover by seeking forward and just skipping old messages.
		return false, fmt.Errorf("consumer failed: Kafka behind Redis. recovery not implemented")
	} else if assignErr == ErrNoWorkers {
		// All workers are occupied or there are no workers at all.
		// This can happen when:
		// - there are no active workers.
		// - workers are slower than the Kafka message stream.
		ok = false
		// TODO Mark this in metrics.
	} else if assignErr != nil {
		return false, fmt.Errorf("failed to run assign tasks algorithm: %w", assignErr)
	} else {
		// Batch has been processed completely
		ok = true
	}
	atomic.StoreInt64(&s.Metrics.offset, lastOffset)
	s.Metrics.assigns.Add(ctx, count)
	if count > 0 {
		s.Log.Debug("Assigning tasks",
			zap.Int64("assigner.offset", lastOffset),
			zap.Int64("assigner.count", count),
			zap.Bool("assigner.ok", ok))
	}
	// Move messages from window to channel.
	for len(s.window) > 0 && s.window[0].Offset <= lastOffset {
		s.window = s.window[1:]
	}
	return
}

type AssignerMetrics struct {
	batches metric.Int64Counter
	offset  int64
	assigns metric.Int64Counter
}

func NewAssignerMetrics(m metric.Meter) (*AssignerMetrics, error) {
	metrics := new(AssignerMetrics)
	var err error
	metrics.batches, err = m.NewInt64Counter("assigner_batches")
	if err != nil {
		return nil, err
	}
	if _, err := m.NewInt64UpDownSumObserver("assigner_offset", func(_ context.Context, res metric.Int64ObserverResult) {
		res.Observe(atomic.LoadInt64(&metrics.offset))
	}); err != nil {
		return nil, err
	}
	metrics.assigns, err = m.NewInt64Counter("assigner_assigns_count")
	if err != nil {
		return nil, err
	}
	return metrics, nil
}
