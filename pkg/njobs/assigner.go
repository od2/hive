package njobs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-redis/redis/v8"
)

// Assigner implements a Kafka consumer group member to process tasks.
//
// It assigns each tasks to one or more workers.
// Assignments get written to the "Assignments" channel.
//
// Internally, it reads in a batches of items from Kafka,
// then assigns them to as much Redis workers as possible.
//
// It also runs a Watchdog background routine.
type Assigner struct {
	Redis   *redis.Client
	Options *Options
}

// Setup checks Redis connectivity.
func (a *Assigner) Setup(_ sarama.ConsumerGroupSession) error {
	return a.Redis.Ping(context.TODO()).Err()
}

// Cleanup does nothing.
func (a *Assigner) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim starts streaming messages from Kafka in batches.
// The algorithm throttles Kafka consumption to match the speed at which nqueue workers consume.
func (a *Assigner) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Build njobs Redis client.
	scripts, err := LoadScripts(ctx, a.Redis)
	if err != nil {
		return fmt.Errorf("failed to load scripts: %w", err)
	}
	r := &RedisClient{
		Redis:         a.Redis,
		Options:       a.Options,
		PartitionKeys: NewPartitionKeys(claim.Topic(), claim.Partition()),
		Scripts:       scripts,
	}
	// Start watchdog background routine and listen for error.
	watchdog := Watchdog{
		RedisClient: r,
		Options:     a.Options,
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
		session:  session,
		r:        r,
	}
	// Start consumer loop.
loop:
	for {
		select {
		case err := <-watchdogErrC:
			if err != nil {
				return fmt.Errorf("error from watchdog: %w", err)
			}
		case msg, ok := <-claim.Messages():
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
	session sarama.ConsumerGroupSession
	claim   sarama.ConsumerGroupClaim
	r       *RedisClient
	window  []*sarama.ConsumerMessage // unacknowledged messages
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
	timer := time.NewTimer(s.Options.AssignInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (s *assignerState) flushStep(ctx context.Context) (ok bool, err error) {
	lastOffset, assignErr := s.r.evalAssignTasks(ctx, s.window)
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
	// Move messages from window to channel.
	for len(s.window) > 0 && s.window[0].Offset <= lastOffset {
		s.window = s.window[1:]
	}
	// Commit offset to Kafka.
	s.session.MarkOffset(s.claim.Topic(), s.claim.Partition(), lastOffset+1, "")
	s.session.Commit()
	return
}
