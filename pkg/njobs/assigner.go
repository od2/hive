package njobs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"go.od2.network/hive/pkg/topology"
	"go.od2.network/hive/pkg/topology/redisshard"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/label"
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
	RedisFactory redisshard.Factory
	Producer     sarama.SyncProducer
	Topology     *topology.Config
	Log          *zap.Logger
}

// Setup is called by sarama when the consumer group member starts.
func (a *Assigner) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is called by sarama after the consumer group member stops.
func (a *Assigner) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim starts streaming messages from Kafka in batches.
// The algorithm throttles Kafka consumption to match the speed at which nqueue workers consume.
func (a *Assigner) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to shard.
	collectionName := topology.CollectionOfTopic(claim.Topic())
	collection := a.Topology.GetCollection(collectionName)
	if collection == nil {
		return fmt.Errorf("selected non-existent collection: %s", collectionName)
	}
	shard := topology.Shard{
		Collection: collectionName,
		Partition:  claim.Partition(),
	}
	redis, err := a.RedisFactory.GetShard(shard)
	if err != nil {
		return fmt.Errorf("failed to get Redis shard from factory: %w", err)
	}
	njobsRedis, err := NewRedisClient(redis, &shard, collection)
	if err != nil {
		return err
	}

	// Start watchdog background routine and listen for error.
	watchdog := Watchdog{
		RedisClient: njobsRedis,
	}
	watchdogErrC := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(watchdogErrC)
		err := watchdog.Run(ctx)
		if ctx.Err() == nil {
			watchdogErrC <- err
		}
	}()

	// Register shard to OpenTelemetry.
	metrics := newAssignerShardMetrics(shard)
	defer metrics.close()

	// Start forwarder background routine and listen for error.
	forwarder := forwarder{
		RedisClient: njobsRedis,
		Producer:    a.Producer,
		Topic:       topology.CollectionTopic(collectionName, topology.TopicCollectionResults),
		Log:         a.Log.Named("forwarder"),
		metrics:     metrics,
	}
	forwarderErrC := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(forwarderErrC)
		err := forwarder.run(ctx)
		if ctx.Err() == nil {
			forwarderErrC <- err
		}
	}()

	// Build Redis nqueue assigner state.
	s := assignerState{
		Assigner: a,
		r:        njobsRedis,
		session:  session,
		claim:    claim,
		metrics:  metrics,
	}
	defer s.metrics.close()
	// Start consumer loop.
	ticker := time.NewTicker(njobsRedis.Collection.AssignInterval)
	defer ticker.Stop()
loop:
	for {
		select {
		case err := <-watchdogErrC:
			if err != nil {
				return fmt.Errorf("error from watchdog: %w", err)
			}
		case err := <-forwarderErrC:
			if err != nil {
				return fmt.Errorf("error from forwarder: %w", err)
			}
		case <-ticker.C:
			if err := s.flush(ctx); err != nil {
				return err
			}
		case msg, ok := <-claim.Messages():
			if !ok {
				break loop
			}
			s.window = append(s.window, msg)
			if uint(len(s.window)) >= s.r.Collection.AssignBatch {
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
	r       *RedisClient
	window  []*sarama.ConsumerMessage // unacknowledged messages
	session sarama.ConsumerGroupSession
	claim   sarama.ConsumerGroupClaim
	metrics *assignerShardMetrics
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
	timer := time.NewTimer(s.r.Collection.AssignInterval)
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
		return true, nil
	}
	lastOffset, count, assignErr := s.r.evalAssignTasks(ctx, s.window)
	if assignErr == ErrSeek {
		// Redis is ahead of Kafka.
		// This is weird, since we would expect Kafka to be more durable than Redis.
		offset, err := s.r.GetOffset(ctx)
		if err != nil {
			return false, fmt.Errorf("failed to get offset during recovery: %w", err)
		}
		s.session.MarkOffset(s.claim.Topic(), s.claim.Partition(), offset+1, "")
		s.session.Commit()
		s.Log.Error("Kafka behind Redis, this should not normally happen, seeking forward",
			zap.Int64("kafka.old_offset", s.window[len(s.window)-1].Offset),
			zap.Int64("kafka.new_offset", offset+1))
		return false, fmt.Errorf("consumer failed: Kafka behind Redis")
	} else if assignErr == ErrNoWorkers {
		// All workers are occupied or there are no workers at all.
		// This can happen when:
		// - there are no active workers.
		// - workers are slower than the Kafka message stream.
		ok = false
	} else if assignErr != nil {
		return false, fmt.Errorf("failed to run assign tasks algorithm: %w", assignErr)
	} else {
		// Batch has been processed completely
		ok = true
	}
	// Update metrics.
	atomic.StoreInt64(&s.metrics.offset, lastOffset)
	atomic.AddInt64(&s.metrics.assigns, count)
	atomic.AddInt64(&s.metrics.assignBatches, 1)
	if ok {
		atomic.AddInt64(&s.metrics.assignProgressBatches, 1)
	}
	if count > 0 {
		s.Log.Debug("Assigning tasks",
			zap.Int64("assigner.offset", lastOffset),
			zap.Int64("assigner.count", count),
			zap.Bool("assigner.ok", ok))
	}
	// Move messages from window to channel, update Kafka.
	if len(s.window) > 0 {
		s.session.MarkMessage(s.window[len(s.window)-1], "")
		s.session.Commit() // commit, since auto-commit is off
	}
	for len(s.window) > 0 && s.window[0].Offset <= lastOffset {
		s.window = s.window[1:]
	}
	return
}

// assignerMetrics holds OpenTelemetry metrics on the assigner.
type assignerMetrics struct {
	observer metric.BatchObserver
	lock     sync.Mutex
	shards   map[topology.Shard]*assignerShardMetrics
	// Assigner
	assignBatches         metric.Int64SumObserver
	assignProgressBatches metric.Int64SumObserver
	assigns               metric.Int64SumObserver
	offsets               metric.Int64ValueObserver
	// Forwarder
	forwardBatches metric.Int64SumObserver
	forwardResults metric.Int64SumObserver
}

// assignerMetrics singleton.
var theAssignerMetrics assignerMetrics
var onceAssignerMetrics sync.Once

// assignerMetrics holds OpenTelemetry metrics of a single shard.
type assignerShardMetrics struct {
	shard topology.Shard
	kvs   []label.KeyValue
	// Assigner
	assignBatches         int64 // atomic
	assignProgressBatches int64 // atomic
	assigns               int64 // atomic
	offset                int64 // atomic
	// Forwarder
	forwardBatches int64 // atomic
	forwardResults int64 // atomic
}

// newAssignerShardMetrics creates metrics for an assigner shard.
func newAssignerShardMetrics(shard topology.Shard) *assignerShardMetrics {
	m := getAssignerMetrics()
	m.lock.Lock()
	defer m.lock.Unlock()
	shardMetrics := &assignerShardMetrics{
		shard: shard,
		kvs: []label.KeyValue{
			label.String("hive.collection", shard.Collection),
			label.Int32("hive.partition", shard.Partition),
		},
	}
	m.shards[shard] = shardMetrics
	return shardMetrics
}

// close stops metrics collections for an assigner shard.
func (a *assignerShardMetrics) close() {
	m := getAssignerMetrics()
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.shards, a.shard)
}

// getAssignerMetrics gets the assigner metrics singleton.
func getAssignerMetrics() *assignerMetrics {
	onceAssignerMetrics.Do(theAssignerMetrics.init)
	return &theAssignerMetrics
}

// init should not be called directly.
func (a *assignerMetrics) init() {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.shards = make(map[topology.Shard]*assignerShardMetrics)
	meter := otel.Meter("hive.assigner")
	obs := meter.NewBatchObserver(func(ctx context.Context, res metric.BatchObserverResult) {
		a.lock.Lock()
		defer a.lock.Unlock()
		for _, shard := range a.shards {
			res.Observe(shard.kvs,
				a.assignBatches.Observation(atomic.LoadInt64(&shard.assignBatches)),
				a.assignProgressBatches.Observation(atomic.LoadInt64(&shard.assignProgressBatches)),
				a.assigns.Observation(atomic.LoadInt64(&shard.assigns)),
				a.offsets.Observation(atomic.LoadInt64(&shard.offset)),
				a.forwardBatches.Observation(atomic.LoadInt64(&shard.forwardBatches)),
				a.forwardResults.Observation(atomic.LoadInt64(&shard.forwardResults)))
		}
	})
	var err error
	a.assignBatches, err = obs.NewInt64SumObserver("hive_assigner_batch_count",
		metric.WithDescription("Hive task assignment batches processed"))
	if err != nil {
		panic(err)
	}
	a.assignProgressBatches, err = obs.NewInt64SumObserver("hive_assigner_batch_progress_count",
		metric.WithDescription("Hive task assignment batches with progress processed"))
	if err != nil {
		panic(err)
	}
	a.offsets, err = obs.NewInt64ValueObserver("hive_assigner_offset",
		metric.WithDescription("Hive latest task assignment Kafka offset"))
	if err != nil {
		panic(err)
	}
	a.assigns, err = obs.NewInt64SumObserver("hive_assigner_assign_count",
		metric.WithDescription("Hive task assignments total"))
	if err != nil {
		panic(err)
	}
	a.forwardBatches, err = obs.NewInt64SumObserver("hive_assigner_forward_batch_count",
		metric.WithDescription("Hive task result batches registered"))
	if err != nil {
		panic(err)
	}
	a.forwardResults, err = obs.NewInt64SumObserver("hive_assigner_forward_result_count",
		metric.WithDescription("Hive task results count registered"))
	if err != nil {
		panic(err)
	}
}
