package reporter

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/topology"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
)

// Worker runs the result reporting mechanism.
type Worker struct {
	MaxDelay  time.Duration
	BatchSize uint

	Factory *items.Factory
	Log     *zap.Logger
}

// Setup is called by sarama when the consumer group member starts.
func (w *Worker) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is called by sarama after the consumer group member stops.
func (w *Worker) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim runs the consumer loop. It reads batches of messages from Kafka.
func (w *Worker) ConsumeClaim(
	saramaSession sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	// Connect to shard.
	collectionName := topology.CollectionOfTopic(claim.Topic())
	if collectionName == "" {
		return fmt.Errorf("invalid topic: %s", claim.Topic())
	}
	itemsStore, err := w.Factory.GetStore(collectionName)
	if err != nil {
		return fmt.Errorf("failed to connect to collection (%s): %w", collectionName, err)
	}
	sess := session{
		Worker:  w,
		session: saramaSession,
		claim:   claim,
		items:   itemsStore,
	}
	for {
		ok, err := sess.nextBatch()
		if err != nil {
			return err
		}
		if !ok {
			return nil // session closed
		}
	}
}

type session struct {
	*Worker
	collection string
	session    sarama.ConsumerGroupSession
	claim      sarama.ConsumerGroupClaim
	items      *items.Store
}

func (s *session) nextBatch() (bool, error) {
	ctx := s.session.Context()
	timer := time.NewTimer(s.MaxDelay)
	defer timer.Stop()
	// Read message batch from Kafka.
	var results []*types.AssignmentResult
	var offset int64
readLoop:
	for i := uint(0); i < s.BatchSize; i++ {
		select {
		case <-timer.C:
			break readLoop
		case msg, ok := <-s.claim.Messages():
			if !ok {
				s.Log.Info("Incoming messages channel closed")
				return false, nil
			}
			offset = msg.Offset
			pointer := new(types.AssignmentResult)
			if err := proto.Unmarshal(msg.Value, pointer); err != nil {
				return false, fmt.Errorf("invalid Protobuf from Kafka: %w", err)
			}
			results = append(results, pointer)
		}
	}
	if len(results) <= 0 {
		return true, nil
	}
	s.Log.Debug("Read batch", zap.Int("result_count", len(results)))
	// Run batch through dedup.
	if err := s.items.PushAssignmentResults(ctx, results); err != nil {
		return false, err
	}
	// Tell Kafka about consumer progress.
	s.session.MarkOffset(s.claim.Topic(), s.claim.Partition(), offset+1, "")
	s.session.Commit()
	s.Log.Debug("Flushed batch")
	return true, nil
}
