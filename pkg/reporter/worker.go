package reporter

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
)

type Worker struct {
	MaxDelay  time.Duration
	BatchSize uint

	ItemStore *items.Store
	Log       *zap.Logger
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
func (w *Worker) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		ok, err := w.nextBatch(session, claim)
		if err != nil {
			return err
		}
		if !ok {
			return nil // session closed
		}
	}
}

func (w *Worker) nextBatch(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (bool, error) {
	ctx := session.Context()
	timer := time.NewTimer(w.MaxDelay)
	defer timer.Stop()
	// Read message batch from Kafka.
	var results []*types.AssignmentResult
	var offset int64
readLoop:
	for i := uint(0); i < w.BatchSize; i++ {
		select {
		case <-timer.C:
			break readLoop
		case msg, ok := <-claim.Messages():
			if !ok {
				w.Log.Info("Incoming messages channel closed")
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
	w.Log.Debug("Read batch", zap.Int("result_count", len(results)))
	// Run batch through dedup.
	w.ItemStore.PushAssignmentResults(ctx, results)
	// Tell Kafka about consumer progress.
	session.MarkOffset(claim.Topic(), claim.Partition(), offset+1, "")
	session.Commit()
	w.Log.Debug("Flushed batch")
	return true, nil
}
