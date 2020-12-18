package fromkafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"go.od2.network/hive/pkg/redisqueue"
	"go.od2.network/hive/pkg/types"
)

// Worker moves tasks from Kafka to Redis.
type Worker struct {
	Producer  *redisqueue.Producer
	MaxDelay  time.Duration
	BatchSize uint
}

// Setup is no-op.
func (w *Worker) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is no-op.
func (w *Worker) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim runs the worker.
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
	timer := time.NewTimer(w.MaxDelay)
	defer timer.Stop()
	// Read message batch from Kafka.
	var taskIDs []string
	var offset int64
	for i := uint(0); i < w.BatchSize; i++ {
		select {
		case <-timer.C:
			return true, nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return false, nil
			}
			offset = msg.Offset
			pointer := new(types.ItemPointer)
			if err := proto.Unmarshal(msg.Value, pointer); err != nil {
				return false, fmt.Errorf("invalid Protobuf from Kafka: %w", err)
			}
			if !pointer.Check() {
				return false, fmt.Errorf("pointer from Kafka did not pass validity check")
			}
			taskIDs = append(taskIDs, pointer.Dst.Id)
		}
	}
	// Write task batch to Redis.
	if err := w.Producer.PushTasks(context.TODO(), taskIDs); err != nil {
		return false, fmt.Errorf("failed to push tasks to Redis: %w", err)
	}
	// Tell Kafka about consumer progress.
	if len(taskIDs) > 0 {
		session.MarkOffset(claim.Topic(), claim.Partition(), offset+1, "")
		session.Commit()
	}
	return true, nil
}
