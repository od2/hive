// Package discovery is responsible finding new items in the pointers.
package discovery

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"go.od2.network/hive/pkg/db"
	"go.od2.network/hive/pkg/redisdedup"
	"go.od2.network/hive/pkg/types"
)

// Worker consumes a Kafka stream of pointers to a collection.
type Worker struct {
	Dedup     redisdedup.Dedup
	MaxDelay  time.Duration
	BatchSize uint

	ItemStore *db.ItemStore
	KafkaSink *KafkaSink
}

type KafkaSink struct {
	Producer sarama.SyncProducer
	Topic    string
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
	timer := time.NewTimer(w.MaxDelay)
	defer timer.Stop()
	// Read message batch from Kafka.
	var pointers []*types.ItemPointer
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
			pointers = append(pointers, pointer)
		}
	}
	// Run batch through dedup.
	preDedupItems := make([]redisdedup.Item, len(pointers))
	for _, ptr := range pointers {
		preDedupItems = append(preDedupItems, ptr)
	}
	dedupItems, err := w.Dedup.DedupItems(context.Background(), preDedupItems)
	if err != nil {
		return false, fmt.Errorf("failed to dedup items: %w", err)
	}
	pointers = make([]*types.ItemPointer, len(dedupItems))
	for i, dedupItem := range dedupItems {
		pointers[i] = dedupItem.(*types.ItemPointer)
	}
	// Write updates to items.
	if err := w.ItemStore.InsertNewlyDiscovered(context.TODO(), pointers); err != nil {
		return false, fmt.Errorf("failed to insert newly discovered items: %w", err)
	}
	// Produce Kafka messages
	if w.KafkaSink != nil {
		var messages []*sarama.ProducerMessage
		for _, pointer := range pointers {
			buf, err := proto.Marshal(pointer.Dst)
			if err != nil {
				return false, fmt.Errorf("failed to marshal protobuf: %w", err)
			}
			messages = append(messages, &sarama.ProducerMessage{
				Topic: w.KafkaSink.Topic,
				Value: sarama.ByteEncoder(buf),
			})
		}
		if err := w.KafkaSink.Producer.SendMessages(messages); err != nil {
			return false, fmt.Errorf("failed to produce Kafka messages: %w", err)
		}
	}
	// Tell Kafka about consumer progress.
	if len(pointers) > 0 {
		session.MarkOffset(claim.Topic(), claim.Partition(), offset+1, "")
		session.Commit()
	}
	return true, nil
}