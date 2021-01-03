// Package discovery is responsible finding new items in the pointers.
package discovery

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"go.od2.network/hive/pkg/dedup"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
)

// Worker consumes a Kafka stream of pointers to a collection.
type Worker struct {
	Dedup     dedup.Dedup
	MaxDelay  time.Duration
	BatchSize uint

	ItemStore *items.Store
	KafkaSink *KafkaSink
	Log       *zap.Logger
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
	ctx := context.TODO()

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
	w.Log.Debug("Read batch", zap.Int("discover_count", len(pointers)))
	// Run batch through dedup.
	preDedupItems := make([]dedup.Item, len(pointers))
	for i, ptr := range pointers {
		preDedupItems[i] = ptr
	}
	dedupItems, err := w.Dedup.DedupItems(ctx, preDedupItems)
	if err != nil {
		return false, fmt.Errorf("failed to dedup items: %w", err)
	}
	pointers = make([]*types.ItemPointer, len(dedupItems))
	for i, dedupItem := range dedupItems {
		pointers[i] = dedupItem.(*types.ItemPointer)
	}
	w.Log.Debug("Deduped batch", zap.Int("dedup_count", len(pointers)))
	// Write updates to items.
	if err := w.ItemStore.InsertDiscovered(ctx, pointers); err != nil {
		return false, fmt.Errorf("failed to insert newly discovered items: %w", err)
	}
	// Add items to dedup.
	if err := w.Dedup.AddItems(ctx, dedupItems); err != nil {
		return false, fmt.Errorf("failed to add items to dedup: %w", err)
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
	w.Log.Debug("Flushed batch")
	return true, nil
}
