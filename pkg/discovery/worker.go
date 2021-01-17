// Package discovery is responsible finding new items in the pointers.
package discovery

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"go.od2.network/hive/pkg/dedup"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/topology"
	"go.od2.network/hive/pkg/types"
	"go.uber.org/zap"
)

// Worker consumes a stream of discovered items,
// writing new items to the task queue.
type Worker struct {
	MaxDelay  time.Duration
	BatchSize uint

	Topology *topology.Config
	Factory  *items.Factory
	Producer sarama.SyncProducer
	Log      *zap.Logger
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
	// TODO Support other types of dedup
	deduper := dedup.SQL{Store: itemsStore}
	sess := session{
		Worker:  w,
		session: saramaSession,
		claim:   claim,
		items:   itemsStore,
		deduper: &deduper,
	}
	// Loop over messages.
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
	deduper    dedup.Dedup
	// TODO Have a logger per session
}

func (s *session) nextBatch() (bool, error) {
	ctx := s.session.Context()
	timer := time.NewTimer(s.MaxDelay)
	defer timer.Stop()
	// Read message batch from Kafka.
	var pointers []*types.ItemPointer
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
			pointer := new(types.ItemPointer)
			if err := proto.Unmarshal(msg.Value, pointer); err != nil {
				return false, fmt.Errorf("invalid Protobuf from Kafka: %w", err)
			}
			if err := pointer.Check(); err != nil {
				return false, fmt.Errorf("invalid pointer: %w", err)
			}
			pointers = append(pointers, pointer)
		}
	}
	if len(pointers) <= 0 {
		return true, nil
	}
	s.Log.Debug("Read batch", zap.Int("discover_count", len(pointers)))
	// Run batch through dedup.
	preDedupItems := make([]dedup.Item, len(pointers))
	for i, ptr := range pointers {
		preDedupItems[i] = ptr
	}
	dedupItems, err := s.deduper.DedupItems(ctx, preDedupItems)
	if err != nil {
		return false, fmt.Errorf("failed to dedup items: %w", err)
	}
	pointers = make([]*types.ItemPointer, len(dedupItems))
	for i, dedupItem := range dedupItems {
		pointers[i] = dedupItem.(*types.ItemPointer)
	}
	s.Log.Debug("Deduped batch", zap.Int("dedup_count", len(pointers)))
	if len(pointers) > 0 {
		// Write updates to items.
		if err := s.items.InsertDiscovered(ctx, pointers); err != nil {
			return false, fmt.Errorf("failed to insert newly discovered items: %w", err)
		}
		// Add items to dedup.
		if err := s.deduper.AddItems(ctx, dedupItems); err != nil {
			return false, fmt.Errorf("failed to add items to dedup: %w", err)
		}
		// Produce Kafka messages
		if s.Producer != nil {
			var messages []*sarama.ProducerMessage
			for _, pointer := range pointers {
				buf, err := proto.Marshal(pointer.Dst)
				if err != nil {
					return false, fmt.Errorf("failed to marshal protobuf: %w", err)
				}
				messages = append(messages, &sarama.ProducerMessage{
					Topic: topology.CollectionTopic(s.collection, "tasks"),
					Key:   sarama.StringEncoder(pointer.Dst.Id),
					Value: sarama.ByteEncoder(buf),
				})
			}
			if err := s.Producer.SendMessages(messages); err != nil {
				return false, fmt.Errorf("failed to produce Kafka messages: %w", err)
			}
		}
	}
	// Tell Kafka about consumer progress.
	if len(pointers) > 0 {
		s.session.MarkOffset(s.claim.Topic(), s.claim.Partition(), offset+1, "")
		s.session.Commit()
	}
	s.Log.Debug("Flushed batch")
	return true, nil
}
