package saramamock

import (
	"context"

	"github.com/Shopify/sarama"
)

// ConsumerGroupSession is a fake sarama.ConsumerGroupSession.
type ConsumerGroupSession struct {
	MClaims       map[string][]int32
	MMemberID     string
	MContext      context.Context
	MGenerationID int32
}

// Claims returns what's saved.
func (m *ConsumerGroupSession) Claims() map[string][]int32 {
	return m.Claims()
}

// MemberID returns what's saved.
func (m *ConsumerGroupSession) MemberID() string {
	return m.MMemberID
}

// GenerationID returns what's saved.
func (m *ConsumerGroupSession) GenerationID() int32 {
	return m.MGenerationID
}

// MarkOffset does nothing.
func (*ConsumerGroupSession) MarkOffset(_ string, _ int32, _ int64, _ string) {
	return
}

// Commit does nothing.
func (*ConsumerGroupSession) Commit() {
	return
}

// ResetOffset does nothing.
func (*ConsumerGroupSession) ResetOffset(_ string, _ int32, _ int64, _ string) {
	return
}

// MarkMessage does nothing.
func (*ConsumerGroupSession) MarkMessage(_ *sarama.ConsumerMessage, _ string) {
	return
}

// Context returns what's saved.
func (m *ConsumerGroupSession) Context() context.Context {
	return m.MContext
}

var _ sarama.ConsumerGroupSession = (*ConsumerGroupSession)(nil)

// ConsumerGroupClaimGenerate is a fake sarama.ConsumerGroupClaim that generates messages.
type ConsumerGroupClaimGenerate struct {
	// NextMessage generates a Kafka message. Does not need to be thread-safe.
	NextMessage func() *sarama.ConsumerMessage
	msgChan     chan *sarama.ConsumerMessage

	// Saved values.
	MTopic               string
	MPartition           int32
	MInitialOffset       int64
	MHighWaterMarkOffset int64
}

// Init must be called before using other methods.
func (c *ConsumerGroupClaimGenerate) Init() {
	c.msgChan = make(chan *sarama.ConsumerMessage)
}

// Topic returns the saved value.
func (c *ConsumerGroupClaimGenerate) Topic() string {
	return c.MTopic
}

// Partition returns the saved value.
func (c *ConsumerGroupClaimGenerate) Partition() int32 {
	return c.MPartition
}

// InitialOffset returns the saved value.
func (c *ConsumerGroupClaimGenerate) InitialOffset() int64 {
	return c.MInitialOffset
}

// HighWaterMarkOffset returns the saved offset.
func (c *ConsumerGroupClaimGenerate) HighWaterMarkOffset() int64 {
	return c.MHighWaterMarkOffset
}

// Messages returns the messages channel.
func (c *ConsumerGroupClaimGenerate) Messages() <-chan *sarama.ConsumerMessage {
	return c.msgChan
}

// Run generates messages until the context is closed.
// It can only be called once and will panic otherwise.
func (c *ConsumerGroupClaimGenerate) Run(ctx context.Context) error {
	defer close(c.msgChan)
	for {
		msg := c.NextMessage()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.msgChan <- msg:
			break // continue
		}
	}
}

var _ sarama.ConsumerGroupClaim = (*ConsumerGroupClaimGenerate)(nil)

// ConsumerGroupClaimChan is a fake sarama.ConsumerGroupClaim
// that returns messages from an existing channel.
type ConsumerGroupClaimChan struct {
	MsgChan chan *sarama.ConsumerMessage

	// Saved values.
	MTopic               string
	MPartition           int32
	MInitialOffset       int64
	MHighWaterMarkOffset int64
}

// Topic returns the saved value.
func (c *ConsumerGroupClaimChan) Topic() string {
	return c.MTopic
}

// Partition returns the saved value.
func (c *ConsumerGroupClaimChan) Partition() int32 {
	return c.MPartition
}

// InitialOffset returns the saved value.
func (c *ConsumerGroupClaimChan) InitialOffset() int64 {
	return c.MInitialOffset
}

// HighWaterMarkOffset returns the saved offset.
func (c *ConsumerGroupClaimChan) HighWaterMarkOffset() int64 {
	return c.MHighWaterMarkOffset
}

// Messages returns the messages channel.
func (c *ConsumerGroupClaimChan) Messages() <-chan *sarama.ConsumerMessage {
	return c.MsgChan
}

var _ sarama.ConsumerGroupClaim = (*ConsumerGroupClaimChan)(nil)
