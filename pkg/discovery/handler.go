package discovery

import (
	"context"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"go.od2.network/hive/pkg/types"
)

// Handler is used to report crawl discovered items to Kafka.
type Handler struct {
	Producer sarama.SyncProducer

	types.UnimplementedDiscoveryServer
}

// ReportDiscovered reports related found items while crawling.
func (h *Handler) ReportDiscovered(
	_ context.Context,
	req *types.ReportDiscoveredRequest,
) (*types.ReportDiscoveredResponse, error) {
	// TODO Mark worker in message
	// TODO Verify collections
	msgs := make([]*sarama.ProducerMessage, len(req.Pointers))
	for i, ptr := range req.Pointers {
		buf, err := proto.Marshal(ptr)
		if err != nil {
			return nil, fmt.Errorf("proto marshal failed: %w", err)
		}
		msgs[i] = &sarama.ProducerMessage{
			Topic: ptr.Dst.Collection + ".discovered",
			Value: sarama.ByteEncoder(buf),
		}
	}
	if err := h.Producer.SendMessages(msgs); err != nil {
		return nil, fmt.Errorf("failed to send messages to Kafka: %w", err)
	}
	// TODO Metrics
	return &types.ReportDiscoveredResponse{}, nil
}