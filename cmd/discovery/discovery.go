package discovery

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/discovery"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/topology"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Cmd is the discovery sub-command.
var Cmd = cobra.Command{
	Use:   "discovery",
	Short: "Run item discovery service.",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := providers.NewApp(fx.Invoke(Run))
		app.Run()
	},
}

// Discovery config keys.
const (
	ConfInterval = "discovery.interval"
	ConfBatch    = "discovery.batch"
)

func init() {
	viper.SetDefault(ConfInterval, 2*time.Second)
	viper.SetDefault(ConfBatch, uint(256))
}

type discoveryIn struct {
	fx.In

	Lifecycle fx.Lifecycle
	Shutdown  fx.Shutdowner
	Topology  *topology.Config
	Factory   *items.Factory
	Sarama    sarama.Client
	Producer  sarama.SyncProducer
}

// Run hooks the discovery service into the application lifecycle.
func Run(log *zap.Logger, inputs discoveryIn) {
	consumeTopics := make([]string, len(inputs.Topology.Collections))
	for i, coll := range inputs.Topology.Collections {
		consumeTopics[i] = topology.CollectionTopic(coll.Name, topology.TopicCollectionDiscovered)
	}
	// Set up SQL dedup.
	worker := &discovery.Worker{
		MaxDelay:  viper.GetDuration(ConfInterval),
		BatchSize: viper.GetUint(ConfBatch),

		Topology: inputs.Topology,
		Factory:  inputs.Factory,
		Producer: inputs.Producer,
		Log:      log,
	}
	consumerGroup, err := providers.GetSaramaConsumerGroup(inputs.Lifecycle, log, inputs.Sarama, "hive.discovery")
	if err != nil {
		log.Fatal("Failed to get consumer group", zap.Error(err))
	}
	providers.RunWithContext(inputs.Lifecycle, func(ctx context.Context) {
		defer inputs.Shutdown.Shutdown()
		for ctx.Err() == nil {
			if err := consumerGroup.Consume(ctx, consumeTopics, worker); err != nil {
				log.Error("Consumer group exited", zap.Error(err))
				return
			}
		}
	})
}
