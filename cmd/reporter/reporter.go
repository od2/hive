package reporter

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/items"
	"go.od2.network/hive/pkg/reporter"
	"go.od2.network/hive/pkg/topology"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Cmd is the reporter sub-command.
var Cmd = cobra.Command{
	Use:   "reporter",
	Short: "Run result reporting service.",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := providers.NewApp(fx.Invoke(Run))
		app.Run()
	},
}

// Reporter config keys.
const (
	ConfInterval      = "reporter.interval"
	ConfBatch         = "reporter.batch"
	ConfConsumerGroup = "reporter.consumer_group"
)

func init() {
	viper.SetDefault(ConfInterval, 2*time.Second)
	viper.SetDefault(ConfBatch, uint(256))
	viper.SetDefault(ConfConsumerGroup, "discovery")
}

type reporterIn struct {
	fx.In

	Lifecycle fx.Lifecycle
	Shutdown  fx.Shutdowner
	Topology  *topology.Config
	Factory   *items.Factory
	Sarama    sarama.Client
}

func Run(log *zap.Logger, inputs reporterIn) {
	topics := make([]string, len(inputs.Topology.Collections))
	for i, coll := range inputs.Topology.Collections {
		topics[i] = topology.CollectionTopic(coll.Name, topology.TopicCollectionResults)
	}
	worker := &reporter.Worker{
		MaxDelay:  viper.GetDuration(ConfInterval),
		BatchSize: viper.GetUint(ConfBatch),
		Factory:   inputs.Factory,
		Log:       log,
	}
	consumerGroup, err := providers.GetSaramaConsumerGroup(inputs.Lifecycle, log, inputs.Sarama, "hive.reporter")
	if err != nil {
		log.Fatal("Failed to get consumer group", zap.Error(err))
	}
	providers.RunWithContext(inputs.Lifecycle, func(ctx context.Context) {
		defer inputs.Shutdown.Shutdown()
		for ctx.Err() == nil {
			if err := consumerGroup.Consume(ctx, topics, worker); err != nil {
				log.Error("Consumer group exited", zap.Error(err))
				return
			}
		}
	})
}
