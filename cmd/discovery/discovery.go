package discovery

import (
	"context"
	"sync"
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

	Lifecycle     fx.Lifecycle
	Shutdown      fx.Shutdowner
	Topology      *topology.Config
	Factory       *items.Factory
	Producer      sarama.SyncProducer
	ConsumerGroup sarama.ConsumerGroup
}

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
	innerCtx, cancel := context.WithCancel(context.Background())
	run := func() {
		defer inputs.Shutdown.Shutdown()
		for {
			if err := inputs.ConsumerGroup.Consume(innerCtx, consumeTopics, worker); err != nil {
				log.Error("Consumer group exited", zap.Error(err))
				return
			}
		}
	}
	var wg sync.WaitGroup
	inputs.Lifecycle.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			wg.Add(1)
			go func() {
				defer wg.Done()
				run()
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			return nil
		},
	})
	log.Info("Waiting for consumer group to exit")
	wg.Wait()
	log.Info("Finished")
}
