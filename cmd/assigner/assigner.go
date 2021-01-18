package assigner

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/topology"
	"go.od2.network/hive/pkg/topology/redisshard"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Cmd is the assigner sub-command.
var Cmd = cobra.Command{
	Use:   "assigner",
	Short: "Run task assigner.",
	Long: "Runs the background process responsible for assigning tasks to workers.\n" +
		"Only one assigner is required per partition.\n" +
		"Running multiple assigners is allowed during surge upgrades.",
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := providers.NewApp(fx.Invoke(Run))
		app.Run()
	},
}

type assignerIn struct {
	fx.In

	Ctx           context.Context
	Lifecycle     fx.Lifecycle
	Shutdown      fx.Shutdowner
	Topology      *topology.Config
	RedisFactory  redisshard.Factory
	ConsumerGroup sarama.ConsumerGroup
	Producer      sarama.SyncProducer
	Metrics       *njobs.AssignerMetrics
}

func Run(log *zap.Logger, inputs assignerIn) {
	// Spin up assigner.
	assigner := njobs.Assigner{
		RedisFactory: inputs.RedisFactory,
		Producer:     inputs.Producer,
		Topology:     inputs.Topology,
		Log:          log,
		Metrics:      inputs.Metrics,
	}
	run := func() {
		// Create list of topics.
		topics := make([]string, len(inputs.Topology.Collections))
		for i, coll := range inputs.Topology.Collections {
			topics[i] = topology.CollectionTopic(coll.Name, topology.TopicCollectionTasks)
		}
		if err := inputs.ConsumerGroup.Consume(inputs.Ctx, topics, &assigner); err != nil {
			log.Error("Assigner failed", zap.Error(err))
			if err := inputs.Shutdown.Shutdown(); err != nil {
				log.Fatal("Failed to shut down", zap.Error(err))
			}
		}
	}
	inputs.Lifecycle.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			go run()
			return nil
		},
	})
}
