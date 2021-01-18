package providers

import (
	"context"

	"github.com/spf13/cobra"
	"go.od2.network/hive/pkg/njobs"
	"go.od2.network/hive/pkg/topology"
	"go.od2.network/hive/pkg/topology/redisshard"
	"go.opentelemetry.io/otel"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Log is the global logger.
var Log *zap.Logger

// Providers holds constructors for shared components.
var Providers = []interface{}{
	// authgw.go
	NewAuthgwBackend,
	NewAuthgwSigner,
	NewAuthgwCache,
	NewWorkerAuthInterceptor,
	// mysql.go
	NewMySQL,
	// njobs.go
	njobs.NewRedisClient,
	// providers.go
	NewContext,
	// sarama.go
	NewSaramaConfig,
	NewSaramaClient,
	GetSaramaConsumerGroup,
	NewSaramaSyncProducer,
	// topology.go
	NewTopologyConfig,
	NewItemsFactory,
	func(t *topology.Config) *topology.RedisShardFactory { return t.RedisShardFactory },
	redisshard.NewFactory,
}

func NewApp(opts ...fx.Option) *fx.App {
	baseOpts := []fx.Option{
		fx.Provide(Providers...),
		fx.Supply(Log),
		fx.Logger(zap.NewStdLog(Log)),
		fx.Supply(otel.GetMeterProvider()),
	}
	baseOpts = append(baseOpts, opts...)
	return fx.New(baseOpts...)
}

func NewCmd(invoke interface{}) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		app := fx.New(
			fx.Provide(Providers...),
			fx.Supply(cmd),
			fx.Supply(args),
			fx.Supply(Log),
			fx.Logger(zap.NewStdLog(Log)),
			// Run the actual command in the injection phase.
			fx.Invoke(invoke),
			// When the injection phase finishes and the app "starts",
			// we are done and we can exit.
			fx.Invoke(func(lc fx.Lifecycle, shutdown fx.Shutdowner) {
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						return shutdown.Shutdown()
					},
				})
			}),
		)
		app.Run()
	}
}

func NewContext(lc fx.Lifecycle) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			cancel()
			return nil
		},
	})
	return ctx
}
