package providers

import (
	"context"
	"sync"

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
	// sarama.go
	NewSaramaConfig,
	NewSaramaClient,
	NewSaramaSyncProducer,
	// topology.go
	NewTopologyConfig,
	NewItemsFactory,
	func(t *topology.Config) *topology.RedisShardFactory { return t.RedisShardFactory },
	redisshard.NewFactory,
	// OpenTelemetry
	otel.GetMeterProvider,
}

// NewApp creates an fx.App with default infrastructure.
func NewApp(opts ...fx.Option) *fx.App {
	baseOpts := []fx.Option{
		fx.Provide(Providers...),
		fx.Supply(Log),
		fx.Logger(zap.NewStdLog(Log)),
	}
	baseOpts = append(baseOpts, opts...)
	return fx.New(baseOpts...)
}

// NewCmd creates an fx.App within a cobra command handler.
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

// RunWithContext runs the provided closure in a goroutine when the app starts.
// The passed context closes when the fx app stops.
// The lifecycle stop hook waits indefinitely for the closure to finish.
func RunWithContext(lc fx.Lifecycle, run func(ctx context.Context)) {
	wg := new(sync.WaitGroup)
	ctx, cancel := context.WithCancel(context.Background())
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			wg.Add(1)
			go func() {
				defer wg.Done()
				run(ctx)
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			cancel()
			wg.Wait()
			return nil
		},
	})
}
