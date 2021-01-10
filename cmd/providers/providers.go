package providers

import (
	"github.com/spf13/cobra"
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
	NewWorkerAuthInterceptor,
	// mysql.go
	NewMySQL,
	// njobs.go
	NewNJobsOptions,
	NewNJobsPartition,
	NewNJobsRedis,
	// sarama.go
	NewSaramaConfig,
	NewSaramaClient,
}

func NewApp(cmd *cobra.Command, opts ...fx.Option) *fx.App {
	baseOpts := []fx.Option{
		fx.Provide(Providers),
		fx.Supply(cmd),
		fx.Supply(Log),
		fx.Logger(zap.NewStdLog(Log)),
		fx.Supply(otel.GetMeterProvider().Meter(cmd.Name())),
	}
	baseOpts = append(baseOpts, opts...)
	return fx.New(baseOpts...)
}

func NewCmd(invoke interface{}) func(cmd *cobra.Command, args []string) {
	return func(cmd *cobra.Command, args []string) {
		app := fx.New(
			fx.Provide(Providers),
			fx.Supply(cmd),
			fx.Supply(args),
			fx.Supply(Log),
			fx.Logger(zap.NewStdLog(Log)),
			fx.Invoke(invoke),
		)
		app.Run()
	}
}
