package providerstest

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"go.od2.network/hive/cmd/providers"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"
	"go.uber.org/zap/zaptest"
)

func Validate(t *testing.T, opts ...fx.Option) {
	opts = append(opts,
		fx.Supply(
			zaptest.NewLogger(t),
			context.Background(),
			metric.Meter{},
			new(cobra.Command),
		),
		fx.Logger(testFxLogger{t}),
		fx.Provide(providers.Providers...))
	assert.NoError(t, fx.ValidateApp(opts...))
}

type testFxLogger struct {
	testing.TB
}

func (l testFxLogger) Printf(fmt string, args ...interface{}) {
	l.Logf(fmt, args...)
}
