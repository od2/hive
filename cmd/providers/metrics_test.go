package providers

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

func TestSetupPrometheus(t *testing.T) {
	GOMPrometheusSync = 100 * time.Millisecond
	handler, err := SetupPrometheus()
	require.NoError(t, err)
	require.NotNil(t, handler)

	saramaConf := sarama.NewConfig()
	saramaConf.MetricRegistry = metrics.DefaultRegistry
	gauge := metrics.DefaultRegistry.GetOrRegister("gom_gauge", metrics.NewGaugeFloat64()).(metrics.GaugeFloat64)
	gauge.Update(2)

	counter, err := otel.Meter("meter").NewInt64Counter("otel_counter")
	require.NoError(t, err)
	counter.Add(context.Background(), 2)

	time.Sleep(time.Second)

	dtos, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	var metricNames []string
	for _, dto := range dtos {
		name := dto.GetName()
		switch {
		case strings.HasPrefix(name, "go_"),
			strings.HasPrefix(name, "process_"):
			continue
		}
		metricNames = append(metricNames, dto.GetName())
	}
	sort.Strings(metricNames)
	assert.Equal(t, []string{
		"hive_gom_gauge",
		"otel_counter",
	}, metricNames)
}
