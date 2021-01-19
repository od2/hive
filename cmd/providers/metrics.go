package providers

import (
	"fmt"
	"net/http"
	"time"

	prometheusmetrics "github.com/deathowl/go-metrics-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rcrowley/go-metrics"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/metric/prometheus"
)

// GOMPrometheusSync specifies the time interval to sync go-metrics to Prometheus.
var GOMPrometheusSync = 5 * time.Second

// SetupPrometheus configures the OpenTelemetry and go-metrics Prometheus exporters.
// Returns the Prometheus exporter HTTP handler.
func SetupPrometheus() (http.Handler, error) {
	// Setup go-metrics Prometheus exporter.
	gomProvder := prometheusmetrics.NewPrometheusProvider(
		metrics.DefaultRegistry,
		"hive", "",
		prometheus.DefaultRegisterer,
		GOMPrometheusSync)
	go gomProvder.UpdatePrometheusMetrics()
	// Set up OpenTelemetry Prometheus exporter.
	exporter, err := otelprom.NewExportPipeline(otelprom.Config{
		Registerer: prometheus.DefaultRegisterer,
		Gatherer:   prometheus.DefaultGatherer,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to build OpenTelemetry Prometheus exporter: %w", err)
	}
	otel.SetMeterProvider(exporter.MeterProvider())
	return exporter, nil
}
