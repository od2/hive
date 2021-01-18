package main

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive/cmd/admin_tool"
	"go.od2.network/hive/cmd/assigner"
	"go.od2.network/hive/cmd/discovery"
	"go.od2.network/hive/cmd/management_api"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/cmd/reporter"
	"go.od2.network/hive/cmd/worker_api"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/metric/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config keys.
const (
	ConfInternalListen = "internal.listen"
)

func init() {
	viper.SetDefault(ConfInternalListen, "")
}

var rootCmd = cobra.Command{
	Use:   "hive",
	Short: "od2/hive server",

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if configPath != "" {
			viper.SetConfigFile(configPath)
			viper.SetConfigType("toml")
			if err := viper.ReadInConfig(); err != nil {
				panic("Failed to read config: " + err.Error())
			}
		}
		viper.SetEnvPrefix("od2")
		viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		viper.AutomaticEnv()

		var logConfig zap.Config
		if devMode {
			logConfig = zap.NewDevelopmentConfig()
		} else {
			logConfig = zap.NewProductionConfig()
		}
		logConfig.DisableCaller = true
		logConfig.DisableStacktrace = true
		logConfig.Level.SetLevel(zapcore.DebugLevel) // TODO Configurable log level
		log, err := logConfig.Build()
		if err != nil {
			panic("failed to build logger: " + err.Error())
		}
		providers.Log = log
		sarama.Logger, err = zap.NewStdLogAt(log.Named("sarama"), zap.InfoLevel)
		if err != nil {
			log.Fatal("Failed to build sarama logger", zap.Error(err))
		}

		internalListen := viper.GetString(ConfInternalListen)
		if internalListen != "" {
			exporter, err := prometheus.NewExportPipeline(prometheus.Config{})
			if err != nil {
				log.Fatal("Failed to build Prometheus exporter")
			}
			otel.SetMeterProvider(exporter.MeterProvider())
			serveMux := http.NewServeMux()
			serveMux.Handle("/metrics", exporter)
			serveMux.HandleFunc("/debug/pprof/", pprof.Index)
			serveMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			serveMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			serveMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			serveMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			go http.ListenAndServe(internalListen, serveMux)
		}
	},
}

var devMode bool
var configPath string

func init() {
	persistentFlags := rootCmd.PersistentFlags()
	persistentFlags.BoolVar(&devMode, "dev", false, "Dev mode")
	persistentFlags.StringVarP(&configPath, "config", "c", "", "Config file")

	rootCmd.AddCommand(
		&admin_tool.Cmd,
		&assigner.Cmd,
		&discovery.Cmd,
		&management_api.Cmd,
		&reporter.Cmd,
		&worker_api.Cmd,
	)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
	}
}
