package main

import (
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var rootCmd = cobra.Command{
	Use:   "hive",
	Short: "od2/hive server",

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if configPath == "" {
			_, _ = fmt.Fprintln(os.Stderr, "No config path given")
			os.Exit(1)
		}
		viper.SetConfigFile(configPath)
		viper.SetConfigType("toml")
		if err := viper.ReadInConfig(); err != nil {
			panic("Failed to read config: " + err.Error())
		}
		var logConfig zap.Config
		if devMode {
			logConfig = zap.NewDevelopmentConfig()
		} else {
			logConfig = zap.NewProductionConfig()
		}
		logConfig.DisableCaller = true
		logConfig.DisableStacktrace = true
		logConfig.Level.SetLevel(zapcore.DebugLevel) // TODO Configurable log level
		var err error
		log, err = logConfig.Build()
		if err != nil {
			panic("failed to build logger: " + err.Error())
		}
	},
}

var devMode bool
var log *zap.Logger
var configPath string

func init() {
	persistentFlags := rootCmd.PersistentFlags()
	persistentFlags.BoolVar(&devMode, "dev", false, "Dev mode")
	persistentFlags.StringVarP(&configPath, "config", "c", "", "Config file")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
	}
}
