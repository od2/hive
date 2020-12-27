package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var rootCmd = cobra.Command{
	Use:   "hive",
	Short: "od2/hive server",

	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		var logConfig zap.Config
		if devMode {
			logConfig = zap.NewDevelopmentConfig()
		} else {
			logConfig = zap.NewProductionConfig()
		}
		var err error
		log, err = logConfig.Build()
		if err != nil {
			panic("failed to build logger: " + err.Error())
		}
	},
}

var devMode bool
var log *zap.Logger

func init() {
	persistentFlags := rootCmd.PersistentFlags()
	persistentFlags.BoolVar(&devMode, "dev", false, "Dev mode")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err.Error())
	}
}
