package allinone

import (
	"github.com/spf13/cobra"
	"go.od2.network/hive/cmd/assigner"
	"go.od2.network/hive/cmd/discovery"
	"go.od2.network/hive/cmd/management"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/cmd/reporter"
	"go.od2.network/hive/cmd/worker"
	"go.uber.org/fx"
)

// Cmd is the all-in-one sub-command.
var Cmd = cobra.Command{
	Use:   "all-in-one",
	Short: "Run all-in-one stack.",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, _ []string) {
		app := providers.NewApp(opts...)
		app.Run()
	},
}

var opts = []fx.Option{
	// assigner
	fx.Invoke(assigner.Run),
	// discovery
	fx.Invoke(discovery.Run),
	// management_api
	fx.Invoke(management.Run),
	// reporter
	fx.Invoke(reporter.Run),
	// worker_api
	fx.Provide(worker.Server),
	fx.Invoke(
		worker.NewDiscoveryServer,
		worker.NewAssignmentsServer,
	),
}
