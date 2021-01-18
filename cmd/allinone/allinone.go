package allinone

import (
	"github.com/spf13/cobra"
	"go.od2.network/hive/cmd/assigner"
	"go.od2.network/hive/cmd/discovery"
	"go.od2.network/hive/cmd/management_api"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/cmd/reporter"
	"go.od2.network/hive/cmd/worker_api"
	"go.uber.org/fx"
)

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
	fx.Invoke(management_api.Run),
	// reporter
	fx.Invoke(reporter.Run),
	// worker_api
	fx.Provide(worker_api.Server),
	fx.Invoke(
		worker_api.NewDiscoveryServer,
		worker_api.NewAssignmentsServer,
	),
}
