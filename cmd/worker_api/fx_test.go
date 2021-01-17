package worker_api

import (
	"testing"

	"go.od2.network/hive/cmd/providers/providerstest"
	"go.uber.org/fx"
)

func TestApp(t *testing.T) {
	providerstest.Validate(t,
		fx.Provide(
			newWorkerAPIFlags,
			runWorkerAPI,
		),
		fx.Invoke(
			newDiscoveryServer,
			newAssignmentsServer,
		))
}
