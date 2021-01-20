package worker

import (
	"testing"

	"go.od2.network/hive/cmd/providers/providerstest"
	"go.uber.org/fx"
)

func TestApp(t *testing.T) {
	providerstest.Validate(t,
		fx.Provide(Server),
		fx.Invoke(
			NewDiscoveryServer,
			NewAssignmentsServer,
		))
}
