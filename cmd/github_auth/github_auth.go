package github_auth

import (
	"context"
	"net/http"

	"github.com/spf13/cobra"
	"go.od2.network/hive/cmd/providers"
	"go.od2.network/hive/pkg/auth"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

var Cmd = cobra.Command{
	Use:   "github-auth",
	Short: "Run GitHub authentication microservice",
	Long:  "Runs a h2c server used by Envoy to construct GitHub identities from OAuth 2.0 tokens",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		app := providers.NewApp(
			cmd,
			fx.Provide(newFlags),
			fx.Invoke(runGitHubAuth),
		)
		app.Run()
	},
}

func init() {
	flags := Cmd.Flags()
	flags.String("bind", ":8000", "Server bind")
}

type flags struct {
	bind string
}

func newFlags(cmd *cobra.Command) *flags {
	f := cmd.Flags()
	bind, err := f.GetString("bind")
	if err != nil {
		panic(err)
	}
	return &flags{
		bind: bind,
	}
}

func runGitHubAuth(
	lc fx.Lifecycle,
	log *zap.Logger,
	fl *flags,
	cache providers.AuthgwCache,
) {
	// Cache
	idp := &auth.GitHubIdP{
		HTTP:  http.DefaultClient,
		Cache: cache.Cache,
		Log:   log,
	}
	// Start HTTP server.
	hs := &http.Server{
		Addr:    fl.bind,
		Handler: idp,
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				log.Info("Starting server", zap.String("bind", fl.bind))
				if err := hs.ListenAndServe(); err != nil {
					log.Fatal("Server failed", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: hs.Shutdown,
	})
}
