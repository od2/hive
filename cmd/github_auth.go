package main

import (
	"net/http"

	lru "github.com/hashicorp/golang-lru"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.od2.network/hive/pkg/auth"
	"go.od2.network/hive/pkg/cachegc"
	"go.uber.org/zap"
)

var githubAuthCmd = cobra.Command{
	Use:   "github-auth",
	Short: "Run GitHub authentication microservice",
	Long:  "Runs a h2c server used by Envoy to construct GitHub identities from OAuth 2.0 tokens",
	Args:  cobra.NoArgs,
	Run:   runGitHubAuth,
}

func init() {
	flags := githubAuthCmd.Flags()
	flags.String("bind", ":8000", "Server bind")

	rootCmd.AddCommand(&githubAuthCmd)
}

func runGitHubAuth(cmd *cobra.Command, _ []string) {
	// Get flags
	flags := cmd.Flags()
	bind, err := flags.GetString("bind")
	if err != nil {
		panic(err)
	}
	// Cache
	baseCache, err := lru.New(viper.GetInt(ConfAuthgwCacheSize))
	if err != nil {
		panic("failed to create cache: " + err.Error())
	}
	cache := cachegc.NewCache(baseCache, viper.GetDuration(ConfAuthgwCacheTTL))
	idp := &auth.GitHubIdP{
		HTTP:  http.DefaultClient,
		Cache: cache,
		Log:   log,
	}
	// Start HTTP server.
	hs := &http.Server{
		Addr:    bind,
		Handler: idp,
	}
	log.Info("Starting server", zap.String("bind", bind))
	if err := hs.ListenAndServe(); err != nil {
		log.Fatal("Server failed", zap.Error(err))
	}
}
