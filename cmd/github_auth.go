package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	lru "github.com/hashicorp/golang-lru"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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
	// Build HTTP handler.
	handler := http.HandlerFunc(func(wr http.ResponseWriter, req *http.Request) {
		log := log.With(zap.String("remote_addr", req.RemoteAddr))
		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			log.Error("Failed to read request body", zap.Error(err))
			return
		}
		wr.Header().Set("Content-Type", "text/plain")
		entry, ok := cache.Get(string(body))
		if ok {
			wr.WriteHeader(http.StatusOK)
			_, _ = wr.Write([]byte(entry.(string)))
			return
		}
		login, err := fetchLogin(req.Context(), string(body))
		if err != nil {
			log.Error("Failed to fetch", zap.Error(err))
			wr.WriteHeader(http.StatusInternalServerError)
			return
		}
		cache.Add(string(body), login)
		wr.WriteHeader(http.StatusOK)
		_, _ = wr.Write([]byte(login))
	})
	// Start h2c (HTTP/2 over TCP) server.
	hs := &http.Server{
		Addr:    bind,
		Handler: handler,
	}
	log.Info("Starting server", zap.String("bind", bind))
	if err := hs.ListenAndServe(); err != nil {
		log.Fatal("Server failed")
	}
}

func fetchLogin(ctx context.Context, authToken string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.github.com/user", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "token "+authToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", res.StatusCode)
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	var u user
	if err := json.Unmarshal(body, &u); err != nil {
		return "", err
	}
	return u.Login, nil
}

type user struct {
	Login string `json:"login"`
}
