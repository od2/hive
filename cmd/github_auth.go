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
			user := entry.(*user)
			log.Info("Cache hit", zap.String("user", user.Login))
			wr.WriteHeader(http.StatusOK)
			buf, err := json.Marshal(user)
			if err != nil {
				log.Error("Failed to marshal user", zap.Error(err))
				return
			}
			_, _ = wr.Write(buf)
			return
		}
		user, err := fetchUser(req.Context(), string(body))
		if err != nil {
			log.Error("Failed to fetch", zap.Error(err))
			wr.WriteHeader(http.StatusInternalServerError)
			return
		}
		log.Info("Cache miss", zap.String("user", user.Login))
		cache.Add(string(body), user)
		wr.WriteHeader(http.StatusOK)
		buf, err := json.Marshal(user)
		if err != nil {
			log.Error("Failed to marshal user", zap.Error(err))
			return
		}
		_, _ = wr.Write(buf)
	})
	// Start h2c (HTTP/2 over TCP) server.
	hs := &http.Server{
		Addr:    bind,
		Handler: handler,
	}
	log.Info("Starting server", zap.String("bind", bind))
	if err := hs.ListenAndServe(); err != nil {
		log.Fatal("Server failed", zap.Error(err))
	}
}

func fetchUser(ctx context.Context, authToken string) (*user, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.github.com/user", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "token "+authToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", res.StatusCode)
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	u := new(user)
	if err := json.Unmarshal(body, u); err != nil {
		return nil, err
	}
	return u, nil
}

type user struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
}
