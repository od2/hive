package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"go.od2.network/hive/pkg/cachegc"
	"go.uber.org/zap"
)

// GitHubIdP is a tiny GitHub-based identity provider,
// for introspecting the identity behind a GitHub OAuth 2.0 token.
//
// This service is implemented in Go because it was not practical to implement in Envoy.
type GitHubIdP struct {
	HTTP  *http.Client
	Cache *cachegc.Cache
	Log   *zap.Logger
}

// ServeHTTP handles identity requests.
func (g *GitHubIdP) ServeHTTP(wr http.ResponseWriter, req *http.Request) {
	log := g.Log.With(zap.String("remote_addr", req.RemoteAddr))
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Error("Failed to read request body", zap.Error(err))
		return
	}
	wr.Header().Set("Content-Type", "text/plain")
	entry, ok := g.Cache.Get(string(body))
	if ok {
		user := entry.(*GitHubUser)
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
	user, err := g.FetchUser(req.Context(), string(body))
	if err != nil {
		log.Error("Failed to fetch", zap.Error(err))
		wr.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Info("Cache miss", zap.String("user", user.Login))
	g.Cache.Add(string(body), user)
	wr.WriteHeader(http.StatusOK)
	buf, err := json.Marshal(user)
	if err != nil {
		log.Error("Failed to marshal user", zap.Error(err))
		return
	}
	_, _ = wr.Write(buf)
}

// GitHubUser contains minimal identity claims.
type GitHubUser struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
}

// FetchUser looks up minimal identity claims for a given GitHub OAuth 2.0 token.
func (g *GitHubIdP) FetchUser(ctx context.Context, authToken string) (*GitHubUser, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.github.com/user", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "token "+authToken)
	res, err := g.HTTP.Do(req)
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
	u := new(GitHubUser)
	if err := json.Unmarshal(body, u); err != nil {
		return nil, err
	}
	return u, nil
}
