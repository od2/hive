// Package redistest contains utilities for unit tests with Redis.
package redistest

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"go.od2.network/hive/pkg/exectest"
)

// Redis is a Redis server and client for use in end-to-end unit tests.
type Redis struct {
	Cmd    *exec.Cmd
	Client *redis.Client

	bg      *exectest.Background
	tempDir string
}

// NewRedis starts an ephemeral Redis server and returns a client.
func NewRedis(ctx context.Context, t testing.TB) *Redis {
	// Run Redis server as subprocess.
	dir, err := ioutil.TempDir("", "redistest-")
	if err != nil {
		panic("failed to get temp dir: " + err.Error())
	}
	socket := filepath.Join(dir, "redis.sock")
	redisCmd := exec.CommandContext(ctx, "redis-server",
		"--port", "0",
		"--unixsocket", socket,
		"--unixsocketperm", "700",
		"--loglevel", "verbose")
	redisCmd.Dir = dir
	bg := exectest.NewBackground(t, redisCmd)
	bg.Name = "redis"
	bg.LogStdout = true
	bg.LogStderr = true
	bg.Start()
	// Create Redis client.
	client := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    socket,
	})
	// Give Redis a second to start up.
	startupTicker := time.NewTicker(100 * time.Millisecond)
	defer startupTicker.Stop()
	var pingErr error
tryLoop:
	for try := 0; try < 30; try++ {
		if try > 0 {
			select {
			case <-startupTicker.C:
				break
			case <-bg.Done():
				break tryLoop
			}
		}
		pingErr = client.Ping(ctx).Err()
		if errors.Is(pingErr, redis.ErrClosed) {
			continue // Redis still not up
		} else if errors.Is(pingErr, os.ErrNotExist) {
			continue // Redis hasn't even created the socket yet
		} else if pingErr != nil {
			t.Fatal("Failed to ping Redis:", pingErr.Error())
		}
		t.Log("redistest: Redis is up")
		return &Redis{
			Cmd:    redisCmd,
			Client: client,

			bg:      bg,
			tempDir: dir,
		}
	}
	if err := bg.Err(); err != nil {
		t.Fatal("Subprocess failed:", err)
	}
	t.Fatal("Failed to ping Redis:", pingErr)
	return nil
}

// Close shuts down the server and client and prints the log.
func (r *Redis) Close(t testing.TB) {
	t.Log("redistest: Removing", r.tempDir)
	r.bg.Close()
	_ = os.RemoveAll(r.tempDir)
}
