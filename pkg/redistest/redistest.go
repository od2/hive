// Package redistest contains utilities for unit tests with Redis.
package redistest

import (
	"context"
	"errors"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

// Redis is a Redis server and client for use in end-to-end unit tests.
type Redis struct {
	Cmd    *exec.Cmd
	Client *redis.Client

	tempDir string
	cancel  context.CancelFunc
	wg      *sync.WaitGroup
}

// NewRedis starts an ephemeral Redis server and returns a client.
func NewRedis(ctx context.Context, t *testing.T) *Redis {
	dir, err := ioutil.TempDir("", "redis-")
	if err != nil {
		panic("failed to get temp dir: " + err.Error())
	}
	socket := filepath.Join(dir, "redis.sock")

	ctx, cancel := context.WithCancel(ctx)
	redisCmd := exec.CommandContext(ctx, "redis-server",
		"--port", "0",
		"--unixsocket", socket,
		"--unixsocketperm", "700",
		"--loglevel", "verbose")
	redisCmd.Dir = dir
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()
		output, err := redisCmd.CombinedOutput()
		t.Log("Redis exited: ", err)
		t.Log(string(output))
	}()

	t.Log("redistest: Started Redis")

	client := redis.NewClient(&redis.Options{
		Network: "unix",
		Addr:    socket,
	})
	// Give Redis a second to start up.
	startupTicker := time.NewTicker(100 * time.Millisecond)
	defer startupTicker.Stop()
	for try := 0; try < 10; try++ {
		<-startupTicker.C
		pingErr := client.Ping(ctx).Err()
		if errors.Is(pingErr, redis.ErrClosed) {
			continue // Redis still not up
		} else if errors.Is(pingErr, os.ErrNotExist) {
			continue // Redis hasn't even created the socket yet
		} else if pingErr != nil {
			t.Fatal("Failed to start Redis: ", pingErr.Error())
		}
		t.Log("redistest: Redis is up")
		return &Redis{
			Cmd:    redisCmd,
			Client: client,

			tempDir: dir,
			cancel:  cancel,
			wg:      wg,
		}
	}
	t.Error("Failed to start Redis")
	t.FailNow()
	return nil
}

// Close shuts down the server and client and prints the log.
func (r *Redis) Close() {
	r.cancel()
	os.RemoveAll(r.tempDir)
	r.wg.Wait()
}
