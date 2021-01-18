package providers

import (
	"context"
	"fmt"
	"net"
	"os"

	"go.uber.org/fx"
	"go.uber.org/zap"
)

// ListenUnix is a wrapper over unix socket listeners with proper cleanup.
func ListenUnix(path string) (net.Listener, error) {
	stat, statErr := os.Stat(path)
	if os.IsNotExist(statErr) {
		return net.Listen("unix", path)
	} else if statErr != nil {
		return nil, statErr
	}
	// Socket still exists, clean up.
	if stat.Mode()|os.ModeSocket == 0 {
		return nil, fmt.Errorf("existing file is not a socket: %s", path)
	}
	if err := os.Remove(path); err != nil {
		return nil, fmt.Errorf("failed to remove socket: %w", err)
	}
	return net.Listen("unix", path)
}

// Listen is a wrapper over net.Listen with better unix socket support.
func Listen(network, address string) (net.Listener, error) {
	switch network {
	case "unix":
		return ListenUnix(address)
	default:
		return net.Listen(network, address)
	}
}

// MustListen wraps Listen, and calls log.Fatal() if listening fails.
func MustListen(log *zap.Logger, network, address string) net.Listener {
	opts := []zap.Field{
		zap.String("listen.net", network),
		zap.String("listen.addr", address),
	}
	log.Info("Starting server", opts...)
	sock, err := Listen(network, address)
	if err != nil {
		opts = append(opts, zap.Error(err))
		log.Fatal("Listener failed", opts...)
		return nil
	}
	return sock
}

// LifecycleServe registers a server on a listener on the provided fx.Lifecycle.
func LifecycleServe(log *zap.Logger, lc fx.Lifecycle, sock net.Listener, server Server) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go func() {
				if err := server.Serve(sock); err != nil {
					log.Fatal("Server failed", zap.Error(err))
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			server.Stop()
			return nil
		},
	})
}

// Server abstracts gRPC and HTTP servers.
type Server interface {
	Serve(sock net.Listener) error
	Stop()
}
