// Package appctx provides a context that can be cancelled using interrupts.
package appctx

import (
	"context"
	"os"
	"os/signal"
	"sync"
)

var once sync.Once
var ctx context.Context

// Context returns the application context that closes when the app gets an interrupt.
// It is safe to call this function multiple times, it will return the same context object.
func Context() context.Context {
	once.Do(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.Background())
		go func() {
			defer cancel()
			c := make(chan os.Signal, 1)
			signal.Notify(c, os.Interrupt)
			<-c
		}()
	})
	return ctx
}
