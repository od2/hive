package ratelimit

import (
	"sync/atomic"
	"time"
)

// RateLimit is a best-effort, lock-free, CloudFlare-style rate-limiter.
//
// Algorithm: https://blog.cloudflare.com/counting-things-a-lot-of-different-things/
type RateLimit struct {
	Target float32
	Window uint  // Window size in seconds
	Epoch  int64 // Window offset
	W0, W1 int64 // Windows
}

// NewRateLimit creates a new rate limiter.
func NewRateLimit(target float32, window uint) *RateLimit {
	return &RateLimit{
		Target: target,
		Window: window,
	}
}

// Count registers requests and returns the time to sleep to match the rate limit.
// This function is safe to access from multiple goroutines at the same time.
func (r *RateLimit) Count(unix int64, x int64) time.Duration {
	// Fetch and update the rate limiting windows.
	epoch := unix / int64(r.Window)
	fastPath := true
	var w0, w1 int64
	for {
		// Shift rate-limiting windows.
		savedEpoch := atomic.LoadInt64(&r.Epoch)
		if savedEpoch >= epoch {
			break // fast path
		}
		fastPath = false
		if !atomic.CompareAndSwapInt64(&r.Epoch, savedEpoch, epoch) {
			continue
		}
		if savedEpoch+1 == epoch {
			w1 = x
			w0 = atomic.SwapInt64(&r.W1, w1)
			atomic.StoreInt64(&r.W0, w0)
		} else {
			atomic.StoreInt64(&r.W0, 0)
			atomic.StoreInt64(&r.W1, 0)
		}
	}
	if fastPath {
		w1 = atomic.AddInt64(&r.W1, x)
		w0 = atomic.LoadInt64(&r.W0)
	}
	// Calculate the estimated rate-limit usage.
	offset := 1.0 - float32(unix%int64(r.Window))/float32(r.Window)
	usage := offset*float32(w0) + float32(w1)
	rate := usage / float32(r.Window)
	if rate <= r.Target {
		return 0
	}
	ban := float32(r.Window) * (rate - r.Target)
	return time.Duration(ban * float32(time.Second))
}
