package server

import (
	"sync"
	"time"
)

// tokenBucket is a bytes-per-second rate limiter with a configurable
// burst capacity. Take returns false when the request would exceed the
// available tokens — callers translate that into StatusRateLimited on
// the wire.
//
// Tokens regenerate at `rate` per second based on wall-clock time, so
// idleness restores the bucket fully (up to capacity) without needing
// a background ticker.
type tokenBucket struct {
	mu       sync.Mutex
	capacity int64
	rate     int64 // tokens per second
	tokens   int64
	last     time.Time
	now      func() time.Time // injectable for tests
}

// newTokenBucket starts the bucket full. capacity caps the largest
// burst; rate is the steady-state replenish rate (per second).
func newTokenBucket(rate, capacity int64) *tokenBucket {
	if capacity < rate {
		capacity = rate
	}
	return &tokenBucket{
		capacity: capacity,
		rate:     rate,
		tokens:   capacity,
		last:     time.Now(),
		now:      time.Now,
	}
}

// take attempts to consume n tokens. Returns true on success; false
// without modifying state when the bucket can't satisfy the request.
// A request larger than capacity always fails — callers must size
// burst correctly for their largest expected payload.
func (b *tokenBucket) take(n int64) bool {
	if n <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	elapsed := now.Sub(b.last).Seconds()
	b.tokens = min64(b.capacity, b.tokens+int64(elapsed*float64(b.rate)))
	b.last = now
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
