package buffer

import (
	"sync"
	"time"
)

// TokenBucket caps sustained throughput independently of buffer depth, so a
// burst that would still fit in the ring buffer can still be smoothed before
// it reaches Kafka.
type TokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	capacity   float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func NewTokenBucket(ratePerSecond int) *TokenBucket {
	return &TokenBucket{
		tokens:     float64(ratePerSecond),
		capacity:   float64(ratePerSecond),
		refillRate: float64(ratePerSecond),
		lastRefill: time.Now(),
	}
}

// Allow reports whether a single unit of work may proceed right now,
// consuming a token if so. It never blocks.
func (t *TokenBucket) Allow() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(t.lastRefill).Seconds()
	t.lastRefill = now

	t.tokens += elapsed * t.refillRate
	if t.tokens > t.capacity {
		t.tokens = t.capacity
	}

	if t.tokens < 1 {
		return false
	}
	t.tokens--
	return true
}
