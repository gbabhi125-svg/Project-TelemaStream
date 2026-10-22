// Package buffer implements the in-memory shock absorber between the socket
// handlers and the Kafka producer, plus the signals used to drive backpressure.
package buffer

import (
	"errors"
	"sync/atomic"
)

var ErrBufferFull = errors.New("ingestion buffer at capacity")

// RingBuffer is a bounded, concurrency-safe queue of raw payloads awaiting
// a Kafka flush. It is backed by a channel so producers and the single
// drain loop never contend on a lock, and it tracks depth separately so
// Utilization() is cheap to poll from every request path.
type RingBuffer struct {
	ch    chan []byte
	depth int64
	cap   int64
}

func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		ch:  make(chan []byte, capacity),
		cap: int64(capacity),
	}
}

// TryPush attempts a non-blocking enqueue. Callers use this on the hot path
// so a saturated buffer never stalls a goroutine waiting on channel space.
func (r *RingBuffer) TryPush(payload []byte) error {
	select {
	case r.ch <- payload:
		atomic.AddInt64(&r.depth, 1)
		return nil
	default:
		return ErrBufferFull
	}
}

func (r *RingBuffer) Pop() ([]byte, bool) {
	payload, ok := <-r.ch
	if ok {
		atomic.AddInt64(&r.depth, -1)
	}
	return payload, ok
}

func (r *RingBuffer) Depth() int64 {
	return atomic.LoadInt64(&r.depth)
}

func (r *RingBuffer) Capacity() int64 {
	return r.cap
}

// Utilization is the fraction of capacity currently occupied. Handlers
// compare this against the configured high watermark to decide whether to
// start shedding or delaying load.
func (r *RingBuffer) Utilization() float64 {
	return float64(atomic.LoadInt64(&r.depth)) / float64(r.cap)
}
