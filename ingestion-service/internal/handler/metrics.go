package handler

import "sync/atomic"

// Metrics are the counters the dashboard's infra-health tile polls. They
// are plain atomics rather than a Prometheus client to keep the ingestion
// binary's dependency footprint minimal.
type Metrics struct {
	Received   int64
	Accepted   int64
	Shed       int64 // rejected because the buffer was over the high watermark
	RateLimited int64 // rejected because the token bucket was empty
	Published  int64
	Invalid    int64
}

func (m *Metrics) Snapshot() Metrics {
	return Metrics{
		Received:    atomic.LoadInt64(&m.Received),
		Accepted:    atomic.LoadInt64(&m.Accepted),
		Shed:        atomic.LoadInt64(&m.Shed),
		RateLimited: atomic.LoadInt64(&m.RateLimited),
		Published:   atomic.LoadInt64(&m.Published),
		Invalid:     atomic.LoadInt64(&m.Invalid),
	}
}
