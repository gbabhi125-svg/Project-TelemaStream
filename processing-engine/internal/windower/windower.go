// Package windower maintains a per-sensor sliding window in memory and
// evaluates it on a fixed slide interval. This is the same computation a
// Flink `SLIDING(5 MIN, SLIDE 10 SEC)` job performs; it is implemented here
// directly on the consumer goroutine to keep the stack JVM-free while
// preserving the architecture's semantics, and is a drop-in replacement
// target for an actual Flink/Spark job at production scale.
package windower

import (
	"sync"
	"time"
)

type Sample struct {
	Value float64
	At    time.Time
}

type Result struct {
	SensorID string
	Avg      float64
	Min      float64
	Max      float64
	Count    int
	WindowEnd time.Time
}

type Windower struct {
	mu         sync.Mutex
	windowSize time.Duration
	samples    map[string][]Sample
}

func New(windowSize time.Duration) *Windower {
	return &Windower{
		windowSize: windowSize,
		samples:    make(map[string][]Sample),
	}
}

func (w *Windower) Add(sensorID string, value float64, at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.samples[sensorID] = append(w.samples[sensorID], Sample{Value: value, At: at})
}

// Evaluate prunes samples older than the window size and returns one
// aggregate per sensor that currently has data. Call this on every slide
// tick (e.g. every 10s).
func (w *Windower) Evaluate(now time.Time) []Result {
	w.mu.Lock()
	defer w.mu.Unlock()

	cutoff := now.Add(-w.windowSize)
	results := make([]Result, 0, len(w.samples))

	for sensorID, samples := range w.samples {
		kept := samples[:0:0]
		for _, s := range samples {
			if s.At.After(cutoff) {
				kept = append(kept, s)
			}
		}
		w.samples[sensorID] = kept

		if len(kept) == 0 {
			delete(w.samples, sensorID)
			continue
		}

		sum, min, max := 0.0, kept[0].Value, kept[0].Value
		for _, s := range kept {
			sum += s.Value
			if s.Value < min {
				min = s.Value
			}
			if s.Value > max {
				max = s.Value
			}
		}

		results = append(results, Result{
			SensorID:  sensorID,
			Avg:       sum / float64(len(kept)),
			Min:       min,
			Max:       max,
			Count:     len(kept),
			WindowEnd: now,
		})
	}

	return results
}
