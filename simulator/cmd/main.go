// Command simulator is the synthetic telemetry generator described in the
// project brief: a multi-goroutine process that impersonates thousands of
// independent IoT devices (soil moisture probes, fleet trackers) to load
// test the ingestion pipeline end to end, including its failure modes.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

type reading struct {
	SensorID   string  `json:"sensor_id"`
	SensorType string  `json:"sensor_type"`
	Value      float64 `json:"value"`
	Unit       string  `json:"unit"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	Timestamp  int64   `json:"ts"`
}

var sensorTypes = []struct {
	name string
	unit string
	min  float64
	max  float64
}{
	{"soil_moisture", "%", 15, 60},
	{"soil_temperature", "C", 10, 35},
	{"fleet_speed", "km/h", 0, 120},
	{"fleet_fuel_level", "%", 5, 100},
}

type connPool struct {
	mu    sync.Mutex
	conns []net.Conn
	addr  string
}

func newConnPool(addr string, size int) *connPool {
	p := &connPool{addr: addr}
	for i := 0; i < size; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			log.Printf("simulator: dial %s failed, retrying: %v", addr, err)
			time.Sleep(time.Second)
			i--
			continue
		}
		p.conns = append(p.conns, conn)
	}
	return p
}

func (p *connPool) get(i int) net.Conn {
	return p.conns[i%len(p.conns)]
}

func main() {
	addr := getEnv("INGEST_TCP_ADDR", "localhost:9500")
	sensorCount := getEnvInt("VIRTUAL_SENSORS", 2000)
	emitIntervalMs := getEnvInt("EMIT_INTERVAL_MS", 500)
	spikeMode := getEnv("SPIKE_MODE", "false") == "true"
	faultRate := getEnvFloat("FAULT_INJECTION_RATE", 0.01)

	poolSize := sensorCount
	if poolSize > 200 {
		poolSize = 200
	}

	log.Printf("simulator: connecting %d sockets to %s for %d virtual sensors", poolSize, addr, sensorCount)
	pool := newConnPool(addr, poolSize)

	var wg sync.WaitGroup
	for i := 0; i < sensorCount; i++ {
		wg.Add(1)
		go runSensor(i, pool, emitIntervalMs, spikeMode, faultRate, &wg)
	}

	log.Printf("simulator: %d virtual sensors emitting every ~%dms (spike_mode=%v fault_rate=%.3f)",
		sensorCount, emitIntervalMs, spikeMode, faultRate)

	wg.Wait()
}

func runSensor(index int, pool *connPool, intervalMs int, spikeMode bool, faultRate float64, wg *sync.WaitGroup) {
	defer wg.Done()

	t := sensorTypes[index%len(sensorTypes)]
	sensorID := fmt.Sprintf("%s-%04d", t.name, index)
	conn := pool.get(index)
	writer := bufio.NewWriter(conn)

	jitter := time.Duration(rand.Intn(intervalMs)) * time.Millisecond
	time.Sleep(jitter)

	baseInterval := time.Duration(intervalMs) * time.Millisecond

	for {
		interval := baseInterval
		if spikeMode && rand.Float64() < 0.3 {
			// Simulate a burst: a batch of devices firing far faster than
			// normal, which is exactly the load pattern backpressure is
			// meant to absorb without dropping the service.
			interval = baseInterval / 10
		}

		value := t.min + rand.Float64()*(t.max-t.min)
		valid := true
		if rand.Float64() < faultRate {
			// Inject a physically impossible reading (negative moisture,
			// negative speed) to exercise the downstream validation layer.
			value = -value
			valid = false
		}
		_ = valid

		r := reading{
			SensorID:   sensorID,
			SensorType: t.name,
			Value:      value,
			Unit:       t.unit,
			Lat:        12.9 + rand.Float64()*0.5,
			Lng:        77.5 + rand.Float64()*0.5,
			Timestamp:  time.Now().UnixMilli(),
		}

		payload, err := json.Marshal(r)
		if err == nil {
			writer.Write(payload)
			writer.WriteByte('\n')
			writer.Flush()
		}

		time.Sleep(interval)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}
