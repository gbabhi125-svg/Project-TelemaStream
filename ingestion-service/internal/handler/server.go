package handler

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/telemastream/ingestion-service/internal/buffer"
	"github.com/telemastream/ingestion-service/internal/queue"
)

// Server owns the shared backpressure primitives and exposes both the raw
// TCP line protocol and the HTTP ingestion endpoint on top of them. Every
// accepted packet flows: socket -> validate -> RingBuffer -> drain loop ->
// Kafka. The buffer is what decouples "network can accept bytes" from
// "Kafka can accept messages", which is the whole point of the exercise.
type Server struct {
	cfg         Config
	buf         *buffer.RingBuffer
	limiter     *buffer.TokenBucket
	producer    *queue.Producer
	metrics     Metrics
	highWatermark float64
}

type Config struct {
	HighWatermark float64
}

func NewServer(buf *buffer.RingBuffer, limiter *buffer.TokenBucket, producer *queue.Producer, highWatermark float64) *Server {
	return &Server{
		buf:           buf,
		limiter:       limiter,
		producer:      producer,
		highWatermark: highWatermark,
	}
}

// RunDrainLoop is the single consumer that pulls payloads off the ring
// buffer and hands them to the (async, batching) Kafka producer. It is the
// only goroutine that touches the buffer's read side, so no additional
// locking is needed beyond the channel itself.
func (s *Server) RunDrainLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		payload, ok := s.buf.Pop()
		if !ok {
			return
		}
		var r Reading
		if err := json.Unmarshal(payload, &r); err != nil {
			continue
		}
		if err := s.producer.Publish(ctx, []byte(r.SensorID), payload); err != nil {
			log.Printf("publish error: %v", err)
			continue
		}
		atomic.AddInt64(&s.metrics.Published, 1)
	}
}

// acceptReading is the shared backpressure gate for both transports: it
// shapes traffic with the token bucket first (smooths sustained rate),
// then checks the ring buffer's high watermark (sheds load before memory
// actually runs out). Order matters — rate limiting is cheap and catches
// most overload before the more expensive buffer check.
func (s *Server) acceptReading(raw []byte) (accepted bool, reason string) {
	atomic.AddInt64(&s.metrics.Received, 1)

	var r Reading
	if err := json.Unmarshal(raw, &r); err != nil {
		atomic.AddInt64(&s.metrics.Invalid, 1)
		return false, "invalid_json"
	}
	if err := r.Validate(); err != nil {
		atomic.AddInt64(&s.metrics.Invalid, 1)
		return false, err.Error()
	}

	if !s.limiter.Allow() {
		atomic.AddInt64(&s.metrics.RateLimited, 1)
		return false, "rate_limited"
	}

	if s.buf.Utilization() >= s.highWatermark {
		atomic.AddInt64(&s.metrics.Shed, 1)
		return false, "buffer_high_watermark"
	}

	if err := s.buf.TryPush(raw); err != nil {
		atomic.AddInt64(&s.metrics.Shed, 1)
		return false, "buffer_full"
	}

	atomic.AddInt64(&s.metrics.Accepted, 1)
	return true, ""
}

// ServeTCP runs the raw line-protocol listener. On backpressure it does not
// reset the connection — it delays the read loop (simulating a slowed TCP
// ACK) so well-behaved clients naturally back off instead of hot-looping
// retries into a saturated buffer.
func (s *Server) ServeTCP(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("ingestion: TCP listener on %s", addr)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("tcp accept error: %v", err)
			continue
		}
		go s.handleTCPConn(conn)
	}
}

func (s *Server) handleTCPConn(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 64*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		payload := append([]byte(nil), line...)
		accepted, _ := s.acceptReading(payload)
		if !accepted {
			// Backpressure signal: delay the next read instead of an
			// immediate ACK, which throttles a well-behaved sender's
			// effective send rate without dropping the connection.
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// ServeHTTP exposes /ingest for HTTP-based producers, /healthz for
// container healthchecks, and /metrics for the dashboard's infra tile.
// /ingest returns 429 under backpressure, matching the architecture's
// explicit "network-level pressure signal" requirement.
func (s *Server) ServeHTTP(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", s.handleIngest)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/metrics", s.handleMetrics)
	log.Printf("ingestion: HTTP listener on %s", addr)
	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	accepted, reason := s.acceptReading(body)
	if !accepted {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(map[string]string{"error": reason})
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	snap := s.metrics.Snapshot()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"received":          snap.Received,
		"accepted":          snap.Accepted,
		"shed":              snap.Shed,
		"rate_limited":       snap.RateLimited,
		"published":         snap.Published,
		"invalid":           snap.Invalid,
		"buffer_depth":      s.buf.Depth(),
		"buffer_capacity":   s.buf.Capacity(),
		"buffer_utilization": s.buf.Utilization(),
	})
}
