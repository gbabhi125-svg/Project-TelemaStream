package main

import (
	"context"
	"log"

	"github.com/telemastream/ingestion-service/internal/buffer"
	"github.com/telemastream/ingestion-service/internal/config"
	"github.com/telemastream/ingestion-service/internal/handler"
	"github.com/telemastream/ingestion-service/internal/queue"
)

func main() {
	cfg := config.Load()

	buf := buffer.NewRingBuffer(cfg.BufferCapacity)
	limiter := buffer.NewTokenBucket(cfg.RateLimitPerSec)
	producer := queue.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer producer.Close()

	srv := handler.NewServer(buf, limiter, producer, cfg.BufferHighWatermark)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go srv.RunDrainLoop(ctx)
	go func() {
		if err := srv.ServeTCP(cfg.TCPListenAddr); err != nil {
			log.Fatalf("tcp server: %v", err)
		}
	}()

	log.Printf("ingestion-service ready: buffer_capacity=%d high_watermark=%.2f rate_limit=%d/s",
		cfg.BufferCapacity, cfg.BufferHighWatermark, cfg.RateLimitPerSec)

	if err := srv.ServeHTTP(cfg.HTTPListenAddr); err != nil {
		log.Fatalf("http server: %v", err)
	}
}
