// Command processing-engine is the stream-analytics stage of the pipeline:
// it consumes raw readings from Kafka, maintains sliding-window aggregates,
// raises threshold alerts, and routes data down both branches of the
// Lambda architecture (hot -> TimescaleDB, cold -> MinIO/Parquet-style).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/telemastream/processing-engine/internal/config"
	"github.com/telemastream/processing-engine/internal/model"
	"github.com/telemastream/processing-engine/internal/storage"
	"github.com/telemastream/processing-engine/internal/windower"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	ts, err := storage.NewTimescale(ctx, cfg.TimescaleDSN)
	if err != nil {
		log.Fatalf("timescale connect: %v", err)
	}
	defer ts.Close()

	cold, err := storage.NewColdStore(cfg.MinioEndpoint, cfg.MinioAccessKey, cfg.MinioSecretKey, cfg.MinioBucket)
	if err != nil {
		log.Fatalf("minio connect: %v", err)
	}
	if err := cold.EnsureBucket(ctx); err != nil {
		log.Fatalf("minio ensure bucket: %v", err)
	}

	alertsWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    cfg.AlertsTopic,
		Balancer: &kafka.LeastBytes{},
	}
	defer alertsWriter.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		GroupID: cfg.ConsumerGroup,
		Topic:   cfg.ReadingsTopic,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	win := windower.New(time.Duration(cfg.WindowSizeSeconds) * time.Second)

	var mu sync.Mutex
	hotBuffer := make([]model.Reading, 0, 1000)
	coldBuffer := make([]model.Reading, 0, 1000)

	flushHot := func() {
		mu.Lock()
		batch := hotBuffer
		hotBuffer = make([]model.Reading, 0, 1000)
		mu.Unlock()
		if err := ts.InsertReadingsBatch(ctx, batch); err != nil {
			log.Printf("timescale insert error: %v", err)
		}
	}

	flushCold := func() {
		mu.Lock()
		batch := coldBuffer
		coldBuffer = make([]model.Reading, 0, 1000)
		mu.Unlock()
		if err := cold.WriteBatch(ctx, batch); err != nil {
			log.Printf("minio write error: %v", err)
		}
	}

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			flushHot()
		}
	}()

	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			flushCold()
		}
	}()

	go func() {
		ticker := time.NewTicker(time.Duration(cfg.WindowSlideSeconds) * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			results := win.Evaluate(time.Now())
			if err := ts.InsertWindows(ctx, results); err != nil {
				log.Printf("timescale window insert error: %v", err)
			}
			for _, r := range results {
				evaluateAlert(ctx, alertsWriter, ts, r, cfg)
			}
		}
	}()

	log.Printf("processing-engine ready: window=%ds slide=%ds moisture_threshold=%.1f",
		cfg.WindowSizeSeconds, cfg.WindowSlideSeconds, cfg.MoistureAlertThreshold)

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Printf("kafka read error: %v", err)
			continue
		}

		var r model.Reading
		if err := json.Unmarshal(msg.Value, &r); err != nil {
			continue
		}

		mu.Lock()
		hotBuffer = append(hotBuffer, r)
		coldBuffer = append(coldBuffer, r)
		mu.Unlock()

		if r.IsValid() {
			win.Add(r.SensorID, r.Value, time.UnixMilli(r.Timestamp))
		}
	}
}

func evaluateAlert(ctx context.Context, writer *kafka.Writer, ts *storage.Timescale, r windower.Result, cfg config.Config) {
	if !strings.HasPrefix(r.SensorID, "soil_moisture") {
		return
	}
	if r.Avg >= cfg.MoistureAlertThreshold {
		return
	}
	message := map[string]interface{}{
		"sensor_id": r.SensorID,
		"avg":       r.Avg,
		"threshold": cfg.MoistureAlertThreshold,
		"window_end": r.WindowEnd,
	}
	payload, _ := json.Marshal(message)

	if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(r.SensorID), Value: payload}); err != nil {
		log.Printf("alert publish error: %v", err)
	}

	msg := formatAlertMessage(r, cfg.MoistureAlertThreshold)
	if err := ts.InsertAlert(ctx, r.SensorID, "low_moisture", msg, "warning"); err != nil {
		log.Printf("alert insert error: %v", err)
	}
}

func formatAlertMessage(r windower.Result, threshold float64) string {
	return fmt.Sprintf("sensor %s averaged %.1f over the last window, below threshold %.1f", r.SensorID, r.Avg, threshold)
}
