package config

import (
	"os"
	"strconv"
)

type Config struct {
	KafkaBrokers        []string
	KafkaTopic          string
	TCPListenAddr       string
	HTTPListenAddr      string
	BufferCapacity      int
	BufferHighWatermark float64
	RateLimitPerSec     int
}

func Load() Config {
	return Config{
		KafkaBrokers:        []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		KafkaTopic:          getEnv("KAFKA_TOPIC", "sensor.readings"),
		TCPListenAddr:       getEnv("TCP_LISTEN_ADDR", ":9500"),
		HTTPListenAddr:      getEnv("HTTP_LISTEN_ADDR", ":9501"),
		BufferCapacity:      getEnvInt("BUFFER_CAPACITY", 50000),
		BufferHighWatermark: getEnvFloat("BUFFER_HIGH_WATERMARK", 0.85),
		RateLimitPerSec:     getEnvInt("RATE_LIMIT_PER_SEC", 20000),
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
