package config

import (
	"os"
	"strconv"
)

type Config struct {
	KafkaBrokers           []string
	ReadingsTopic          string
	AlertsTopic            string
	ConsumerGroup          string
	TimescaleDSN           string
	MinioEndpoint          string
	MinioAccessKey         string
	MinioSecretKey         string
	MinioBucket            string
	WindowSizeSeconds      int
	WindowSlideSeconds     int
	MoistureAlertThreshold float64
}

func Load() Config {
	return Config{
		KafkaBrokers:           []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		ReadingsTopic:          getEnv("READINGS_TOPIC", "sensor.readings"),
		AlertsTopic:            getEnv("ALERTS_TOPIC", "sensor.alerts"),
		ConsumerGroup:          getEnv("CONSUMER_GROUP", "processing-engine"),
		TimescaleDSN:           getEnv("TIMESCALE_DSN", ""),
		MinioEndpoint:          getEnv("MINIO_ENDPOINT", "localhost:9000"),
		MinioAccessKey:         getEnv("MINIO_ACCESS_KEY", "telemastream"),
		MinioSecretKey:         getEnv("MINIO_SECRET_KEY", "telemastream123"),
		MinioBucket:            getEnv("MINIO_BUCKET", "telemastream-cold"),
		WindowSizeSeconds:      getEnvInt("WINDOW_SIZE_SECONDS", 300),
		WindowSlideSeconds:     getEnvInt("WINDOW_SLIDE_SECONDS", 10),
		MoistureAlertThreshold: getEnvFloat("MOISTURE_ALERT_THRESHOLD", 20),
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
