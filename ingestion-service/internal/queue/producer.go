package queue

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer wraps a kafka-go Writer configured for high-throughput,
// batch-flushed async writes so a single slow broker round trip never
// blocks the drain loop pulling off the ring buffer.
type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    500,
		BatchTimeout: 50 * time.Millisecond,
		Async:        true,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				log.Printf("kafka: batch of %d messages failed: %v", len(messages), err)
			}
		},
	}
	return &Producer{writer: w}
}

func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
