package kafka

import (
	"context"
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"gpu-telemetry-pipeline/internal/queue"
)

// ProducerConfig holds settings for publishing to a topic.
type ProducerConfig struct {
	Brokers []string
	Topic   string
	// Zero applies a 50 ms default.
	BatchTimeout time.Duration
}

// Producer is a queue.Producer backed by a kafka-go Writer.
type Producer struct {
	writer *kafkago.Writer
}

// NewProducer creates a producer that routes by key (hash balancer), preserving
// per-GPU ordering, with RequireAll acks for durable writes.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: at least one broker is required")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka: topic is required")
	}

	batchTimeout := cfg.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = 50 * time.Millisecond
	}

	w := &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.Brokers...),
		Topic:                  cfg.Topic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireAll,
		BatchTimeout:           batchTimeout,
		AllowAutoTopicCreation: true, // streamer may start before topic exists
	}

	return &Producer{writer: w}, nil
}

// Publish blocks until all messages are acknowledged.
func (p *Producer) Publish(ctx context.Context, msgs ...queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	native := make([]kafkago.Message, 0, len(msgs))
	for _, m := range msgs {
		native = append(native, kafkago.Message{Key: m.Key, Value: m.Value})
	}
	return p.writer.WriteMessages(ctx, native...)
}

// Close flushes buffered messages and closes the underlying writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
