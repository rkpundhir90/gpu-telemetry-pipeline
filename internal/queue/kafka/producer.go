package kafka

import (
	"context"
	"errors"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"gpu-telemetry-pipeline/internal/queue"
)

// ProducerConfig holds the settings needed to publish to a topic.
type ProducerConfig struct {
	Brokers []string
	Topic   string

	// BatchTimeout bounds how long the writer waits to fill a batch before
	// flushing. A small value keeps streaming latency low; zero applies a
	// sensible default.
	BatchTimeout time.Duration
}

// Producer is a queue.Producer backed by a kafka-go Writer.
type Producer struct {
	writer *kafkago.Writer
}

// NewProducer constructs a producer for the given topic. Messages are routed by
// key with a hash balancer, so keying by GPU UUID pins each GPU to one partition
// and preserves per-GPU ordering. RequireAll acks make a successful Publish mean
// the data is durably replicated before the Streamer moves on.
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
		Addr:         kafkago.TCP(cfg.Brokers...),
		Topic:        cfg.Topic,
		Balancer:     &kafkago.Hash{},
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: batchTimeout,
		// The Streamer may start before the collector's topic exists (e.g. in the
		// compose stack); let the first write create it.
		AllowAutoTopicCreation: true,
	}

	return &Producer{writer: w}, nil
}

// Publish writes the messages and blocks until they are acknowledged. kafka-go
// retries transient errors internally; a returned error is terminal for this
// call and the caller should retry.
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
