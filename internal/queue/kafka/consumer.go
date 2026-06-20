// Package kafka provides the Kafka-backed implementation of queue.Consumer.
//
// It uses segmentio/kafka-go, a pure-Go client, deliberately: the service image
// is built with CGO_ENABLED=0 onto a distroless-static base, so a librdkafka
// (CGO) client such as confluent-kafka-go cannot be linked. The pure-Go client
// also keeps the runtime image free of native dependencies.
//
// Dynamic scaling is delegated to Kafka consumer groups: every Collector
// instance constructs a Consumer with the same GroupID, and Kafka distributes
// the topic's partitions across the live members, rebalancing automatically as
// instances are added or removed.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"gpu-telemetry-pipeline/internal/queue"
)

// Config holds the settings needed to join a topic as a group member.
type Config struct {
	Brokers []string
	Topic   string
	GroupID string

	// MinBytes/MaxBytes bound each fetch request; MaxWait caps how long a fetch
	// blocks waiting to fill MinBytes. Sensible defaults are applied when zero.
	MinBytes int
	MaxBytes int
	MaxWait  time.Duration
}

// Consumer is a queue.Consumer backed by a kafka-go group Reader.
type Consumer struct {
	reader *kafkago.Reader
}

// New constructs a group consumer for the given topic. Offsets are committed
// explicitly (CommitMessages), never automatically, so the Collector controls
// at-least-once semantics: an offset advances only after the record is durably
// stored.
func New(cfg Config) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: at least one broker is required")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka: topic is required")
	}
	if cfg.GroupID == "" {
		return nil, errors.New("kafka: group id is required")
	}

	minBytes := cfg.MinBytes
	if minBytes <= 0 {
		minBytes = 1 // deliver as soon as anything is available (low latency)
	}
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 10 << 20 // 10 MiB
	}
	maxWait := cfg.MaxWait
	if maxWait <= 0 {
		maxWait = 500 * time.Millisecond
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: minBytes,
		MaxBytes: maxBytes,
		MaxWait:  maxWait,
		// Start a brand-new group at the earliest offset so no telemetry that
		// was already streamed is skipped.
		StartOffset: kafkago.FirstOffset,
	})

	return &Consumer{reader: reader}, nil
}

// Fetch returns the next message for this group member without committing it.
func (c *Consumer) Fetch(ctx context.Context) (queue.Message, error) {
	m, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return queue.Message{}, err
	}
	// Stash the kafka message so Commit can map back to its partition/offset.
	return queue.NewMessage(m.Key, m.Value, m), nil
}

// Commit advances the committed offset past the given messages.
func (c *Consumer) Commit(ctx context.Context, msgs ...queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	native := make([]kafkago.Message, 0, len(msgs))
	for _, m := range msgs {
		km, ok := m.Raw().(kafkago.Message)
		if !ok {
			return fmt.Errorf("kafka: message is not committable (raw type %T)", m.Raw())
		}
		native = append(native, km)
	}
	return c.reader.CommitMessages(ctx, native...)
}

// Close leaves the group and releases the underlying reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
