// Package queue defines the messaging abstraction over the queue technology.
// Both the Collector (Consumer) and the Streamer (Producer) depend only on these
// interfaces; implementations live in queue/kafka and queue/grpc.
package queue

import "context"

// Message is a single payload from the queue. Value is the JSON-encoded
// telemetry.Record. raw carries implementation-specific bookkeeping (e.g. the
// Kafka offset or gRPC offset int64) needed to commit; callers treat it as
// opaque and pass the Message back to Commit unmodified.
type Message struct {
	Key   []byte
	Value []byte

	raw any
}

// NewMessage builds a Message with an opaque implementation handle. Used by
// queue implementations, not callers.
func NewMessage(key, value []byte, raw any) Message {
	return Message{Key: key, Value: value, raw: raw}
}

// Raw returns the opaque implementation handle.
func (m Message) Raw() any { return m.raw }

// Consumer is the read side of the queue for one consumer-group member.
// Fetch blocks until a message is available or ctx is cancelled. Commit must be
// called after the message is durably persisted to advance the group offset.
type Consumer interface {
	Fetch(ctx context.Context) (Message, error)
	Commit(ctx context.Context, msgs ...Message) error
	Close() error
}

// Producer is the write side of the queue. Each message's Key routes to a
// partition; keying by GPU UUID keeps a GPU's datapoints ordered end-to-end.
// Publish blocks until the queue has accepted the messages.
type Producer interface {
	Publish(ctx context.Context, msgs ...Message) error
	Close() error
}
