// Package queue defines the messaging abstraction that decouples the Collector
// from any concrete message-queue technology.
//
// The project brief's end goal is a *custom* message queue (no Kafka/RabbitMQ).
// To get there without rewriting the Collector, everything above this package
// programs against the Consumer interface; Kafka is merely the first
// implementation (see queue/kafka). Swapping in the custom queue later means
// adding one more implementation of Consumer, not touching collector logic.
//
// The interface is deliberately small and models the "competing consumers"
// pattern: many Collector instances share a group, the queue distributes
// partitions/messages among them, and processed messages are acknowledged
// explicitly so delivery is at-least-once.
package queue

import "context"

// Message is a single payload pulled from the queue. Value is the encoded
// telemetry.Record (JSON). raw carries implementation-specific bookkeeping
// (e.g. the Kafka partition/offset) needed to acknowledge the message; callers
// must treat it as opaque and pass the Message back to Commit unmodified.
type Message struct {
	Key   []byte
	Value []byte

	raw any
}

// NewMessage builds a Message with an opaque implementation handle. Queue
// implementations use this; callers do not.
func NewMessage(key, value []byte, raw any) Message {
	return Message{Key: key, Value: value, raw: raw}
}

// Raw returns the opaque implementation handle attached to the message.
func (m Message) Raw() any { return m.raw }

// Consumer is the read side of the queue, scoped to a single consumer-group
// member. It is the only queue surface the Collector depends on.
//
// Implementations must be safe for sequential use by one Collector goroutine
// (fetch -> process -> commit). Fetch blocks until a message is available or
// ctx is cancelled, supporting the long-poll loop at the heart of the Collector.
type Consumer interface {
	// Fetch returns the next message for this consumer, blocking until one is
	// available or ctx is done. It does not advance the committed offset; the
	// caller must Commit after the message is durably persisted.
	Fetch(ctx context.Context) (Message, error)

	// Commit acknowledges that the given messages have been processed, advancing
	// the committed offset so they are not redelivered to the group. Committing
	// the highest offset in a batch is sufficient for ordered partitions.
	Commit(ctx context.Context, msgs ...Message) error

	// Close releases the consumer's resources and triggers a group rebalance so
	// surviving members pick up this member's partitions. Safe to call once.
	Close() error
}
