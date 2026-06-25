package server

import (
	"testing"
)

func msgs(n int) []Message {
	out := make([]Message, n)
	for i := range out {
		out[i] = Message{Value: []byte{byte(i)}}
	}
	return out
}

func noWait() bool { return false }

func TestBoundedBuffer_EvictsOldest(t *testing.T) {
	topic := newTopic(8)
	topic.Produce(msgs(8)) // fill exactly

	// produce 4 more: evicts oldest 2 (8/4), baseOffset advances to 2
	topic.Produce(msgs(4))

	if topic.baseOffset != 2 {
		t.Fatalf("baseOffset = %d, want 2", topic.baseOffset)
	}
	if len(topic.messages) != 10 {
		t.Fatalf("len(messages) = %d, want 10", len(topic.messages))
	}
}

func TestBoundedBuffer_ConsumerReset(t *testing.T) {
	topic := newTopic(8)
	topic.Produce(msgs(12)) // triggers eviction, baseOffset = 3

	// A consumer whose offset is behind the eviction window should be reset.
	topic.deliveredOffsets["g"] = 1

	msg, off, err := topic.Consume("g", noWait)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if off != topic.baseOffset {
		t.Fatalf("offset after reset = %d, want %d (baseOffset)", off, topic.baseOffset)
	}
	_ = msg
}

func TestBoundedBuffer_OffsetContinuity(t *testing.T) {
	topic := newTopic(4)

	// Produce 4 messages: offsets 0-3
	topic.Produce(msgs(4))
	// Produce 2 more: evicts oldest 1 (4/4=1), baseOffset=1, offsets 4-5
	topic.Produce(msgs(2))

	// Consumer starts from offset 2 (still valid)
	topic.deliveredOffsets["g"] = 2

	_, off, err := topic.Consume("g", noWait)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if off != 2 {
		t.Fatalf("got offset %d, want 2", off)
	}
}

func TestStats(t *testing.T) {
	b := NewBroker(100)
	b.GetTopic("test").Produce(msgs(5))

	s := b.Stats()
	ts, ok := s.Topics["test"]
	if !ok {
		t.Fatal("topic 'test' missing from stats")
	}
	if ts.Buffered != 5 {
		t.Fatalf("Buffered = %d, want 5", ts.Buffered)
	}
	if ts.HeadOffset != 5 {
		t.Fatalf("HeadOffset = %d, want 5", ts.HeadOffset)
	}
}
