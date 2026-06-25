package server

import (
	"context"
	"runtime"
	"testing"
	"time"
)

func consumeOne(t *Topic, groupID string) (BatchedMsg, error) {
	batch, err := t.ConsumeBatch(groupID, 1, context.Background())
	if err != nil {
		return BatchedMsg{}, err
	}
	return batch[0], nil
}

func msgs(n int) []Message {
	out := make([]Message, n)
	for i := range out {
		out[i] = Message{Value: []byte{byte(i)}}
	}
	return out
}

// Ring eviction: each write past capacity evicts exactly one oldest slot (O(1)).
// With cap=8, producing 12 messages evicts 4 (one per overflow write).
func TestRing_EvictsOneAtATime(t *testing.T) {
	topic := newTopic(8)
	topic.Produce(msgs(8)) // fill: next=8, base=0

	topic.Produce(msgs(4)) // 4 more: each evicts one → base advances to 4
	// next=12, base=4, buffered=8

	if topic.base != 4 {
		t.Fatalf("base = %d, want 4", topic.base)
	}
	if topic.next != 12 {
		t.Fatalf("next = %d, want 12", topic.next)
	}
	if int(topic.next-topic.base) != 8 {
		t.Fatalf("buffered = %d, want 8 (ring stays at capacity)", topic.next-topic.base)
	}
}

// A consumer whose delivered offset has fallen behind the eviction window is
// silently reset to base (not an error).
func TestRing_ConsumerReset(t *testing.T) {
	topic := newTopic(8)
	topic.Produce(msgs(12)) // base=4, next=12

	topic.deliveredOffsets["g"] = 1 // stale: behind base=4

	got, err := consumeOne(topic, "g")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Offset != topic.base {
		t.Fatalf("offset after reset = %d, want %d (base)", got.Offset, topic.base)
	}
}

// Offsets are strictly monotonic across separate Produce calls.
func TestRing_OffsetContinuity(t *testing.T) {
	topic := newTopic(100)

	topic.Produce(msgs(5)) // offsets 0-4
	topic.Produce(msgs(3)) // offsets 5-7

	// Consumer should read all 8 in order.
	batch, err := topic.ConsumeBatch("g", 100, context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch) != 8 {
		t.Fatalf("expected 8 messages, got %d", len(batch))
	}
	for i, m := range batch {
		if m.Offset != int64(i) {
			t.Fatalf("batch[%d].Offset = %d, want %d", i, m.Offset, i)
		}
	}
}

// A consumer at a still-valid offset (not evicted) reads from where it left off.
func TestRing_ValidOffsetPreserved(t *testing.T) {
	topic := newTopic(4)
	topic.Produce(msgs(4)) // next=4, base=0
	topic.Produce(msgs(2)) // next=6, base=2 (offsets 2-5 buffered)

	// A consumer that was at offset 2 should continue from there.
	topic.deliveredOffsets["g"] = 2

	got, err := consumeOne(topic, "g")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Offset != 2 {
		t.Fatalf("got offset %d, want 2", got.Offset)
	}
}

// ConsumeBatch blocks until Produce wakes it, then returns immediately.
func TestConsumeBatch_BlocksUntilProduce(t *testing.T) {
	topic := newTopic(100)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan BatchedMsg, 1)
	go func() {
		m, _ := consumeOne(topic, "g")
		done <- m
	}()

	runtime.Gosched() // let the consumer goroutine reach the select
	topic.Produce([]Message{{Value: []byte("hello")}})

	select {
	case got := <-done:
		if string(got.Msg.Value) != "hello" {
			t.Fatalf("unexpected value %q", got.Msg.Value)
		}
	case <-ctx.Done():
		t.Fatal("consumer did not unblock after produce")
	}
}

// Cancelling the context unblocks a sleeping consumer.
func TestConsumeBatch_CtxCancel(t *testing.T) {
	topic := newTopic(100)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := topic.ConsumeBatch("g", 10, ctx)
		done <- err
	}()

	runtime.Gosched()
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected non-nil error after cancel")
		}
	case <-time.After(time.Second):
		t.Fatal("consumer did not unblock after ctx cancel")
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
	if ts.Head != 5 {
		t.Fatalf("Head = %d, want 5", ts.Head)
	}
	if ts.Base != 0 {
		t.Fatalf("Base = %d, want 0", ts.Base)
	}
}
