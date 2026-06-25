package server

import (
	"context"
	"sync"
)

type Message struct {
	Key   []byte
	Value []byte
}

type TopicStats struct {
	Buffered         int
	Base             int64
	Head             int64
	CommittedOffsets map[string]int64
}

// Topic is a bounded append-only in-memory log backed by a fixed-size circular
// ring buffer. Writing past capacity overwrites the oldest slot — O(1) eviction
// with no copy or allocation spike. Lagging consumers are silently reset to the
// new base rather than erroring.
//
// Blocking consumers wait on a notify channel that is closed on every Produce
// and immediately replaced, letting ConsumeBatch select on ctx.Done() natively.
type Topic struct {
	mu               sync.Mutex
	buf              []Message        // circular ring: buf[offset % cap]
	cap              int64            // fixed capacity (len(buf))
	base             int64            // logical offset of the oldest buffered message
	next             int64            // logical offset of the next slot to write
	committedOffsets map[string]int64
	deliveredOffsets map[string]int64
	notify           chan struct{} // closed on each Produce, replaced atomically
}

func newTopic(capacity int) *Topic {
	return &Topic{
		buf:              make([]Message, capacity),
		cap:              int64(capacity),
		committedOffsets: make(map[string]int64),
		deliveredOffsets: make(map[string]int64),
		notify:           make(chan struct{}),
	}
}

// Produce appends msgs to the ring. When the ring is full each new write
// advances base by one, evicting the oldest message in O(1) with no copy.
func (t *Topic) Produce(msgs []Message) []int64 {
	t.mu.Lock()
	offsets := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		t.buf[t.next%t.cap] = m
		offsets = append(offsets, t.next)
		t.next++
		if t.next-t.base > t.cap {
			t.base++ // overwrite oldest slot; advance the eviction boundary
		}
	}
	// Capture and replace notify before unlocking so the new channel is visible
	// to any consumer that wakes before we call close.
	notify := t.notify
	t.notify = make(chan struct{})
	t.mu.Unlock()
	// Close after unlock: woken consumers acquire the mutex immediately.
	close(notify)
	return offsets
}

// ConsumeBatch blocks until at least one message is available or ctx is
// cancelled. Holds the lock only for the read+copy — the select runs outside
// the lock so Produce is never blocked by a sleeping consumer.
func (t *Topic) ConsumeBatch(groupID string, maxBatch int, ctx context.Context) ([]BatchedMsg, error) {
	for {
		t.mu.Lock()

		// Advance a lagging consumer to the oldest still-buffered offset.
		if t.deliveredOffsets[groupID] < t.base {
			t.deliveredOffsets[groupID] = t.base
		}

		if ctx.Err() != nil {
			t.mu.Unlock()
			return nil, ctx.Err()
		}

		if t.deliveredOffsets[groupID] < t.next {
			from := t.deliveredOffsets[groupID]
			count := int(t.next - from)
			if count > maxBatch {
				count = maxBatch
			}
			batch := make([]BatchedMsg, count)
			for i := range batch {
				off := from + int64(i)
				batch[i] = BatchedMsg{Msg: t.buf[off%t.cap], Offset: off}
			}
			t.deliveredOffsets[groupID] += int64(count)
			t.mu.Unlock()
			return batch, nil
		}

		notifyCh := t.notify
		t.mu.Unlock()

		select {
		case <-notifyCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (t *Topic) Commit(groupID string, offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.committedOffsets[groupID] = offset
	return nil
}

func (t *Topic) stats() TopicStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	committed := make(map[string]int64, len(t.committedOffsets))
	for g, o := range t.committedOffsets {
		committed[g] = o
	}
	return TopicStats{
		Buffered:         int(t.next - t.base),
		Base:             t.base,
		Head:             t.next,
		CommittedOffsets: committed,
	}
}

type BatchedMsg struct {
	Msg    Message
	Offset int64
}

type BrokerStats struct {
	Topics      map[string]TopicStats
	MaxMessages int
}

// Broker manages multiple topics, creating them on first access.
// Uses a read-lock fast path so topic lookups after the first call are lock-free
// on the write side.
type Broker struct {
	mu          sync.RWMutex
	topics      map[string]*Topic
	maxMessages int
}

func NewBroker(maxMessages int) *Broker {
	return &Broker{
		topics:      make(map[string]*Topic),
		maxMessages: maxMessages,
	}
}

func (b *Broker) GetTopic(name string) *Topic {
	b.mu.RLock()
	t := b.topics[name]
	b.mu.RUnlock()
	if t != nil {
		return t
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if t = b.topics[name]; t != nil { // another goroutine may have created it
		return t
	}
	t = newTopic(b.maxMessages)
	b.topics[name] = t
	return t
}

func (b *Broker) Stats() BrokerStats {
	b.mu.RLock()
	defer b.mu.RUnlock()
	stats := BrokerStats{
		Topics:      make(map[string]TopicStats, len(b.topics)),
		MaxMessages: b.maxMessages,
	}
	for name, t := range b.topics {
		stats.Topics[name] = t.stats()
	}
	return stats
}
