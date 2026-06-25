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
	BaseOffset       int64
	HeadOffset       int64
	CommittedOffsets map[string]int64
}

// Topic is a bounded append-only in-memory log. When full the oldest quarter is
// evicted; lagging consumers are silently reset to the new base rather than erroring.
//
// Blocking consumers wait on a notify channel that is closed on every Produce and
// immediately replaced. This lets ConsumeBatch select on ctx.Done() natively,
// removing the per-call goroutine that was needed with sync.Cond.
type Topic struct {
	mu               sync.Mutex
	messages         []Message
	baseOffset       int64 // logical offset of messages[0]
	maxMessages      int
	committedOffsets map[string]int64
	deliveredOffsets map[string]int64
	notify           chan struct{} // closed on each Produce, replaced atomically
}

func newTopic(maxMessages int) *Topic {
	return &Topic{
		messages:         make([]Message, 0, maxMessages),
		maxMessages:      maxMessages,
		committedOffsets: make(map[string]int64),
		deliveredOffsets: make(map[string]int64),
		notify:           make(chan struct{}),
	}
}

func (t *Topic) Produce(msgs []Message) []int64 {
	t.mu.Lock()
	offsets := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		offsets = append(offsets, t.baseOffset+int64(len(t.messages)))
		t.messages = append(t.messages, m)
	}

	// Batch-evict a quarter at a time to amortise the copy cost.
	if len(t.messages) > t.maxMessages {
		evict := t.maxMessages / 4
		t.messages = t.messages[evict:]
		t.baseOffset += int64(evict)
	}

	// Capture and replace notify before unlocking so waiting consumers see the
	// close only after the new channel is in place.
	notify := t.notify
	t.notify = make(chan struct{})
	t.mu.Unlock()

	// Close after unlock: woken consumers acquire the mutex immediately rather
	// than queueing behind us.
	close(notify)
	return offsets
}

// ConsumeBatch blocks until at least one message is available or ctx is cancelled.
// It loops with a lock-release-select-reacquire cycle so no goroutine is spawned
// per call.
func (t *Topic) ConsumeBatch(groupID string, maxBatch int, ctx context.Context) ([]BatchedMsg, error) {
	for {
		t.mu.Lock()

		// Advance a lagging consumer to the oldest buffered offset.
		if t.deliveredOffsets[groupID] < t.baseOffset {
			t.deliveredOffsets[groupID] = t.baseOffset
		}

		if ctx.Err() != nil {
			t.mu.Unlock()
			return nil, ctx.Err()
		}

		if t.deliveredOffsets[groupID] < t.baseOffset+int64(len(t.messages)) {
			available := t.baseOffset + int64(len(t.messages)) - t.deliveredOffsets[groupID]
			count := int(available)
			if count > maxBatch {
				count = maxBatch
			}
			batch := make([]BatchedMsg, count)
			for i := range batch {
				localIdx := t.deliveredOffsets[groupID] - t.baseOffset
				batch[i] = BatchedMsg{Msg: t.messages[localIdx], Offset: t.deliveredOffsets[groupID]}
				t.deliveredOffsets[groupID]++
			}
			t.mu.Unlock()
			return batch, nil
		}

		// No messages yet — wait outside the lock so Produce can append.
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
		Buffered:         len(t.messages),
		BaseOffset:       t.baseOffset,
		HeadOffset:       t.baseOffset + int64(len(t.messages)),
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
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.topics[name]
	if !ok {
		t = newTopic(b.maxMessages)
		b.topics[name] = t
	}
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

