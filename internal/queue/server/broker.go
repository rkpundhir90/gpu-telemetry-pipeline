package server

import (
	"context"
	"fmt"
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
type Topic struct {
	mu               sync.RWMutex
	messages         []Message
	baseOffset       int64 // logical offset of messages[0]
	maxMessages      int
	committedOffsets map[string]int64
	deliveredOffsets map[string]int64
	cond             *sync.Cond // signals waiting consumers when new messages arrive
}

func newTopic(maxMessages int) *Topic {
	t := &Topic{
		messages:         make([]Message, 0, maxMessages),
		maxMessages:      maxMessages,
		committedOffsets: make(map[string]int64),
		deliveredOffsets: make(map[string]int64),
	}
	t.cond = sync.NewCond(&t.mu)
	return t
}

func (t *Topic) Produce(msgs []Message) []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

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

	t.cond.Broadcast() // wake consumers waiting for new data
	return offsets
}

// Consume blocks until a message is available or wait returns false. Advances
// lagging consumers to baseOffset rather than returning an error.
func (t *Topic) Consume(groupID string, wait func() bool) (*Message, int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.deliveredOffsets[groupID] < t.baseOffset {
		t.deliveredOffsets[groupID] = t.baseOffset
	}

	for wait() && t.deliveredOffsets[groupID] >= t.baseOffset+int64(len(t.messages)) {
		t.cond.Wait()
	}

	if t.deliveredOffsets[groupID] >= t.baseOffset+int64(len(t.messages)) {
		return nil, 0, fmt.Errorf("no messages available")
	}

	localIdx := t.deliveredOffsets[groupID] - t.baseOffset
	offset := t.deliveredOffsets[groupID]
	msg := t.messages[localIdx]
	t.deliveredOffsets[groupID]++

	return &msg, offset, nil
}

func (t *Topic) Commit(groupID string, offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.committedOffsets[groupID] = offset
	return nil
}

func (t *Topic) stats() TopicStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
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

// ConsumeBatch blocks until at least one message is available or ctx is cancelled.
func (t *Topic) ConsumeBatch(groupID string, maxBatch int, ctx context.Context) ([]BatchedMsg, error) {
	// Unblock cond.Wait when ctx is cancelled so shutdown isn't delayed.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			t.cond.Broadcast()
		case <-stop:
		}
	}()

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.deliveredOffsets[groupID] < t.baseOffset {
		t.deliveredOffsets[groupID] = t.baseOffset
	}

	for ctx.Err() == nil && t.deliveredOffsets[groupID] >= t.baseOffset+int64(len(t.messages)) {
		t.cond.Wait()
	}

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

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
	return batch, nil
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
