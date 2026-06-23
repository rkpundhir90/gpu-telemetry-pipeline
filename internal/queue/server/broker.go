package server

import (
	"fmt"
	"sync"
)

// Message represents a single data point in the queue.
type Message struct {
	Key   []byte
	Value []byte
}

// Topic manages a sequence of messages and tracks offsets for various consumer groups.
type Topic struct {
	mu sync.RWMutex
	// messages is the append-only log of data for this topic.
	messages []Message
	// committedOffsets tracks the last acknowledged offset for each groupID.
	committedOffsets map[string]int64
	// deliveredOffsets tracks the next message to be delivered to a group.
	deliveredOffsets map[string]int64
	// cond is used to signal waiting consumers when new messages are produced.
	cond *sync.Cond
}

func NewTopic() *Topic {
	t := &Topic{
		messages:         make([]Message, 0),
		committedOffsets: make(map[string]int64),
		deliveredOffsets: make(map[string]int64),
	}
	t.cond = sync.NewCond(&t.mu)
	return t
}

// Produce appends a batch of messages to the topic.
func (t *Topic) Produce(msgs []Message) []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	offsets := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		offsets = append(offsets, int64(len(t.messages)))
		t.messages = append(t.messages, m)
	}

	// Wake up any consumers waiting for new data.
	t.cond.Broadcast()
	return offsets
}

// Consume retrieves the next available message for a group.
// It blocks until a message is available or the wait function returns false.
func (t *Topic) Consume(groupID string, wait func() bool) (*Message, int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for wait() && t.deliveredOffsets[groupID] >= int64(len(t.messages)) {
		t.cond.Wait()
	}

	if t.deliveredOffsets[groupID] >= int64(len(t.messages)) {
		return nil, 0, fmt.Errorf("no messages available")
	}

	offset := t.deliveredOffsets[groupID]
	msg := t.messages[offset]
	t.deliveredOffsets[groupID]++

	return &msg, offset, nil
}

// Commit advances the committed offset for a group.
func (t *Topic) Commit(groupID string, offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// In a simple MVP, we just set the offset. In a real system, we'd validate it.
	t.committedOffsets[groupID] = offset
	return nil
}

// Broker manages multiple topics.
type Broker struct {
	mu     sync.RWMutex
	topics map[string]*Topic
}

func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]*Topic),
	}
}

func (b *Broker) GetTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.topics[name]
	if !ok {
		t = NewTopic()
		b.topics[name] = t
	}
	return t
}
