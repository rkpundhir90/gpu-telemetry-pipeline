package server

import (
	"fmt"
	"sync"
)

type Message struct {
	Key   []byte
	Value []byte
}

// Topic is an append-only log with per-consumer-group offset tracking.
type Topic struct {
	mu               sync.RWMutex
	messages         []Message
	committedOffsets map[string]int64
	deliveredOffsets map[string]int64
	cond             *sync.Cond // signals waiting consumers when new messages arrive
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

func (t *Topic) Produce(msgs []Message) []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	offsets := make([]int64, 0, len(msgs))
	for _, m := range msgs {
		offsets = append(offsets, int64(len(t.messages)))
		t.messages = append(t.messages, m)
	}

	t.cond.Broadcast() // wake consumers waiting for new data
	return offsets
}

// Consume returns the next message for a group, blocking until one is available
// or wait returns false.
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

func (t *Topic) Commit(groupID string, offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.committedOffsets[groupID] = offset
	return nil
}

// Broker manages multiple topics, creating them on first access.
type Broker struct {
	mu     sync.RWMutex
	topics map[string]*Topic
}

func NewBroker() *Broker {
	return &Broker{topics: make(map[string]*Topic)}
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
