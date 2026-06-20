package collector

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/telemetry"
)

// --- fakes ----------------------------------------------------------------

// fakeConsumer serves a fixed set of messages then blocks until ctx is done,
// mimicking a queue that has nothing more to deliver. It records committed
// messages so tests can assert offset-advance behaviour.
type fakeConsumer struct {
	msgs chan queue.Message

	mu         sync.Mutex
	committed  []queue.Message
	failCommit bool
}

func newFakeConsumer(buffer int) *fakeConsumer {
	return &fakeConsumer{msgs: make(chan queue.Message, buffer)}
}

func (f *fakeConsumer) Fetch(ctx context.Context) (queue.Message, error) {
	select {
	case m, ok := <-f.msgs:
		if !ok {
			<-ctx.Done() // channel drained: behave like an empty queue
			return queue.Message{}, ctx.Err()
		}
		return m, nil
	case <-ctx.Done():
		return queue.Message{}, ctx.Err()
	}
}

func (f *fakeConsumer) Commit(_ context.Context, msgs ...queue.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCommit {
		return errors.New("commit failed")
	}
	f.committed = append(f.committed, msgs...)
	return nil
}

func (f *fakeConsumer) Close() error { return nil }

func (f *fakeConsumer) committedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.committed)
}

// fakeStore records inserted records and can be made to fail a number of times.
type fakeStore struct {
	mu        sync.Mutex
	inserted  []telemetry.Record
	failTimes int // number of leading Insert calls that should fail
}

func (s *fakeStore) Insert(_ context.Context, records []telemetry.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failTimes > 0 {
		s.failTimes--
		return errors.New("insert failed")
	}
	s.inserted = append(s.inserted, records...)
	return nil
}

func (s *fakeStore) Ping(context.Context) error  { return nil }
func (s *fakeStore) Close(context.Context) error { return nil }
func (s *fakeStore) insertedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.inserted)
}

// --- helpers --------------------------------------------------------------

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func validMessage(uuid string) queue.Message {
	r := telemetry.Record{
		Timestamp:  time.Date(2025, 7, 18, 20, 42, 34, 0, time.UTC),
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		UUID:       uuid,
		Value:      42,
	}
	b, _ := r.Marshal()
	return queue.NewMessage([]byte(uuid), b, nil)
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// runCollector starts a collector and returns a stop func that cancels and waits
// for Run to return.
func runCollector(t *testing.T, c *Collector) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not return after cancel")
		}
	}
}

// --- tests ----------------------------------------------------------------

func TestPersistsAndCommitsAllMessages(t *testing.T) {
	cons := newFakeConsumer(8)
	st := &fakeStore{}
	for _, id := range []string{"GPU-a", "GPU-b", "GPU-c"} {
		cons.msgs <- validMessage(id)
	}

	c := New(cons, st, Config{BatchSize: 2, FlushInterval: 10 * time.Millisecond}, quietLogger())
	stop := runCollector(t, c)
	defer stop()

	waitFor(t, func() bool { return st.insertedCount() == 3 }, time.Second)
	waitFor(t, func() bool { return cons.committedCount() == 3 }, time.Second)

	if got := c.Stats().Persisted.Load(); got != 3 {
		t.Errorf("persisted = %d, want 3", got)
	}
}

func TestPoisonMessageDroppedButCommitted(t *testing.T) {
	cons := newFakeConsumer(8)
	st := &fakeStore{}
	cons.msgs <- queue.NewMessage(nil, []byte("{garbage"), nil) // unparseable
	cons.msgs <- validMessage("GPU-a")

	c := New(cons, st, Config{BatchSize: 10, FlushInterval: 10 * time.Millisecond}, quietLogger())
	stop := runCollector(t, c)
	defer stop()

	// Only the valid record is persisted, but BOTH messages must be committed so
	// the poison message does not block the partition.
	waitFor(t, func() bool { return cons.committedCount() == 2 }, time.Second)

	if got := st.insertedCount(); got != 1 {
		t.Errorf("inserted = %d, want 1", got)
	}
	if got := c.Stats().Dropped.Load(); got != 1 {
		t.Errorf("dropped = %d, want 1", got)
	}
}

func TestNoCommitWhenPersistFails(t *testing.T) {
	cons := newFakeConsumer(8)
	st := &fakeStore{failTimes: 100} // always fail
	cons.msgs <- validMessage("GPU-a")

	c := New(cons, st, Config{BatchSize: 1, FlushInterval: 10 * time.Millisecond}, quietLogger())
	stop := runCollector(t, c)
	defer stop()

	// Persist keeps failing, so nothing is committed and the flush-error counter
	// climbs (the queue would redeliver in production).
	waitFor(t, func() bool { return c.Stats().FlushErrs.Load() >= 1 }, time.Second)

	if got := cons.committedCount(); got != 0 {
		t.Errorf("committed = %d, want 0 (must not advance offset on failure)", got)
	}
	if got := c.Stats().Persisted.Load(); got != 0 {
		t.Errorf("persisted = %d, want 0", got)
	}
}

func TestCommitFailureRecordedAsFlushError(t *testing.T) {
	cons := newFakeConsumer(8)
	cons.failCommit = true
	st := &fakeStore{}
	cons.msgs <- validMessage("GPU-a")

	c := New(cons, st, Config{BatchSize: 1, FlushInterval: 10 * time.Millisecond}, quietLogger())
	stop := runCollector(t, c)
	defer stop()

	// The record is persisted, but the commit fails: it is counted as a flush
	// error (the message will be redelivered and re-inserted idempotently).
	waitFor(t, func() bool { return c.Stats().FlushErrs.Load() >= 1 }, time.Second)

	if got := st.insertedCount(); got < 1 {
		t.Errorf("inserted = %d, want >= 1 (record is stored before commit)", got)
	}
	if got := cons.committedCount(); got != 0 {
		t.Errorf("committed = %d, want 0 (commit failed)", got)
	}
}
