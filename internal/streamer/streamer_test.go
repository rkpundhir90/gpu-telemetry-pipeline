package streamer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/telemetry"
)

// fakeProducer records published messages and can be made to fail.
type fakeProducer struct {
	mu        sync.Mutex
	published []queue.Message
	fail      bool
}

func (p *fakeProducer) Publish(_ context.Context, msgs ...queue.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fail {
		return errors.New("publish failed")
	}
	p.published = append(p.published, msgs...)
	return nil
}

func (p *fakeProducer) Close() error { return nil }

func (p *fakeProducer) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

func (p *fakeProducer) last() queue.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.published[len(p.published)-1]
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testRecords() []telemetry.Record {
	return []telemetry.Record{
		{UUID: "GPU-a", MetricName: "DCGM_FI_DEV_GPU_UTIL", Value: 1},
		{UUID: "GPU-b", MetricName: "DCGM_FI_DEV_GPU_UTIL", Value: 2},
	}
}

func TestStreamsOncePerRecordWhenNotLooping(t *testing.T) {
	prod := &fakeProducer{}
	s := New(prod, testRecords(), Config{Interval: time.Millisecond, Loop: false}, quietLogger())

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if prod.count() != 2 {
		t.Fatalf("published %d, want 2", prod.count())
	}
	if got := s.Stats().Streamed.Load(); got != 2 {
		t.Errorf("streamed = %d, want 2", got)
	}
	if got := s.Stats().Loops.Load(); got != 1 {
		t.Errorf("loops = %d, want 1", got)
	}
}

func TestStampsProcessingTimeAndKeysByUUID(t *testing.T) {
	prod := &fakeProducer{}
	s := New(prod, testRecords()[:1], Config{Interval: time.Millisecond}, quietLogger())

	before := time.Now().UTC()
	_ = s.Run(context.Background())
	after := time.Now().UTC()

	msg := prod.last()
	if string(msg.Key) != "GPU-a" {
		t.Errorf("partition key = %q, want GPU-a", msg.Key)
	}
	rec, err := telemetry.Unmarshal(msg.Value)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if rec.Timestamp.Before(before) || rec.Timestamp.After(after) {
		t.Errorf("timestamp %v not stamped within the run window", rec.Timestamp)
	}
}

func TestLoopsUntilContextCancelled(t *testing.T) {
	prod := &fakeProducer{}
	s := New(prod, testRecords(), Config{Interval: time.Millisecond, Loop: true}, quietLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if s.Stats().Loops.Load() < 2 {
		t.Errorf("expected multiple loops, got %d", s.Stats().Loops.Load())
	}
}

func TestPublishFailureCountedNotFatal(t *testing.T) {
	prod := &fakeProducer{fail: true}
	s := New(prod, testRecords(), Config{Interval: time.Millisecond, Loop: false}, quietLogger())

	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := s.Stats().PublishErrs.Load(); got != 2 {
		t.Errorf("publish errors = %d, want 2", got)
	}
	if got := s.Stats().Streamed.Load(); got != 0 {
		t.Errorf("streamed = %d, want 0", got)
	}
}

func TestLoadFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "telemetry.csv")
	if err := os.WriteFile(path, []byte(sampleCSV), 0o600); err != nil {
		t.Fatalf("write temp csv: %v", err)
	}
	recs, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
}

func TestLoadRequiresUsablePath(t *testing.T) {
	if _, err := Load(""); err == nil {
		t.Error("expected error when path is empty")
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.csv")); err == nil {
		t.Error("expected error for nonexistent file")
	}
}
