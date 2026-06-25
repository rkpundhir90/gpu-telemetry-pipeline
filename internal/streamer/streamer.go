// Package streamer loads GPU telemetry from a CSV and replays it onto the queue,
// stamping each datapoint with its publish time.
package streamer

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/telemetry"
)

type Config struct {
	Interval      time.Duration // per-replica delay between datapoints
	Loop          bool          // replay the dataset endlessly
	CheckpointDir string        // directory for progress checkpoint; empty = no checkpoint
}

func (c *Config) withDefaults() {
	if c.Interval <= 0 {
		c.Interval = 10 * time.Millisecond
	}
}

type Stats struct {
	Streamed    atomic.Int64
	PublishErrs atomic.Int64
	Loops       atomic.Int64
}

type Streamer struct {
	producer queue.Producer
	records  []telemetry.Record
	log      *slog.Logger
	cfg      Config
	stats    Stats
	cp       *checkpointer
}

// Load reads telemetry records from the CSV at path. Timestamps are left zero;
// Run stamps each record at publish time.
func Load(path string) ([]telemetry.Record, error) {
	if path == "" {
		return nil, errors.New("streamer: csv path is required (set STREAMER_CSV_PATH)")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseCSV(f)
}

// New constructs a Streamer. The producer must outlive Run.
func New(producer queue.Producer, records []telemetry.Record, cfg Config, log *slog.Logger) *Streamer {
	cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Streamer{
		producer: producer,
		records:  records,
		log:      log,
		cfg:      cfg,
		cp:       newCheckpointer(cfg.CheckpointDir),
	}
}

func (s *Streamer) Stats() *Stats { return &s.stats }

// Run replays the dataset until ctx is cancelled (or once if Loop is false).
// If CheckpointDir is set, Run resumes from the last saved position so a pod
// restart continues from where it left off rather than replaying from the start.
func (s *Streamer) Run(ctx context.Context) error {
	startIdx := s.cp.load(len(s.records))
	s.log.Info("streamer started",
		"records", len(s.records),
		"interval", s.cfg.Interval.String(),
		"loop", s.cfg.Loop,
		"resume_from", startIdx,
	)

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		for i := startIdx; i < len(s.records); i++ {
			select {
			case <-ctx.Done():
				s.cp.save(i) // save position so next start resumes here
				s.log.Info("streamer stopped", "reason", "context cancelled",
					"streamed", s.stats.Streamed.Load(), "checkpoint", i)
				return nil
			case <-ticker.C:
			}
			s.publish(ctx, &s.records[i])

			// Checkpoint every 500 records so restart overhead is bounded.
			if i > 0 && i%500 == 0 {
				s.cp.save(i + 1)
			}
		}
		startIdx = 0
		s.cp.save(0)

		loops := s.stats.Loops.Add(1)
		s.log.Info("completed dataset pass",
			"loop", loops,
			"streamed", s.stats.Streamed.Load(),
			"publish_errors", s.stats.PublishErrs.Load(),
		)
		if !s.cfg.Loop {
			s.log.Info("streamer finished", "streamed", s.stats.Streamed.Load())
			return nil
		}
	}
}

// publish stamps the record with now and writes it keyed by UUID. A publish
// failure is counted but does not stop the stream.
func (s *Streamer) publish(ctx context.Context, rec *telemetry.Record) {
	rec.Timestamp = time.Now().UTC()

	value, err := rec.Marshal()
	if err != nil {
		s.stats.PublishErrs.Add(1)
		s.log.Error("marshal failed", "error", err, "uuid", rec.UUID, "metric", rec.MetricName)
		return
	}

	msg := queue.NewMessage([]byte(rec.UUID), value, nil)
	if err := s.producer.Publish(ctx, msg); err != nil {
		// Cancelled context during shutdown is expected; don't count it.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		s.stats.PublishErrs.Add(1)
		s.log.Error("publish failed", "error", err, "uuid", rec.UUID, "metric", rec.MetricName)
		return
	}
	n := s.stats.Streamed.Add(1)
	// Log first publish then every 500th to confirm connectivity without flooding.
	if n == 1 || n%500 == 0 {
		s.log.Info("publishing records",
			"streamed", n,
			"publish_errors", s.stats.PublishErrs.Load(),
			"uuid", rec.UUID,
			"metric", rec.MetricName,
		)
	}
}

// checkpointer writes the current record index to disk so restarts resume rather than replay.
// nil is safe (CheckpointDir unset).
type checkpointer struct {
	path string
}

func newCheckpointer(dir string) *checkpointer {
	if dir == "" {
		return nil
	}
	return &checkpointer{path: filepath.Join(dir, "progress")}
}

// load returns the saved index, or 0 on any error (missing file, corrupt data,
// out-of-bounds for current dataset size).
func (c *checkpointer) load(max int) int {
	if c == nil {
		return 0
	}
	data, err := os.ReadFile(c.path)
	if err != nil {
		return 0
	}
	idx, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || idx < 0 || idx >= max {
		return 0
	}
	return idx
}

// save writes idx; errors are silently discarded (missing checkpoint = replay from 0, not a fatal failure).
func (c *checkpointer) save(idx int) {
	if c == nil {
		return
	}
	_ = os.WriteFile(c.path, []byte(strconv.Itoa(idx)), 0600)
}
