// Package streamer loads GPU telemetry from a CSV and replays it onto the queue,
// stamping each datapoint with its publish time.
package streamer

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/telemetry"
)

// Config tunes the replay behaviour of a Streamer.
type Config struct {
	Interval time.Duration // per-replica delay between datapoints
	Loop     bool          // replay the dataset endlessly
}

func (c *Config) withDefaults() {
	if c.Interval <= 0 {
		c.Interval = 10 * time.Millisecond
	}
}

// Stats are cumulative counters exposed for observability.
type Stats struct {
	Streamed    atomic.Int64
	PublishErrs atomic.Int64
	Loops       atomic.Int64
}

// Streamer replays a fixed set of records onto a queue.Producer.
type Streamer struct {
	producer queue.Producer
	records  []telemetry.Record
	log      *slog.Logger
	cfg      Config
	stats    Stats
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
	return &Streamer{producer: producer, records: records, log: log, cfg: cfg}
}

func (s *Streamer) Stats() *Stats { return &s.stats }

// Run replays the dataset until ctx is cancelled (or once if Loop is false).
func (s *Streamer) Run(ctx context.Context) error {
	s.log.Info("streamer started",
		"records", len(s.records),
		"interval", s.cfg.Interval.String(),
		"loop", s.cfg.Loop,
	)

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		for i := range s.records {
			select {
			case <-ctx.Done():
				s.log.Info("streamer stopped", "reason", "context cancelled",
					"streamed", s.stats.Streamed.Load())
				return nil
			case <-ticker.C:
			}
			s.publish(ctx, &s.records[i])
		}

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
