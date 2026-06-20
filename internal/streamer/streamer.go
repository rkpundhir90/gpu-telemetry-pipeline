// Package streamer implements the Telemetry Streamer: it loads GPU telemetry
// from a CSV and replays it onto the queue, stamping each datapoint with the
// time it is processed (published), per the project brief.
//
// Scaling is horizontal and coordination-free, mirroring the Collector. Each
// replica streams the full dataset independently; because every datapoint's
// timestamp is its publish time, two replicas emitting the same CSV row produce
// two distinct datapoints rather than a duplicate. Adding replicas therefore
// multiplies the telemetry rate with no sharding or shared state, so a
// Deployment + HorizontalPodAutoscaler can scale Streamers up and down freely.
// Keying each message by GPU UUID keeps a given GPU's series on one partition,
// so it stays ordered through the pipeline.
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
	// Interval is the delay between successive datapoints. It sets the per-replica
	// stream rate; the fleet's aggregate rate scales with the replica count.
	Interval time.Duration

	// Loop replays the dataset endlessly to simulate a continuous stream. When
	// false the Streamer publishes the dataset once and returns.
	Loop bool
}

func (c *Config) withDefaults() {
	if c.Interval <= 0 {
		c.Interval = 10 * time.Millisecond
	}
}

// Stats are cumulative counters exposed for observability (logs / health).
type Stats struct {
	Streamed    atomic.Int64 // datapoints successfully published
	PublishErrs atomic.Int64 // failed publish attempts
	Loops       atomic.Int64 // completed passes over the dataset
}

// Streamer replays a fixed set of records onto a queue.Producer.
type Streamer struct {
	producer queue.Producer
	records  []telemetry.Record
	log      *slog.Logger
	cfg      Config
	stats    Stats
}

// Load reads telemetry records from the CSV at path. The path points at a file
// mounted from a PersistentVolume, so the dataset is provisioned independently of
// the image and read at runtime. The records carry no timestamp; Run stamps each
// one at publish time.
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

// New constructs a Streamer over the given records. The producer is owned by the
// caller and must outlive Run; Run does not close it.
func New(producer queue.Producer, records []telemetry.Record, cfg Config, log *slog.Logger) *Streamer {
	cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Streamer{producer: producer, records: records, log: log, cfg: cfg}
}

// Stats returns a pointer to the live counters.
func (s *Streamer) Stats() *Stats { return &s.stats }

// Run replays the dataset until ctx is cancelled (or once, if Loop is false). It
// returns nil on a clean (context-cancelled) shutdown.
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
		s.log.Debug("completed dataset pass", "loop", loops, "streamed", s.stats.Streamed.Load())
		if !s.cfg.Loop {
			s.log.Info("streamer finished", "streamed", s.stats.Streamed.Load())
			return nil
		}
	}
}

// publish stamps the record with the current processing time and writes it,
// keyed by GPU UUID. A publish failure is counted and logged but does not stop
// the stream: this is a best-effort simulator, and the next datapoint follows on
// the next tick.
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
		// A cancelled context during shutdown is expected, not an error to count.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		s.stats.PublishErrs.Add(1)
		s.log.Error("publish failed", "error", err, "uuid", rec.UUID, "metric", rec.MetricName)
		return
	}
	s.stats.Streamed.Add(1)
}
