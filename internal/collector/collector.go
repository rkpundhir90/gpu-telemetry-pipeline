// Package collector consumes telemetry from a queue, persists records in batches,
// and commits offsets only after a batch is durably stored (at-least-once delivery).
package collector

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/store"
	"gpu-telemetry-pipeline/internal/telemetry"
)

// Config tunes the batching behaviour of a Collector.
type Config struct {
	BatchSize     int           // records before a forced flush
	FlushInterval time.Duration // max time a partial batch waits before flushing
	FlushTimeout  time.Duration // bounds a single persist+commit attempt
}

func (c *Config) withDefaults() {
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = time.Second
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = 15 * time.Second
	}
}

// Stats are cumulative counters exposed for observability.
type Stats struct {
	Persisted atomic.Int64
	Dropped   atomic.Int64
	Batches   atomic.Int64
	FlushErrs atomic.Int64
}

// Collector wires a queue consumer to a telemetry store.
type Collector struct {
	consumer queue.Consumer
	store    store.TelemetryStore
	log      *slog.Logger
	cfg      Config
	stats    Stats
}

// New constructs a Collector. The consumer and store must outlive Run.
func New(consumer queue.Consumer, st store.TelemetryStore, cfg Config, log *slog.Logger) *Collector {
	cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Collector{consumer: consumer, store: st, log: log, cfg: cfg}
}

func (c *Collector) Stats() *Stats { return &c.stats }

// item pairs a parsed record with its source message. parsed=false for messages
// that failed validation: they are not stored, but still committed to unblock
// the partition (poison-message drop).
type item struct {
	msg    queue.Message
	record telemetry.Record
	parsed bool
}

// Run consumes until ctx is cancelled, then drains any buffered records before
// returning. Returns nil on a clean shutdown.
func (c *Collector) Run(ctx context.Context) error {
	c.log.Info("collector started",
		"batch_size", c.cfg.BatchSize,
		"flush_interval", c.cfg.FlushInterval.String(),
	)

	// Fetch blocks, so it runs in its own goroutine and feeds a channel the main
	// loop can multiplex with the flush ticker.
	items := make(chan item)
	fetchErr := make(chan error, 1)
	go c.fetchLoop(ctx, items, fetchErr)

	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]item, 0, c.cfg.BatchSize)

	for {
		select {
		case it, ok := <-items:
			if !ok {
				c.flush(batch)
				err := <-fetchErr
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					c.log.Info("collector stopped", "reason", "context cancelled")
					return nil
				}
				return err
			}
			batch = append(batch, it)
			if len(batch) >= c.cfg.BatchSize {
				batch = c.flush(batch)
			}
		case <-ticker.C:
			batch = c.flush(batch)
		}
	}
}

// fetchLoop pulls messages, parses them, and forwards items until ctx is done.
func (c *Collector) fetchLoop(ctx context.Context, items chan<- item, fetchErr chan<- error) {
	defer close(items)
	c.log.Info("fetch loop started — waiting for messages from queue")
	var fetched int64
	for {
		msg, err := c.consumer.Fetch(ctx)
		if err != nil {
			fetchErr <- err
			return
		}
		fetched++
		if fetched == 1 {
			c.log.Info("first message received from queue", "bytes", len(msg.Value))
		}

		it := item{msg: msg}
		rec, perr := telemetry.Unmarshal(msg.Value)
		if perr == nil {
			perr = rec.Validate()
		}
		if perr != nil {
			c.stats.Dropped.Add(1)
			preview := string(msg.Value)
			if len(preview) > 200 {
				preview = preview[:200] + "..."
			}
			c.log.Warn("dropping unparseable message",
				"error", perr,
				"bytes", len(msg.Value),
				"preview", preview,
			)
		} else {
			it.record = rec
			it.parsed = true
		}

		select {
		case items <- it:
		case <-ctx.Done():
			fetchErr <- ctx.Err()
			return
		}
	}
}

// flush persists parsed records and commits all messages (including dropped ones)
// so offsets advance. On failure nothing is committed; the queue redelivers the
// batch and idempotent re-insert makes that safe.
func (c *Collector) flush(batch []item) []item {
	if len(batch) == 0 {
		return batch[:0]
	}

	records := make([]telemetry.Record, 0, len(batch))
	msgs := make([]queue.Message, 0, len(batch))
	for _, it := range batch {
		msgs = append(msgs, it.msg)
		if it.parsed {
			records = append(records, it.record)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.FlushTimeout)
	defer cancel()

	if err := c.store.Insert(ctx, records); err != nil {
		c.stats.FlushErrs.Add(1)
		c.log.Error("persist failed; batch will be redelivered",
			"error", err, "records", len(records))
		return batch[:0]
	}

	if err := c.consumer.Commit(ctx, msgs...); err != nil {
		// Records stored but commit failed: redelivered and re-inserted idempotently.
		c.stats.FlushErrs.Add(1)
		c.log.Error("commit failed after persist; messages will be redelivered",
			"error", err, "messages", len(msgs))
		return batch[:0]
	}

	c.stats.Persisted.Add(int64(len(records)))
	c.stats.Batches.Add(1)
	c.log.Info("flushed batch",
		"parsed", len(records),
		"dropped_in_batch", len(msgs)-len(records),
		"committed", len(msgs),
		"total_persisted", c.stats.Persisted.Load(),
		"total_dropped", c.stats.Dropped.Load(),
		"batches", c.stats.Batches.Load(),
	)
	return batch[:0]
}
