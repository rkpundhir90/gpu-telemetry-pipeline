// Package collector implements the Telemetry Collector: it consumes telemetry
// from the queue, parses each message into a telemetry.Record, persists records
// in batches, and acknowledges the queue only after a batch is durably stored.
//
// Scaling is horizontal and external to this code: each replica runs one
// Collector bound to one queue.Consumer that is a member of a shared consumer
// group. The queue distributes partitions across replicas, so adding or removing
// replicas changes throughput without any code change here. Within a replica the
// design is a single consume->batch->persist->commit loop, which keeps
// at-least-once semantics simple and correct.
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
	// BatchSize is the number of records that triggers a flush. Larger batches
	// amortise the per-write cost; smaller batches lower end-to-end latency.
	BatchSize int

	// FlushInterval forces a flush of a partially-filled batch so low-traffic
	// periods still persist promptly.
	FlushInterval time.Duration

	// FlushTimeout bounds a single persist+commit attempt, and also bounds the
	// final drain flush during shutdown (when the run context is already done).
	FlushTimeout time.Duration
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

// Stats are cumulative counters exposed for observability (logs / health).
type Stats struct {
	Persisted atomic.Int64 // records successfully stored
	Dropped   atomic.Int64 // messages discarded as unparseable/invalid
	Batches   atomic.Int64 // batches committed
	FlushErrs atomic.Int64 // failed flush attempts (records redelivered)
}

// Collector wires a queue consumer to a telemetry store.
type Collector struct {
	consumer queue.Consumer
	store    store.TelemetryStore
	log      *slog.Logger
	cfg      Config
	stats    Stats
}

// New constructs a Collector. The consumer and store are owned by the caller and
// must outlive Run; Run does not close them.
func New(consumer queue.Consumer, st store.TelemetryStore, cfg Config, log *slog.Logger) *Collector {
	cfg.withDefaults()
	if log == nil {
		log = slog.Default()
	}
	return &Collector{consumer: consumer, store: st, log: log, cfg: cfg}
}

// Stats returns a pointer to the live counters.
func (c *Collector) Stats() *Stats { return &c.stats }

// item pairs a parsed record with the raw queue message it came from, so the
// message can be committed once the record is persisted. parsed is false for
// messages that failed to parse/validate: they are not stored, but their offset
// is still committed so they do not block the partition (poison-message drop).
type item struct {
	msg    queue.Message
	record telemetry.Record
	parsed bool
}

// Run consumes until ctx is cancelled, then drains and flushes any buffered
// records before returning. It returns nil on a clean (context-cancelled)
// shutdown.
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
				// Fetch loop stopped (ctx cancelled or consumer closed). Drain
				// whatever is buffered, then report why fetching ended.
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

// fetchLoop pulls messages, parses them, and forwards items until ctx is done or
// the consumer errors. It always closes items and reports the terminating error.
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
			// parsed stays false: not stored, but still committed downstream.
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

// flush persists the parsed records in the batch and, on success, commits every
// message (parsed and dropped) so offsets advance. On failure nothing is
// committed, so the queue redelivers the whole batch (idempotent re-insert makes
// this safe). It returns a fresh, empty batch slice for reuse.
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
		return batch[:0] // do not commit; queue redelivers
	}

	if err := c.consumer.Commit(ctx, msgs...); err != nil {
		// Records are stored but the commit failed: they will be redelivered and
		// re-inserted idempotently. Log and continue.
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
