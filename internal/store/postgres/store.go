// Package postgres implements store.TelemetryStore on PostgreSQL with the
// TimescaleDB extension.
//
// TimescaleDB is a natural fit for this workload: telemetry is append-heavy,
// time-ordered, and queried by GPU over time windows. The table is promoted to
// a hypertable so writes and the API's "telemetry by GPU, ordered by time,
// optionally windowed" reads are automatically partitioned by time.
//
// The driver is jackc/pgx (pure Go), so the service still links statically
// (CGO_ENABLED=0) onto the distroless-static base image.
//
// At-least-once delivery from the Collector is made idempotent here via a unique
// constraint on (uuid, metric_name, time) plus ON CONFLICT DO NOTHING, so a
// redelivered batch after a crash does not create duplicate rows.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"gpu-telemetry-pipeline/internal/telemetry"
)

// TableName is the hypertable telemetry is written to. Exported so the API layer
// can reference the same table without hard-coding the string in two places.
const TableName = "gpu_telemetry"

const insertSQL = `
INSERT INTO ` + TableName + `
  (time, metric_name, gpu_id, device, uuid, model_name, hostname, container, pod, namespace, value, labels)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
ON CONFLICT (uuid, metric_name, time) DO NOTHING`

// Store is a TimescaleDB-backed telemetry store. It is safe for concurrent use:
// pgxpool manages a connection pool internally.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to PostgreSQL using the given DSN and returns a ready Store. The
// caller is responsible for running EnsureSchema once at startup.
func New(ctx context.Context, dsn string) (*Store, error) {
	if dsn == "" {
		return nil, errors.New("postgres: dsn is required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &Store{pool: pool}, nil
}

// EnsureSchema creates the telemetry table, the idempotency constraint, and the
// query index, then promotes the table to a TimescaleDB hypertable. All steps
// are idempotent and safe to run on every startup (every Collector replica may
// call this; concurrent runs converge on the same schema).
//
// Hypertable promotion is best-effort: if the timescaledb extension is not
// available, the table still functions as plain PostgreSQL with a B-tree index,
// so the pipeline remains runnable (with a logged warning from the caller).
func (s *Store) EnsureSchema(ctx context.Context) error {
	const createTable = `
CREATE TABLE IF NOT EXISTS ` + TableName + ` (
  time        timestamptz      NOT NULL,
  metric_name text             NOT NULL,
  gpu_id      text,
  device      text,
  uuid        text             NOT NULL,
  model_name  text,
  hostname    text,
  container   text,
  pod         text,
  namespace   text,
  value       double precision,
  labels      jsonb
)`
	if _, err := s.pool.Exec(ctx, createTable); err != nil {
		return fmt.Errorf("postgres: create table: %w", err)
	}

	// Unique key for idempotent inserts. It includes the partition column
	// (time), which TimescaleDB requires for unique indexes on hypertables.
	const uniqueIdx = `
CREATE UNIQUE INDEX IF NOT EXISTS ` + TableName + `_uuid_metric_time_uidx
  ON ` + TableName + ` (uuid, metric_name, time)`
	if _, err := s.pool.Exec(ctx, uniqueIdx); err != nil {
		return fmt.Errorf("postgres: create unique index: %w", err)
	}

	// Primary read path for the API: a GPU's series ordered by time (DESC for
	// "latest first" and efficient time-window scans).
	const queryIdx = `
CREATE INDEX IF NOT EXISTS ` + TableName + `_uuid_time_idx
  ON ` + TableName + ` (uuid, time DESC)`
	if _, err := s.pool.Exec(ctx, queryIdx); err != nil {
		return fmt.Errorf("postgres: create query index: %w", err)
	}

	// Promote to a hypertable (best-effort; requires the timescaledb extension).
	return s.ensureHypertable(ctx)
}

// ErrHypertableUnavailable indicates TimescaleDB hypertable promotion could not
// be completed (e.g. the extension is not installed). The plain table is still
// usable; callers may log this as a warning rather than fail.
var ErrHypertableUnavailable = errors.New("postgres: timescaledb hypertable unavailable")

func (s *Store) ensureHypertable(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS timescaledb`); err != nil {
		return fmt.Errorf("%w: create extension: %v", ErrHypertableUnavailable, err)
	}
	// if_not_exists keeps this idempotent across restarts and replicas;
	// migrate_data lets it succeed even if rows already exist.
	const hypertable = `SELECT create_hypertable($1, 'time', if_not_exists => TRUE, migrate_data => TRUE)`
	if _, err := s.pool.Exec(ctx, hypertable, TableName); err != nil {
		return fmt.Errorf("%w: create_hypertable: %v", ErrHypertableUnavailable, err)
	}
	return nil
}

// Insert writes a batch of records in a single pipelined round trip. Duplicate
// rows (same uuid/metric/time) are silently ignored, making redelivery safe.
//
// The whole batch shares one implicit failure boundary: if any statement errors,
// Insert returns that error and the caller must not advance the queue offset, so
// the batch is redelivered and (idempotently) retried.
func (s *Store) Insert(ctx context.Context, records []telemetry.Record) error {
	if len(records) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for i := range records {
		r := &records[i]

		var labels any
		if len(r.Labels) > 0 {
			b, err := json.Marshal(r.Labels)
			if err != nil {
				return fmt.Errorf("postgres: marshal labels for %s/%s: %w", r.UUID, r.MetricName, err)
			}
			labels = string(b)
		}

		batch.Queue(insertSQL,
			r.Timestamp.UTC(), r.MetricName, r.GPUID, r.Device, r.UUID,
			r.ModelName, r.Hostname, r.Container, r.Pod, r.Namespace,
			r.Value, labels,
		)
	}

	br := s.pool.SendBatch(ctx, batch)
	// Drain every queued result so the batch is fully applied; surface the first
	// error. Close is also called on the error path to release the connection.
	var firstErr error
	for range records {
		if _, err := br.Exec(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if cerr := br.Close(); cerr != nil && firstErr == nil {
		firstErr = cerr
	}
	if firstErr != nil {
		return fmt.Errorf("postgres: insert batch of %d: %w", len(records), firstErr)
	}
	return nil
}

// Ping verifies the database is reachable.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases the connection pool. The ctx is accepted to satisfy the
// store.TelemetryStore interface; pgxpool.Close itself does not block on it.
func (s *Store) Close(_ context.Context) error {
	s.pool.Close()
	return nil
}
