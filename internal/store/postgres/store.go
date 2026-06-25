// Package postgres implements store.TelemetryStore on PostgreSQL + TimescaleDB.
// The table is a hypertable partitioned by time; a unique index on
// (uuid, metric_name, time) makes inserts idempotent under at-least-once delivery.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"gpu-telemetry-pipeline/internal/store"
	"gpu-telemetry-pipeline/internal/telemetry"
)

// TableName is the hypertable telemetry is written to.
const TableName = "gpu_telemetry"

const insertSQL = `
INSERT INTO ` + TableName + `
  (time, metric_name, gpu_id, device, uuid, model_name, hostname, container, pod, namespace, value, labels)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)
ON CONFLICT (uuid, metric_name, time) DO NOTHING`

// Store is a TimescaleDB-backed telemetry store, safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to PostgreSQL and returns a ready Store. Call EnsureSchema once at startup.
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

// EnsureSchema creates the table, idempotency index, and query index, then
// promotes to a TimescaleDB hypertable. All steps are idempotent and safe to
// run on every startup across multiple replicas.
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

	// Unique key must include the partition column (time) — required by TimescaleDB.
	const uniqueIdx = `
CREATE UNIQUE INDEX IF NOT EXISTS ` + TableName + `_uuid_metric_time_uidx
  ON ` + TableName + ` (uuid, metric_name, time)`
	if _, err := s.pool.Exec(ctx, uniqueIdx); err != nil {
		return fmt.Errorf("postgres: create unique index: %w", err)
	}

	// Primary read path: GPU series ordered newest-first with optional time window.
	const queryIdx = `
CREATE INDEX IF NOT EXISTS ` + TableName + `_uuid_time_idx
  ON ` + TableName + ` (uuid, time DESC)`
	if _, err := s.pool.Exec(ctx, queryIdx); err != nil {
		return fmt.Errorf("postgres: create query index: %w", err)
	}

	return s.ensureHypertable(ctx)
}

// ErrHypertableUnavailable is returned when TimescaleDB hypertable promotion
// fails (e.g. extension not installed). The plain table is still usable.
var ErrHypertableUnavailable = errors.New("postgres: timescaledb hypertable unavailable")

func (s *Store) ensureHypertable(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS timescaledb`); err != nil {
		return fmt.Errorf("%w: create extension: %v", ErrHypertableUnavailable, err)
	}
	// if_not_exists + migrate_data: idempotent across restarts even when rows exist.
	const hypertable = `SELECT create_hypertable($1, 'time', if_not_exists => TRUE, migrate_data => TRUE)`
	if _, err := s.pool.Exec(ctx, hypertable, TableName); err != nil {
		return fmt.Errorf("%w: create_hypertable: %v", ErrHypertableUnavailable, err)
	}
	return nil
}

// Insert writes a batch in a single pipelined round trip. Duplicates are
// silently ignored. On any error the caller must not advance the queue offset.
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
	// Drain every result to apply the full batch; surface the first error.
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

const listGPUsSQL = `
SELECT uuid,
       COALESCE(MAX(model_name), '') AS model_name,
       COALESCE(MAX(hostname), '')   AS hostname,
       MAX(time)                     AS last_seen
FROM ` + TableName + `
GROUP BY uuid
ORDER BY uuid`

func (s *Store) ListGPUs(ctx context.Context) ([]store.GPU, error) {
	rows, err := s.pool.Query(ctx, listGPUsSQL)
	if err != nil {
		return nil, fmt.Errorf("postgres: list gpus: %w", err)
	}
	defer rows.Close()

	var gpus []store.GPU
	for rows.Next() {
		var g store.GPU
		if err := rows.Scan(&g.UUID, &g.ModelName, &g.Hostname, &g.LastSeen); err != nil {
			return nil, fmt.Errorf("postgres: scan gpu: %w", err)
		}
		gpus = append(gpus, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list gpus: %w", err)
	}
	return gpus, nil
}

const queryTelemetrySQL = `
SELECT time, metric_name,
       COALESCE(gpu_id, ''), COALESCE(device, ''), uuid,
       COALESCE(model_name, ''), COALESCE(hostname, ''),
       COALESCE(container, ''), COALESCE(pod, ''), COALESCE(namespace, ''),
       COALESCE(value, 0), labels
FROM ` + TableName + `
WHERE uuid = $1
  AND ($2::timestamptz IS NULL OR time >= $2)
  AND ($3::timestamptz IS NULL OR time <= $3)
ORDER BY time DESC
LIMIT $4`

func (s *Store) QueryTelemetry(ctx context.Context, q store.TelemetryQuery) ([]telemetry.Record, error) {
	rows, err := s.pool.Query(ctx, queryTelemetrySQL,
		q.UUID, nullableTime(q.Start), nullableTime(q.End), q.Limit)
	if err != nil {
		return nil, fmt.Errorf("postgres: query telemetry: %w", err)
	}
	defer rows.Close()

	var records []telemetry.Record
	for rows.Next() {
		var (
			r      telemetry.Record
			labels []byte
		)
		if err := rows.Scan(
			&r.Timestamp, &r.MetricName, &r.GPUID, &r.Device, &r.UUID,
			&r.ModelName, &r.Hostname, &r.Container, &r.Pod, &r.Namespace,
			&r.Value, &labels,
		); err != nil {
			return nil, fmt.Errorf("postgres: scan telemetry: %w", err)
		}
		if len(labels) > 0 {
			if err := json.Unmarshal(labels, &r.Labels); err != nil {
				return nil, fmt.Errorf("postgres: unmarshal labels for %s: %w", r.UUID, err)
			}
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: query telemetry: %w", err)
	}
	return records, nil
}

// nullableTime maps the zero time to SQL NULL for unbounded window edges.
func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases the connection pool. ctx is accepted to satisfy the interface.
func (s *Store) Close(_ context.Context) error {
	s.pool.Close()
	return nil
}
