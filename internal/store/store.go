// Package store defines the persistence abstraction for telemetry, decoupling
// the Collector (and later the API) from any concrete database.
//
// MongoDB is the first implementation (see store/mongo), but the Collector only
// depends on TelemetryStore, so an alternative time-series backend can be added
// without changing collector logic.
package store

import (
	"context"
	"time"

	"gpu-telemetry-pipeline/internal/telemetry"
)

// TelemetryStore persists telemetry records and reports connectivity. It is the
// write side, consumed by the Collector.
//
// Insert must be idempotent: the Collector provides at-least-once delivery, so
// the same record may be presented more than once after a crash/redelivery, and
// the store is responsible for not creating duplicates.
type TelemetryStore interface {
	// Insert durably writes a batch of records. A batch is the unit the
	// Collector commits queue offsets against, so a non-nil error means none of
	// the batch should be considered persisted (the offsets will not advance and
	// the records will be redelivered).
	Insert(ctx context.Context, records []telemetry.Record) error

	// Ping verifies the backend is reachable; used by readiness checks.
	Ping(ctx context.Context) error

	// Close releases the backend connection.
	Close(ctx context.Context) error
}

// GPU summarises one GPU that has reported telemetry, for the API's "list GPUs"
// endpoint.
type GPU struct {
	UUID      string    `json:"uuid"`
	ModelName string    `json:"model_name,omitempty"`
	Hostname  string    `json:"hostname,omitempty"`
	LastSeen  time.Time `json:"last_seen"`
}

// TelemetryQuery bounds a telemetry read. A zero Start or End is unbounded on
// that side; Limit caps the number of rows returned.
type TelemetryQuery struct {
	UUID  string
	Start time.Time
	End   time.Time
	Limit int
}

// TelemetryReader is the read side of the store, consumed by the API. It is kept
// separate from TelemetryStore so the API depends only on the queries it needs,
// not on the Collector's write surface.
type TelemetryReader interface {
	// ListGPUs returns one summary per GPU that has reported telemetry, ordered
	// by UUID.
	ListGPUs(ctx context.Context) ([]GPU, error)

	// QueryTelemetry returns a GPU's datapoints ordered by time (newest first),
	// filtered to the query's optional time window and capped at its Limit.
	QueryTelemetry(ctx context.Context, q TelemetryQuery) ([]telemetry.Record, error)

	// Ping verifies the backend is reachable; used by readiness checks.
	Ping(ctx context.Context) error
}
