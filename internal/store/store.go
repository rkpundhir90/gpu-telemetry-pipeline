// Package store defines the persistence abstraction for telemetry, decoupling
// the Collector (and later the API) from any concrete database.
//
// MongoDB is the first implementation (see store/mongo), but the Collector only
// depends on TelemetryStore, so an alternative time-series backend can be added
// without changing collector logic.
package store

import (
	"context"

	"gpu-telemetry-pipeline/internal/telemetry"
)

// TelemetryStore persists telemetry records and reports connectivity.
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
