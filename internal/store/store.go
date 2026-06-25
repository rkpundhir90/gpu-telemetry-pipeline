// Package store defines the persistence abstraction for telemetry.
package store

import (
	"context"
	"time"

	"gpu-telemetry-pipeline/internal/telemetry"
)

// TelemetryStore is the write side, used by the Collector. Insert must be
// idempotent: at-least-once delivery means the same record may arrive more
// than once after a crash/redelivery.
type TelemetryStore interface {
	// Insert writes a batch of records. On error no offsets advance; the batch is
	// redelivered and idempotently retried.
	Insert(ctx context.Context, records []telemetry.Record) error
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}

// GPU summarises one GPU that has reported telemetry.
type GPU struct {
	UUID      string    `json:"uuid"`
	ModelName string    `json:"model_name,omitempty"`
	Hostname  string    `json:"hostname,omitempty"`
	LastSeen  time.Time `json:"last_seen"`
}

// TelemetryQuery bounds a telemetry read. Zero Start/End is unbounded.
type TelemetryQuery struct {
	UUID  string
	Start time.Time
	End   time.Time
	Limit int
}

// TelemetryReader is the read side, used by the API. Kept separate from
// TelemetryStore so the API does not depend on the Collector's write surface.
type TelemetryReader interface {
	ListGPUs(ctx context.Context) ([]GPU, error)
	QueryTelemetry(ctx context.Context, q TelemetryQuery) ([]telemetry.Record, error)
	Ping(ctx context.Context) error
}
