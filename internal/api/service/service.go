// Package service holds the API's business logic, sitting between the HTTP
// handlers (presentation) and the store (repository). It is bundled with the API
// only — no other service depends on it.
//
// The layering is: handlers decode/encode HTTP and map errors to status codes;
// this service validates inputs, applies business policy (query limits, window
// rules), and orchestrates the store; the store executes data access. Keeping
// these concerns separate means the rules below are unit-testable without HTTP
// and reusable if the API grows more handlers.
package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gpu-telemetry-pipeline/internal/store"
	"gpu-telemetry-pipeline/internal/telemetry"
)

const (
	// DefaultQueryLimit caps a telemetry query when the caller gives no limit;
	// MaxQueryLimit is the ceiling a caller can request, so one request cannot
	// pull an unbounded series.
	DefaultQueryLimit = 1000
	MaxQueryLimit     = 10000
)

// ErrInvalidArgument wraps input that fails business validation. The API layer
// maps it to HTTP 400.
var ErrInvalidArgument = errors.New("invalid argument")

// TelemetryService is the API's business layer over the telemetry store.
type TelemetryService struct {
	store store.TelemetryReader
}

// New constructs a TelemetryService over a telemetry reader (the repository).
func New(reader store.TelemetryReader) *TelemetryService {
	return &TelemetryService{store: reader}
}

// Query is a validated telemetry lookup. A zero Start or End is unbounded on
// that side; a Limit of 0 means "use the default".
type Query struct {
	GPUID string
	Start time.Time
	End   time.Time
	Limit int
}

// ListGPUs returns every GPU that has reported telemetry.
func (s *TelemetryService) ListGPUs(ctx context.Context) ([]store.GPU, error) {
	return s.store.ListGPUs(ctx)
}

// QueryTelemetry validates and normalises the query, then reads the GPU's
// series from the store. It enforces that the GPU id is present, that the time
// window is ordered, and that the row limit is defaulted and capped.
func (s *TelemetryService) QueryTelemetry(ctx context.Context, q Query) ([]telemetry.Record, error) {
	if q.GPUID == "" {
		return nil, fmt.Errorf("%w: gpu id is required", ErrInvalidArgument)
	}
	if !q.Start.IsZero() && !q.End.IsZero() && q.End.Before(q.Start) {
		return nil, fmt.Errorf("%w: end_time must not be before start_time", ErrInvalidArgument)
	}

	return s.store.QueryTelemetry(ctx, store.TelemetryQuery{
		UUID:  q.GPUID,
		Start: q.Start,
		End:   q.End,
		Limit: normaliseLimit(q.Limit),
	})
}

// CheckReadiness reports whether the backing store is reachable.
func (s *TelemetryService) CheckReadiness(ctx context.Context) error {
	return s.store.Ping(ctx)
}

// normaliseLimit applies the default when unset and clamps to the maximum.
func normaliseLimit(limit int) int {
	if limit <= 0 {
		return DefaultQueryLimit
	}
	if limit > MaxQueryLimit {
		return MaxQueryLimit
	}
	return limit
}
