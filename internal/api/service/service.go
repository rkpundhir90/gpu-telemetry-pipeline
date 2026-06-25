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
	DefaultQueryLimit = 1000
	MaxQueryLimit     = 10000
)

// ErrInvalidArgument wraps input that fails business validation; mapped to HTTP 400.
var ErrInvalidArgument = errors.New("invalid argument")

// TelemetryService is the API's business layer over the telemetry store.
type TelemetryService struct {
	store store.TelemetryReader
}

func New(reader store.TelemetryReader) *TelemetryService {
	return &TelemetryService{store: reader}
}

// Query is a validated telemetry lookup. Zero Start/End means unbounded; zero Limit uses the default.
type Query struct {
	GPUID string
	Start time.Time
	End   time.Time
	Limit int
}

func (s *TelemetryService) ListGPUs(ctx context.Context) ([]store.GPU, error) {
	return s.store.ListGPUs(ctx)
}

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

func (s *TelemetryService) CheckReadiness(ctx context.Context) error {
	return s.store.Ping(ctx)
}

func normaliseLimit(limit int) int {
	if limit <= 0 {
		return DefaultQueryLimit
	}
	if limit > MaxQueryLimit {
		return MaxQueryLimit
	}
	return limit
}
