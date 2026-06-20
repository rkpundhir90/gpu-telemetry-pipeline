package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/store"
	"gpu-telemetry-pipeline/internal/telemetry"
)

// fakeReader is a programmable store.TelemetryReader for service tests.
type fakeReader struct {
	gpus     []store.GPU
	records  []telemetry.Record
	lastQ    store.TelemetryQuery
	listErr  error
	queryErr error
	pingErr  error
}

func (f *fakeReader) ListGPUs(context.Context) ([]store.GPU, error) { return f.gpus, f.listErr }

func (f *fakeReader) QueryTelemetry(_ context.Context, q store.TelemetryQuery) ([]telemetry.Record, error) {
	f.lastQ = q
	return f.records, f.queryErr
}

func (f *fakeReader) Ping(context.Context) error { return f.pingErr }

func TestQueryTelemetryRequiresGPUID(t *testing.T) {
	svc := New(&fakeReader{})
	_, err := svc.QueryTelemetry(context.Background(), Query{GPUID: ""})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestQueryTelemetryRejectsReversedWindow(t *testing.T) {
	svc := New(&fakeReader{})
	start := time.Date(2025, 7, 18, 21, 0, 0, 0, time.UTC)
	end := start.Add(-time.Hour)
	_, err := svc.QueryTelemetry(context.Background(), Query{GPUID: "GPU-a", Start: start, End: end})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestQueryTelemetryDefaultsAndClampsLimit(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int
	}{
		{"unset uses default", 0, DefaultQueryLimit},
		{"in range passes through", 250, 250},
		{"over max clamps", MaxQueryLimit + 5000, MaxQueryLimit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeReader{}
			svc := New(r)
			if _, err := svc.QueryTelemetry(context.Background(), Query{GPUID: "GPU-a", Limit: tc.in}); err != nil {
				t.Fatalf("QueryTelemetry: %v", err)
			}
			if r.lastQ.Limit != tc.want {
				t.Errorf("limit = %d, want %d", r.lastQ.Limit, tc.want)
			}
		})
	}
}

func TestQueryTelemetryPassesWindowThrough(t *testing.T) {
	r := &fakeReader{}
	svc := New(r)
	start := time.Date(2025, 7, 18, 20, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	if _, err := svc.QueryTelemetry(context.Background(), Query{GPUID: "GPU-a", Start: start, End: end, Limit: 10}); err != nil {
		t.Fatalf("QueryTelemetry: %v", err)
	}
	if r.lastQ.UUID != "GPU-a" || !r.lastQ.Start.Equal(start) || !r.lastQ.End.Equal(end) {
		t.Errorf("query not forwarded correctly: %+v", r.lastQ)
	}
}

func TestQueryTelemetryPropagatesStoreError(t *testing.T) {
	svc := New(&fakeReader{queryErr: errors.New("boom")})
	_, err := svc.QueryTelemetry(context.Background(), Query{GPUID: "GPU-a"})
	if err == nil || errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want a non-validation store error", err)
	}
}

func TestListGPUsAndReadinessDelegate(t *testing.T) {
	r := &fakeReader{gpus: []store.GPU{{UUID: "GPU-a"}}}
	svc := New(r)
	gpus, err := svc.ListGPUs(context.Background())
	if err != nil || len(gpus) != 1 {
		t.Fatalf("ListGPUs = %v, %v", gpus, err)
	}
	if err := svc.CheckReadiness(context.Background()); err != nil {
		t.Errorf("CheckReadiness = %v, want nil", err)
	}
	down := New(&fakeReader{pingErr: errors.New("down")})
	if err := down.CheckReadiness(context.Background()); err == nil {
		t.Error("CheckReadiness should surface a ping error")
	}
}
