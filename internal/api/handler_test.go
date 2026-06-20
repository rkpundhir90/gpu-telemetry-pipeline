package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"gpu-telemetry-pipeline/internal/store"
	"gpu-telemetry-pipeline/internal/telemetry"
)

// fakeReader is a programmable store.TelemetryReader for handler tests.
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

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestServer(r *fakeReader) http.Handler {
	return NewRouter(NewHandlers(r, quietLogger()), quietLogger())
}

func do(t *testing.T, h http.Handler, method, target string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestListGPUs(t *testing.T) {
	r := &fakeReader{gpus: []store.GPU{{UUID: "GPU-a", ModelName: "H100"}}}
	rec := do(t, newTestServer(r), http.MethodGet, "/api/v1/gpus")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got []store.GPU
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got) != 1 || got[0].UUID != "GPU-a" {
		t.Errorf("unexpected body: %s", rec.Body.String())
	}
}

func TestListGPUsEmptyIsArrayNotNull(t *testing.T) {
	rec := do(t, newTestServer(&fakeReader{}), http.MethodGet, "/api/v1/gpus")
	if body := rec.Body.String(); body != "[]" {
		t.Errorf("empty list body = %q, want []", body)
	}
}

func TestListGPUsStoreError(t *testing.T) {
	r := &fakeReader{listErr: errors.New("boom")}
	rec := do(t, newTestServer(r), http.MethodGet, "/api/v1/gpus")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestQueryTelemetryParsesWindowAndLimit(t *testing.T) {
	r := &fakeReader{records: []telemetry.Record{{UUID: "GPU-a", MetricName: "m", Value: 1}}}
	srv := newTestServer(r)
	rec := do(t, srv, http.MethodGet,
		"/api/v1/gpus/GPU-a/telemetry?start_time=2025-07-18T20:42:34Z&end_time=2025-07-18T21:00:00Z&limit=50")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	if r.lastQ.UUID != "GPU-a" {
		t.Errorf("uuid = %q, want GPU-a", r.lastQ.UUID)
	}
	if r.lastQ.Limit != 50 {
		t.Errorf("limit = %d, want 50", r.lastQ.Limit)
	}
	if r.lastQ.Start.IsZero() || r.lastQ.End.IsZero() {
		t.Errorf("expected bounded window, got %+v", r.lastQ)
	}
}

func TestQueryTelemetryDefaultsLimitAndUnboundedWindow(t *testing.T) {
	r := &fakeReader{}
	srv := newTestServer(r)
	rec := do(t, srv, http.MethodGet, "/api/v1/gpus/GPU-a/telemetry")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if r.lastQ.Limit != defaultQueryLimit {
		t.Errorf("limit = %d, want default %d", r.lastQ.Limit, defaultQueryLimit)
	}
	if !r.lastQ.Start.IsZero() || !r.lastQ.End.IsZero() {
		t.Errorf("expected unbounded window, got %+v", r.lastQ)
	}
}

func TestQueryTelemetryClampsLimit(t *testing.T) {
	r := &fakeReader{}
	rec := do(t, newTestServer(r), http.MethodGet, "/api/v1/gpus/GPU-a/telemetry?limit=999999")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if r.lastQ.Limit != maxQueryLimit {
		t.Errorf("limit = %d, want clamped %d", r.lastQ.Limit, maxQueryLimit)
	}
}

func TestQueryTelemetryRejectsBadParams(t *testing.T) {
	srv := newTestServer(&fakeReader{})
	for _, target := range []string{
		"/api/v1/gpus/GPU-a/telemetry?start_time=not-a-time",
		"/api/v1/gpus/GPU-a/telemetry?end_time=2025-13-99",
		"/api/v1/gpus/GPU-a/telemetry?limit=0",
		"/api/v1/gpus/GPU-a/telemetry?limit=-5",
		"/api/v1/gpus/GPU-a/telemetry?limit=abc",
	} {
		rec := do(t, srv, http.MethodGet, target)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", target, rec.Code)
		}
	}
}

func TestQueryTelemetryStoreError(t *testing.T) {
	r := &fakeReader{queryErr: errors.New("boom")}
	rec := do(t, newTestServer(r), http.MethodGet, "/api/v1/gpus/GPU-a/telemetry")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestReadiness(t *testing.T) {
	okRec := do(t, newTestServer(&fakeReader{}), http.MethodGet, "/readyz")
	if okRec.Code != http.StatusOK {
		t.Errorf("ready status = %d, want 200", okRec.Code)
	}
	downRec := do(t, newTestServer(&fakeReader{pingErr: errors.New("down")}), http.MethodGet, "/readyz")
	if downRec.Code != http.StatusServiceUnavailable {
		t.Errorf("unready status = %d, want 503", downRec.Code)
	}
}

func TestLiveness(t *testing.T) {
	rec := do(t, newTestServer(&fakeReader{}), http.MethodGet, "/healthz")
	if rec.Code != http.StatusOK {
		t.Errorf("health status = %d, want 200", rec.Code)
	}
}
