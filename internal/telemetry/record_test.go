package telemetry

import (
	"errors"
	"testing"
	"time"
)

func sampleRecord() Record {
	return Record{
		Timestamp:  time.Date(2025, 7, 18, 20, 42, 34, 0, time.UTC),
		MetricName: "DCGM_FI_DEV_GPU_UTIL",
		GPUID:      "1",
		Device:     "nvidia1",
		UUID:       "GPU-bc7a12ab-4998-fdc5-0785-2678a929a142",
		ModelName:  "NVIDIA H100 80GB HBM3",
		Hostname:   "mtv5-dgx1-hgpu-031",
		Value:      100,
		Labels:     map[string]string{"gpu": "1", "job": "dgx_dcgm_exporter"},
	}
}

func TestRecordRoundTrip(t *testing.T) {
	in := sampleRecord()
	b, err := in.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	out, err := Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !out.Timestamp.Equal(in.Timestamp) {
		t.Errorf("timestamp: got %v want %v", out.Timestamp, in.Timestamp)
	}
	if out.UUID != in.UUID || out.MetricName != in.MetricName || out.Value != in.Value {
		t.Errorf("core fields mismatch: %+v vs %+v", out, in)
	}
	if out.Labels["job"] != "dgx_dcgm_exporter" {
		t.Errorf("labels not preserved: %+v", out.Labels)
	}
}

func TestUnmarshalInvalidJSON(t *testing.T) {
	if _, err := Unmarshal([]byte("{not json")); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Record)
		wantErr bool
	}{
		{"valid", func(*Record) {}, false},
		{"missing uuid", func(r *Record) { r.UUID = "" }, true},
		{"missing metric", func(r *Record) { r.MetricName = "" }, true},
		{"zero timestamp", func(r *Record) { r.Timestamp = time.Time{} }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := sampleRecord()
			tc.mutate(&r)
			err := r.Validate()
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if !errors.Is(err, ErrInvalidRecord) {
					t.Errorf("error should wrap ErrInvalidRecord, got %v", err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
