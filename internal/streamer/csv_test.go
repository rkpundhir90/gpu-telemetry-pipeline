package streamer

import (
	"strings"
	"testing"
)

const sampleCSV = `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-aaa","NVIDIA H100","host-1","","","","42","DCGM_FI_DRIVER_VERSION=""535.129.03"",gpu=""0"""
"2025-07-18T20:42:34Z","DCGM_FI_DEV_GPU_UTIL","1","nvidia1","GPU-bbb","NVIDIA H100","host-1","","","","100",""
`

func TestParseCSV(t *testing.T) {
	recs, err := parseCSV(strings.NewReader(sampleCSV))
	if err != nil {
		t.Fatalf("parseCSV: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}

	r := recs[0]
	if r.UUID != "GPU-aaa" || r.MetricName != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("unexpected identity fields: %+v", r)
	}
	if r.Value != 42 {
		t.Errorf("value = %v, want 42", r.Value)
	}
	if !r.Timestamp.IsZero() {
		t.Error("Timestamp should be left zero for the streamer to stamp")
	}
	if r.Labels["DCGM_FI_DRIVER_VERSION"] != "535.129.03" || r.Labels["gpu"] != "0" {
		t.Errorf("labels not parsed: %v", r.Labels)
	}
	if recs[1].Labels != nil {
		t.Errorf("empty labels_raw should yield nil labels, got %v", recs[1].Labels)
	}
}

func TestParseCSVSkipsUnusableRows(t *testing.T) {
	csv := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
"t","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","","model","h","","","","42",""
"t","","0","nvidia0","GPU-ccc","model","h","","","","42",""
"t","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-ddd","model","h","","","","not-a-number",""
"t","DCGM_FI_DEV_GPU_UTIL","0","nvidia0","GPU-eee","model","h","","","","7",""
`
	recs, err := parseCSV(strings.NewReader(csv))
	if err != nil {
		t.Fatalf("parseCSV: %v", err)
	}
	if len(recs) != 1 || recs[0].UUID != "GPU-eee" {
		t.Fatalf("expected only the one valid row, got %+v", recs)
	}
}

func TestParseCSVErrors(t *testing.T) {
	if _, err := parseCSV(strings.NewReader("metric_name,uuid\nx,y\n")); err == nil {
		t.Error("expected error for missing required column (value)")
	}
	if _, err := parseCSV(strings.NewReader("")); err == nil {
		t.Error("expected error for empty input")
	}
}
