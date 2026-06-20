package config

import (
	"testing"
	"time"
)

func TestSplitAndTrim(t *testing.T) {
	got := splitAndTrim(" a:1 , b:2,, c:3 ")
	want := []string{"a:1", "b:2", "c:3"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestCollectorConfigDefaults(t *testing.T) {
	// With no env set, defaults apply. (Tests run with a clean env per package.)
	cfg := CollectorConfig()
	if cfg.KafkaTopic != "gpu-telemetry" {
		t.Errorf("KafkaTopic = %q, want gpu-telemetry", cfg.KafkaTopic)
	}
	if cfg.BatchSize != 500 {
		t.Errorf("BatchSize = %d, want 500", cfg.BatchSize)
	}
	if cfg.FlushInterval != time.Second {
		t.Errorf("FlushInterval = %v, want 1s", cfg.FlushInterval)
	}
	if len(cfg.KafkaBrokers) == 0 {
		t.Error("KafkaBrokers should have a default")
	}
}

func TestGetenvDuration(t *testing.T) {
	t.Setenv("TEST_DUR", "250ms")
	if got := getenvDuration("TEST_DUR", time.Second); got != 250*time.Millisecond {
		t.Errorf("got %v, want 250ms", got)
	}
	if got := getenvDuration("TEST_DUR_MISSING", time.Second); got != time.Second {
		t.Errorf("fallback: got %v, want 1s", got)
	}
	t.Setenv("TEST_DUR_BAD", "not-a-duration")
	if got := getenvDuration("TEST_DUR_BAD", time.Second); got != time.Second {
		t.Errorf("bad value should fall back: got %v", got)
	}
}
