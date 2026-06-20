package kafka

import "testing"

func TestNewValidatesConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"no brokers", Config{Topic: "t", GroupID: "g"}},
		{"no topic", Config{Brokers: []string{"b:9092"}, GroupID: "g"}},
		{"no group", Config{Brokers: []string{"b:9092"}, Topic: "t"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); err == nil {
				t.Fatal("expected error for invalid config")
			}
		})
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	c, err := New(Config{Brokers: []string{"localhost:9092"}, Topic: "t", GroupID: "g"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil || c.reader == nil {
		t.Fatal("expected a constructed consumer with a reader")
	}
	// New must not dial; closing should be clean.
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
