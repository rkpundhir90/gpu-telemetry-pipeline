package kafka

import "testing"

func TestNewProducerValidatesConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  ProducerConfig
	}{
		{"no brokers", ProducerConfig{Topic: "t"}},
		{"no topic", ProducerConfig{Brokers: []string{"b:9092"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewProducer(tc.cfg); err == nil {
				t.Fatal("expected error for invalid config")
			}
		})
	}
}

func TestNewProducerAppliesDefaults(t *testing.T) {
	p, err := NewProducer(ProducerConfig{Brokers: []string{"localhost:9092"}, Topic: "t"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil || p.writer == nil {
		t.Fatal("expected a constructed producer with a writer")
	}
	// NewProducer must not dial; closing should be clean.
	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
