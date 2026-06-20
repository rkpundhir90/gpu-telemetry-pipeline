package config

import (
	"strings"
	"time"
)

// API holds the API gateway's settings.
type API struct {
	ListenAddr string
}

func APIConfig() API {
	return API{
		ListenAddr: getenv("API_LISTEN_ADDR", ":8080"),
	}
}

// Collector holds the Telemetry Collector's settings, sourced from the
// environment so the same binary runs unchanged across local dev, Docker
// Compose, and Kubernetes (where Helm injects these as env vars).
type Collector struct {
	// Kafka consumer-group settings. Brokers is a comma-separated list. All
	// replicas share GroupID to form one competing-consumers group, which is how
	// scaling up/down redistributes partitions automatically.
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	// PostgreSQL/TimescaleDB connection string (pgx DSN or URL form).
	PostgresDSN string

	// Batching behaviour.
	BatchSize     int
	FlushInterval time.Duration
	FlushTimeout  time.Duration

	// HealthAddr is where the liveness/readiness HTTP server listens (for k8s
	// probes). Separate from the API gateway's port.
	HealthAddr string
}

func CollectorConfig() Collector {
	return Collector{
		KafkaBrokers: splitAndTrim(getenv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:   getenv("KAFKA_TOPIC", "gpu-telemetry"),
		KafkaGroupID: getenv("KAFKA_GROUP_ID", "telemetry-collectors"),

		PostgresDSN: getenv("POSTGRES_DSN",
			"postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable"),

		BatchSize:     getenvInt("COLLECTOR_BATCH_SIZE", 500),
		FlushInterval: getenvDuration("COLLECTOR_FLUSH_INTERVAL", time.Second),
		FlushTimeout:  getenvDuration("COLLECTOR_FLUSH_TIMEOUT", 15*time.Second),

		HealthAddr: getenv("COLLECTOR_HEALTH_ADDR", ":8081"),
	}
}

// splitAndTrim turns "a:1, b:2" into ["a:1","b:2"], dropping empty entries.
func splitAndTrim(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
