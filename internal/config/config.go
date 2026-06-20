package config

import (
	"strings"
	"time"
)

// API holds the API gateway's settings.
type API struct {
	ListenAddr string

	// PostgresDSN is the TimescaleDB connection string the API reads telemetry
	// from (the same store the Collector writes to).
	PostgresDSN string
}

func APIConfig() API {
	return API{
		ListenAddr: getenv("API_LISTEN_ADDR", ":8080"),
		PostgresDSN: getenv("POSTGRES_DSN",
			"postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable"),
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

// Streamer holds the Telemetry Streamer's settings. Like the Collector, it is
// sourced from the environment so the same binary runs unchanged across local
// dev, Docker Compose, and Kubernetes.
type Streamer struct {
	// Kafka producer settings. Brokers is a comma-separated list. The topic is
	// the same one the Collector consumes from.
	KafkaBrokers []string
	KafkaTopic   string

	// CSVPath is the telemetry source file, read at runtime. In Kubernetes it
	// points at a file mounted from a PersistentVolume; required (the Streamer
	// has no built-in dataset).
	CSVPath string

	// Interval is the per-replica delay between datapoints; Loop replays the
	// dataset endlessly to simulate a continuous stream.
	Interval time.Duration
	Loop     bool

	// HealthAddr is where the liveness/readiness HTTP server listens (for k8s
	// probes).
	HealthAddr string
}

func StreamerConfig() Streamer {
	return Streamer{
		KafkaBrokers: splitAndTrim(getenv("KAFKA_BROKERS", "localhost:9092")),
		KafkaTopic:   getenv("KAFKA_TOPIC", "gpu-telemetry"),

		CSVPath:  getenv("STREAMER_CSV_PATH", ""),
		Interval: getenvDuration("STREAMER_INTERVAL", 10*time.Millisecond),
		Loop:     getenvBool("STREAMER_LOOP", true),

		HealthAddr: getenv("STREAMER_HEALTH_ADDR", ":8082"),
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
