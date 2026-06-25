package config

import (
	"strings"
	"time"
)

// API holds the API gateway's settings.
type API struct {
	ListenAddr  string
	PostgresDSN string
}

func APIConfig() API {
	return API{
		ListenAddr: getenv("API_LISTEN_ADDR", ":8080"),
		PostgresDSN: getenv("POSTGRES_DSN",
			"postgres://telemetry:telemetry@localhost:5432/telemetry?sslmode=disable"),
	}
}

// Collector holds the Telemetry Collector's settings.
type Collector struct {
	// QueueType selects the queue implementation: "kafka" or "grpc".
	QueueType    string
	QueueAddr    string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	PostgresDSN string

	BatchSize     int
	FlushInterval time.Duration
	FlushTimeout  time.Duration

	HealthAddr string
}

func CollectorConfig() Collector {
	return Collector{
		QueueType:    getenv("QUEUE_TYPE", "kafka"),
		QueueAddr:    getenv("QUEUE_ADDR", "localhost:50051"),
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

// Streamer holds the Telemetry Streamer's settings.
type Streamer struct {
	// QueueType selects the queue implementation: "kafka" or "grpc".
	QueueType    string
	QueueAddr    string
	KafkaBrokers []string
	KafkaTopic   string

	// CSVPath is the telemetry source file, read at runtime (PV mount in k8s).
	CSVPath string

	Interval time.Duration
	Loop     bool

	HealthAddr string
}

func StreamerConfig() Streamer {
	return Streamer{
		QueueType:    getenv("QUEUE_TYPE", "kafka"),
		QueueAddr:    getenv("QUEUE_ADDR", "localhost:50051"),
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
