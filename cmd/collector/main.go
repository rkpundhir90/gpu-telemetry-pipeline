package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gpu-telemetry-pipeline/internal/collector"
	"gpu-telemetry-pipeline/internal/config"
	grpcqueue "gpu-telemetry-pipeline/internal/queue/grpc"
	kafkaqueue "gpu-telemetry-pipeline/internal/queue/kafka"
	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/store/postgres"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "collector")
	cfg := config.CollectorConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("collector exited with error", "error", err)
		os.Exit(1)
	}
	log.Info("collector exited cleanly")
}

func run(ctx context.Context, cfg config.Collector, log *slog.Logger) error {
	connectCtx, cancelConnect := context.WithTimeout(ctx, 30*time.Second)
	defer cancelConnect()

	st, err := postgres.New(connectCtx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = st.Close(closeCtx)
	}()

	if err := st.EnsureSchema(connectCtx); err != nil {
		if errors.Is(err, postgres.ErrHypertableUnavailable) {
			log.Warn("continuing without TimescaleDB hypertable", "error", err)
		} else {
			return err
		}
	} else {
		log.Info("schema ready (TimescaleDB hypertable)")
	}

	var consumer queue.Consumer
	if cfg.QueueType == "grpc" {
		consumer, err = grpcqueue.NewConsumer(cfg.QueueAddr, cfg.KafkaTopic, cfg.KafkaGroupID)
		if err != nil {
			return err
		}
		log.Info("connected to gRPC queue", "addr", cfg.QueueAddr, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
	} else {
		consumer, err = kafkaqueue.New(kafkaqueue.Config{
			Brokers: cfg.KafkaBrokers,
			Topic:   cfg.KafkaTopic,
			GroupID: cfg.KafkaGroupID,
		})
		if err != nil {
			return err
		}
		log.Info("joined kafka consumer group", "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
	}
	defer func() { _ = consumer.Close() }()

	coll := collector.New(consumer, st, collector.Config{
		BatchSize:     cfg.BatchSize,
		FlushInterval: cfg.FlushInterval,
		FlushTimeout:  cfg.FlushTimeout,
	}, log)

	healthSrv := startHealthServer(cfg.HealthAddr, st, coll.Stats(), log)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = healthSrv.Shutdown(shutdownCtx)
	}()

	return coll.Run(ctx)
}

func startHealthServer(addr string, st interface {
	Ping(context.Context) error
}, stats *collector.Stats, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := st.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"unavailable"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{
			"persisted":    stats.Persisted.Load(),
			"dropped":      stats.Dropped.Load(),
			"batches":      stats.Batches.Load(),
			"flush_errors": stats.FlushErrs.Load(),
		})
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("health server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server error", "error", err)
		}
	}()
	return srv
}
