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

	"gpu-telemetry-pipeline/internal/config"
	grpcqueue "gpu-telemetry-pipeline/internal/queue/grpc"
	kafkaqueue "gpu-telemetry-pipeline/internal/queue/kafka"
	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/streamer"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "streamer")
	cfg := config.StreamerConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("streamer exited with error", "error", err)
		os.Exit(1)
	}
	log.Info("streamer exited cleanly")
}

func run(ctx context.Context, cfg config.Streamer, log *slog.Logger) error {
	records, err := streamer.Load(cfg.CSVPath)
	if err != nil {
		return err
	}
	log.Info("loaded telemetry dataset", "records", len(records), "path", cfg.CSVPath)

	var producer queue.Producer
	if cfg.QueueType == "grpc" {
		producer, err = grpcqueue.NewProducer(cfg.QueueAddr, cfg.KafkaTopic)
		if err != nil {
			return err
		}
		log.Info("connected to gRPC queue", "addr", cfg.QueueAddr, "topic", cfg.KafkaTopic)
	} else {
		producer, err = kafkaqueue.NewProducer(kafkaqueue.ProducerConfig{
			Brokers: cfg.KafkaBrokers,
			Topic:   cfg.KafkaTopic,
		})
		if err != nil {
			return err
		}
		log.Info("connected producer", "brokers", cfg.KafkaBrokers, "topic", cfg.KafkaTopic)
	}
	defer func() { _ = producer.Close() }()

	str := streamer.New(producer, records, streamer.Config{
		Interval:      cfg.Interval,
		Loop:          cfg.Loop,
		CheckpointDir: cfg.CheckpointDir,
	}, log)

	healthSrv := startHealthServer(cfg.HealthAddr, str.Stats(), log)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = healthSrv.Shutdown(shutdownCtx)
	}()

	return str.Run(ctx)
}

func startHealthServer(addr string, stats *streamer.Stats, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()

	ok := func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
	mux.HandleFunc("/healthz", ok)
	mux.HandleFunc("/readyz", ok)

	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]int64{
			"streamed":       stats.Streamed.Load(),
			"publish_errors": stats.PublishErrs.Load(),
			"loops":          stats.Loops.Load(),
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
