package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	swaggerfiles "github.com/swaggo/files"
	ginswagger "github.com/swaggo/gin-swagger"

	"gpu-telemetry-pipeline/internal/api"
	"gpu-telemetry-pipeline/internal/api/service"
	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/store/postgres"
)

const openAPISpecPath = "api/openapi/swagger.json"

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "api")
	cfg := config.APIConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Error("api exited with error", "error", err)
		os.Exit(1)
	}
	log.Info("api gateway exited cleanly")
}

func run(ctx context.Context, cfg config.API, log *slog.Logger) error {
	// Bound the initial connect so a misconfigured DSN fails fast instead of
	// hanging the pod's startup.
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
	log.Info("connected to telemetry store")

	svc := service.New(st)
	handlers := api.NewHandlers(svc, log)
	router := api.NewRouter(handlers, log)

	if _, err := os.Stat(openAPISpecPath); err == nil {
		router.StaticFile("/openapi.json", filepath.Clean(openAPISpecPath))
		router.GET("/swagger/*any", ginswagger.WrapHandler(
			swaggerfiles.Handler,
			ginswagger.URL("/openapi.json"),
		))
		log.Info("serving OpenAPI spec", "path", openAPISpecPath)
	} else {
		log.Warn("OpenAPI spec not found; run `make openapi` to generate it", "expected", openAPISpecPath)
	}

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		log.Info("api gateway listening", "addr", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return err
	case <-ctx.Done():
		log.Info("api gateway shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return httpServer.Shutdown(shutdownCtx)
}
