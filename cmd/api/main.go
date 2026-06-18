package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	swaggerfiles "github.com/swaggo/files"
	ginswagger "github.com/swaggo/gin-swagger"

	"github.com/rkpundhir90/gpu-telemetry-pipeline/internal/api"
	"github.com/rkpundhir90/gpu-telemetry-pipeline/internal/config"
)

const openAPISpecPath = "api/openapi/swagger.json"

func main() {
	log := observability.NewLogger("api")
    cfg := config.APIConfig()
	handlers := api.NewHandlers(repo)
	router := api.NewRouter(handlers, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	go func() {
		log.Info("api gateway listening", "addr", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("api server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("api gateway shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}
}