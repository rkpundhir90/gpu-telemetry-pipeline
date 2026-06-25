package api

import (
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "gpu-telemetry-pipeline/docs" // generated OpenAPI spec (run `make openapi`)
)

// NewRouter wires the Gin engine with middleware and routes.
//
// @title		GPU Telemetry Pipeline API
// @version		1.0
// @description	REST API exposing GPU telemetry collected from an elastic streaming pipeline.
// @BasePath	/
func NewRouter(h *Handlers, log *slog.Logger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), structuredLogger(log))

	r.GET("/healthz", h.Health)
	r.GET("/readyz", h.Ready)

	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))

	v1 := r.Group("/api/v1")
	{
		v1.GET("/gpus", h.ListGPUs)
		v1.GET("/gpus/:id/telemetry", h.QueryTelemetry)
	}

	return r
}

// structuredLogger emits one slog access-log line per request.
func structuredLogger(log *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("http request",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"latency_ms", time.Since(start).Milliseconds(),
			"client_ip", c.ClientIP(),
		)
	}
}