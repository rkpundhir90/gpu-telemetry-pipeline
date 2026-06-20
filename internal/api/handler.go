package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"gpu-telemetry-pipeline/internal/api/service"
	"gpu-telemetry-pipeline/internal/store"
	"gpu-telemetry-pipeline/internal/telemetry"
)

// Handlers are the HTTP presentation layer: they decode requests, delegate to
// the service (business layer), and map results/errors to HTTP responses. They
// hold no business logic and never touch the store directly.
type Handlers struct {
	svc *service.TelemetryService
	log *slog.Logger
}

// NewHandlers wires the handlers to the telemetry service.
func NewHandlers(svc *service.TelemetryService, log *slog.Logger) *Handlers {
	if log == nil {
		log = slog.Default()
	}
	return &Handlers{svc: svc, log: log}
}

// ErrorResponse is the JSON body returned for 4xx/5xx responses.
type ErrorResponse struct {
	Error string `json:"error"`
}

// ListGPUs godoc
//
//	@Summary		List all GPUs
//	@Description	Returns every GPU for which telemetry data has been collected.
//	@Tags			gpus
//	@Produce		json
//	@Success		200	{array}		store.GPU
//	@Failure		500	{object}	api.ErrorResponse
//	@Router			/api/v1/gpus [get]
func (h *Handlers) ListGPUs(c *gin.Context) {
	gpus, err := h.svc.ListGPUs(c.Request.Context())
	if err != nil {
		h.log.Error("list gpus failed", "error", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to list gpus"})
		return
	}
	if gpus == nil {
		gpus = []store.GPU{}
	}
	c.JSON(http.StatusOK, gpus)
}

// QueryTelemetry godoc
//
//	@Summary		Query telemetry by GPU
//	@Description	Returns telemetry for a specific GPU ordered by time (newest first), with optional inclusive time-window filters.
//	@Tags			telemetry
//	@Produce		json
//	@Param			id			path		string	true	"GPU UUID"
//	@Param			start_time	query		string	false	"Inclusive lower bound (RFC3339, e.g. 2025-07-18T20:42:34Z)"
//	@Param			end_time	query		string	false	"Inclusive upper bound (RFC3339)"
//	@Param			limit		query		int		false	"Max rows to return (default 1000, max 10000)"
//	@Success		200			{array}		telemetry.Record
//	@Failure		400			{object}	api.ErrorResponse
//	@Failure		500			{object}	api.ErrorResponse
//	@Router			/api/v1/gpus/{id}/telemetry [get]
func (h *Handlers) QueryTelemetry(c *gin.Context) {
	start, err := parseTime(c.Query("start_time"))
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "start_time must be RFC3339"})
		return
	}
	end, err := parseTime(c.Query("end_time"))
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "end_time must be RFC3339"})
		return
	}
	limit, err := parseLimit(c.Query("limit"))
	if err != nil {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "limit must be a positive integer"})
		return
	}

	records, err := h.svc.QueryTelemetry(c.Request.Context(), service.Query{
		GPUID: c.Param("id"),
		Start: start,
		End:   end,
		Limit: limit,
	})
	if err != nil {
		if errors.Is(err, service.ErrInvalidArgument) {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
			return
		}
		h.log.Error("query telemetry failed", "error", err, "uuid", c.Param("id"))
		c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "failed to query telemetry"})
		return
	}
	if records == nil {
		records = []telemetry.Record{}
	}
	c.JSON(http.StatusOK, records)
}

// Health godoc
//
//	@Summary	Liveness probe
//	@Tags		system
//	@Produce	json
//	@Success	200	{object}	map[string]string
//	@Router		/healthz [get]
func (h *Handlers) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// Ready godoc
//
//	@Summary	Readiness probe (checks datastore connectivity)
//	@Tags		system
//	@Produce	json
//	@Success	200	{object}	map[string]string
//	@Failure	503	{object}	api.ErrorResponse
//	@Router		/readyz [get]
func (h *Handlers) Ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if err := h.svc.CheckReadiness(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, ErrorResponse{Error: "datastore unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ready"})
}

// parseTime parses an optional RFC3339 timestamp; an empty string yields the
// zero time (an unbounded window edge).
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339, s)
}

// parseLimit parses an optional row limit. Absent yields 0 (the service applies
// its default); a present value must be a positive integer.
func parseLimit(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, errors.New("invalid limit")
	}
	return n, nil
}
