package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Handlers struct {
}

func NewHandlers() *Handlers {
	return &Handlers{}
}

type ErrorResponse struct {
	Error string `json:"error"`
}

// ListGPUs godoc
//
//	@Summary		List all GPUs
//	@Description	Returns every GPU for which telemetry data has been collected.
//	@Tags			gpus
//	@Produce		json
//	@Success		200	{string}		string
//	@Failure		500	{object}	api.ErrorResponse
//	@Router			/api/v1/gpus [get]
func (h *Handlers) ListGPUs(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, ErrorResponse{Error: "not implemented"})
}

// QueryTelemetry godoc
//
//	@Summary		Query telemetry by GPU
//	@Description	Returns telemetry for a specific GPU ordered by time, with optional inclusive time-window filters.
//	@Tags			telemetry
//	@Produce		json
//	@Param			id			path		string	true	"GPU UUID"
//	@Param			start_time	query		string	false	"Inclusive lower bound (RFC3339, e.g. 2025-07-18T20:42:34Z)"
//	@Param			end_time	query		string	false	"Inclusive upper bound (RFC3339)"
//	@Param			limit		query		int		false	"Max rows to return"
//	@Success		200			{string}		string
//	@Failure		400			{object}	api.ErrorResponse
//	@Failure		500			{object}	api.ErrorResponse
//	@Router			/api/v1/gpus/{id}/telemetry [get]
func (h *Handlers) QueryTelemetry(c *gin.Context) {
	id := c.Param("id")
	if id == "" {
		c.JSON(http.StatusBadRequest, ErrorResponse{Error: "gpu id is required"})
		return
	}

	c.JSON(http.StatusNotImplemented, ErrorResponse{Error: "not implemented"})
}

// Health godoc
//
//	@Summary	Liveness/readiness probe
//	@Tags		system
//	@Produce	json
//	@Success	200	{object}	map[string]string
//	@Router		/healthz [get]
func (h *Handlers) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
