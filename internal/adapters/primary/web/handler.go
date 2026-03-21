package web

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/sulte4/marketflow/internal/core/domain"
	"github.com/sulte4/marketflow/internal/ports"
)

type Response struct {
	Data  any    `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

type Handler struct {
	marketService ports.MarketService
}

func NewHandler(marketservice ports.MarketService) *Handler {
	return &Handler{
		marketService: marketservice,
	}
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	slog.Info("health check requested",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.String("remote_addr", r.RemoteAddr))

	result := h.marketService.Health(r.Context())

	// 200 only when every component is healthy.
	// 503 for "degraded" (partial failure) and "down" (total failure) so that
	// load balancers and Kubernetes probes treat any non-OK state as unhealthy.
	statusCode := http.StatusOK
	if !result.IsHealthy() {
		statusCode = http.StatusServiceUnavailable
	}

	slog.Info("health check response",
		slog.Int("http_status", statusCode),
		slog.String("overall", result.Status),
		slog.String("postgres", result.Postgres.Status),
		slog.String("redis", result.Redis.Status))

	writeJSON(w, statusCode, result)
}

func (h *Handler) HandleLatestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	exchange := r.PathValue("exchange")

	// slog.Info("latest price request received",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.String("method", r.Method),
	// 	slog.String("path", r.URL.Path))

	ticker, err := h.marketService.GetLatestPrice(r.Context(), exchange, symbol)
	if err != nil {
		slog.Error("failed to get latest price",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.String("error", err.Error()))
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	// slog.Info("latest price request completed",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.Any("ticker", ticker))
	writeJSON(w, http.StatusOK, ticker)
}

func (h *Handler) HandleHighestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	exchange := r.PathValue("exchange")
	period := r.URL.Query().Get("period")

	slog.Info("highest price request received",
		slog.String("symbol", symbol),
		slog.String("exchange", exchange),
		slog.String("period", period),
		slog.String("method", r.Method))

	duration, _ := time.ParseDuration(period)

	ticker, err := h.marketService.GetHighestPrice(r.Context(), exchange, symbol, int64(duration.Seconds()))
	if err != nil {
		if errors.Is(err, domain.ErrNoData) {
			writeJSON(w, http.StatusOK, nil)
			return
		}
		slog.Error("failed to get highest price",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.Int64("period_seconds", int64(duration.Seconds())),
			slog.String("error", err.Error()))
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, ticker)
}

func (h *Handler) HandleLowestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	exchange := r.PathValue("exchange")
	period := r.URL.Query().Get("period")

	// slog.Info("lowest price request received",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.String("period", period),
	// 	slog.String("method", r.Method))

	duration, _ := time.ParseDuration(period)

	ticker, err := h.marketService.GetLowestPrice(r.Context(), exchange, symbol, int64(duration.Seconds()))
	if err != nil {
		if errors.Is(err, domain.ErrNoData) {
			writeJSON(w, http.StatusOK, nil)
			return
		}
		slog.Error("failed to get lowest price",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.Int64("period_seconds", int64(duration.Seconds())),
			slog.String("error", err.Error()))
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, ticker)
}

func (h *Handler) HandleAveragePrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	exchange := r.PathValue("exchange")
	period := r.URL.Query().Get("period")

	// slog.Info("average price request received",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.String("period", period),
	// 	slog.String("method", r.Method))

	duration, _ := time.ParseDuration(period)

	ticker, err := h.marketService.GetAveragePrice(r.Context(), exchange, symbol, int64(duration.Seconds()))
	if err != nil {
		if errors.Is(err, domain.ErrNoData) {
			writeJSON(w, http.StatusOK, nil)
			return
		}
		slog.Error("failed to get average price",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.Int64("period_seconds", int64(duration.Seconds())),
			slog.String("error", err.Error()))
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, ticker)
}

func (h *Handler) HandleModeTest(w http.ResponseWriter, r *http.Request) {
	// slog.Info("switching to test mode",
	// 	slog.String("method", r.Method),
	// 	slog.String("path", r.URL.Path),
	// 	slog.String("remote_addr", r.RemoteAddr))

	h.marketService.ModeTest()

	slog.Info("test mode activated")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "test mode activated"}`))
}

func (h *Handler) HandleModeLive(w http.ResponseWriter, r *http.Request) {
	// slog.Info("switching to live mode",
	// 	slog.String("method", r.Method),
	// 	slog.String("path", r.URL.Path),
	// 	slog.String("remote_addr", r.RemoteAddr))

	h.marketService.ModeLive()

	slog.Info("live mode activated")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "live mode activated"}`))
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(Response{Data: data})
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(Response{Error: message})
}
