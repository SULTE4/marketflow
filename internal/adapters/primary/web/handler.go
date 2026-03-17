package web

import (
	"encoding/json"
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
}

func (h *Handler) HandleLatestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	exchange := r.PathValue("exchange")
	ticker, err := h.marketService.GetLatestPrice(r.Context(), exchange, symbol)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	response := TickerResponse{Symbol: symbol, Source: exchange, Price: ticker.Price, Timestamp: time.UnixMilli(ticker.Timestamp)}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) HandleHighestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	exchange := r.PathValue("exchange")
	period := r.URL.Query().Get("period")
	duration, err := time.ParseDuration(period)
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrInvalidInput.Error())
	}
	price, err := h.marketService.GetHighestPrice(r.Context(), exchange, symbol, int64(duration.Seconds()))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	response := TickerResponse{Symbol: symbol, Source: exchange, Price: price}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) HandleLowestPrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	exchange := r.PathValue("exchange")
	period := r.URL.Query().Get("period")
	duration, err := time.ParseDuration(period)
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrInvalidInput.Error())
	}
	price, err := h.marketService.GetLowestPrice(r.Context(), exchange, symbol, int64(duration.Seconds()))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	response := TickerResponse{Symbol: symbol, Source: exchange, Price: price}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) HandleAveragePrice(w http.ResponseWriter, r *http.Request) {
	symbol := r.PathValue("symbol")
	exchange := r.PathValue("exchange")
	period := r.URL.Query().Get("period")
	duration, err := time.ParseDuration(period)
	if err != nil {
		writeError(w, http.StatusBadRequest, domain.ErrInvalidInput.Error())
	}

	price, err := h.marketService.GetAveragePrice(r.Context(), exchange, symbol, int64(duration.Seconds()))
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	response := TickerResponse{Symbol: symbol, Source: exchange, Price: price}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) HandleModeTest(w http.ResponseWriter, r *http.Request) {
	// Логика переключения на тестовый режим
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status": "test mode activated"}`))
}

func (h *Handler) HandleModeLive(w http.ResponseWriter, r *http.Request) {
	// Логика переключения на live режим
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
