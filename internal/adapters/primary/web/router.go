package web

import "net/http"

func New(h *Handler) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /prices/latest/{symbol}", h.HandleLatestPrice)
	mux.HandleFunc("GET /prices/latest/{exchange}/{symbol}", h.HandleLatestPrice)

	mux.HandleFunc("GET /prices/highest/{symbol}", h.HandleHighestPrice)
	mux.HandleFunc("GET /prices/highest/{exchange}/{symbol}", h.HandleHighestPrice)

	mux.HandleFunc("GET /prices/lowest/{symbol}", h.HandleLowestPrice)
	mux.HandleFunc("GET /prices/lowest/{exchange}/{symbol}", h.HandleLowestPrice)

	mux.HandleFunc("GET /prices/average/{symbol}", h.HandleAveragePrice)
	mux.HandleFunc("GET /prices/average/{exchange}/{symbol}", h.HandleAveragePrice)

	mux.HandleFunc("POST /mode/test", h.HandleModeTest)
	mux.HandleFunc("POST /mode/live", h.HandleModeLive)
	return mux
}
