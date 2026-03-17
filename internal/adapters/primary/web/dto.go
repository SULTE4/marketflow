package web

import "time"

type TickerResponse struct {
	Symbol    string    `json:"symbol"`
	Source    string    `json:"source,omitempty"`
	Price     float32   `json:"price"`
	Timestamp time.Time `json:"timestamp,omitzero"`
}
