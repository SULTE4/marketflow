package domain

import "fmt"

type Ticker struct {
	Symbol    string  `json:"Symbol"`
	Source    string  `json:"Source"`
	Price     float32 `json:"price"`
	Timestamp int64   `json:"timestamp"`
}

type TickerFilter struct {
	Symbol string
	Source string
	Period int64
}

func (t *Ticker) ToString() string {
	return fmt.Sprintf("Symbol: %s | Source: %s | Price: %.2f | Timestamp: %d",
		t.Symbol, t.Source, t.Price, t.Timestamp)
}
