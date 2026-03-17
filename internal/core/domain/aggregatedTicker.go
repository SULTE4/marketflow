package domain

import (
	"fmt"
	"math"
)

type AggregatedTicker struct {
	Symbol string
	Source string
	Count  int
	Max    float32
	Min    float32
	Sum    float64
	Avg    float64
}

func NewAggregatedTicker() *AggregatedTicker {
	return &AggregatedTicker{
		Symbol: "-",
		Min:    math.MaxFloat32,
		Max:    -math.MaxFloat32,
		Sum:    0,
		Count:  0,
		Source: "-",
	}
}

func (v *AggregatedTicker) Calculation(exchange Ticker) {
	v.Count++
	v.Min = min(v.Min, exchange.Price)
	v.Max = max(v.Max, exchange.Price)
	v.Sum += float64(exchange.Price)
}

func (v *AggregatedTicker) Reset() {
	v.Min = math.MaxFloat32
	v.Max = -math.MaxFloat32
	v.Count = 0
	v.Sum = 0
}

func (v *AggregatedTicker) ToString() string {
	return fmt.Sprintf("Pair: %s |  Source: %s | Count: %d | Min: %.2f | Max: %.2f | Sum: %.2f",
		v.Symbol, v.Source, v.Count, v.Min, v.Max, v.Sum)
}
