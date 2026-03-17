package ports

import (
	"context"

	"github.com/sulte4/marketflow/internal/core/domain"
)

type MarketService interface {
	Health(ctx context.Context) error
	GetLatestPrice(ctx context.Context, exchange, symbol string) (*domain.Ticker, error)
	GetHighestPrice(ctx context.Context, exchange, symbol string, period int64) (float32, error)
	GetLowestPrice(ctx context.Context, exchange, symbol string, period int64) (float32, error)
	GetAveragePrice(ctx context.Context, exchange, symbol string, period int64) (float32, error)
	ModeLive()
	ModeTest()
}
