package ports

import (
	"context"

	"github.com/sulte4/marketflow/internal/core/domain"
)

type TickerCache interface {
	Set(ctx context.Context, ticker *domain.Ticker) error
	GetLatestByExchange(ctx context.Context, symbol, exchangeName string) (*domain.Ticker, error)
	GetLatest(ctx context.Context, symbol string) (*domain.Ticker, error)
	Ping(ctx context.Context) error
}

type TickerRepository interface {
	Save(ctx context.Context, ticker *domain.AggregatedTicker) error
	GetMaximum(ctx context.Context, f *domain.TickerFilter) (*domain.Ticker, error)
	GetMinimum(ctx context.Context, f *domain.TickerFilter) (*domain.Ticker, error)
	GetAverage(ctx context.Context, f *domain.TickerFilter) (*domain.Ticker, error)
	Ping(ctx context.Context) error
}

type ExchangeSource interface {
	Dial() error
	Stream(ctx context.Context, out chan<- domain.Ticker) error
	SourceName() string
	Close() error
}
