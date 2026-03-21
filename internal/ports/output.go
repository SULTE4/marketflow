package ports

import (
	"context"
	"time"

	"github.com/sulte4/marketflow/internal/core/domain"
)

type TickerCache interface {
	Set(ctx context.Context, ticker *domain.Ticker) error
	GetLatestByExchange(ctx context.Context, symbol, exchangeName string) (*domain.Ticker, error)
	GetLatest(ctx context.Context, symbol string) (*domain.Ticker, error)
	AddToTimeSeries(ctx context.Context, ticker *domain.Ticker) error
	GetTickersInRange(ctx context.Context, symbol, exchange string, sinceMs int64) ([]*domain.Ticker, error)
	Ping(ctx context.Context) error
}

type TickerRepository interface {
	Save(ctx context.Context, ticker *domain.AggregatedTicker) error
	GetMaximum(ctx context.Context, f *domain.TickerFilter) (*domain.Ticker, error)
	GetMinimum(ctx context.Context, f *domain.TickerFilter) (*domain.Ticker, error)
	GetAverage(ctx context.Context, f *domain.TickerFilter) (*domain.Ticker, error)
	// GetAverageWithCount returns the weighted average price, the total
	// effective tick count, AND the timestamp of the most-recently saved batch
	// row that matched the filter.
	//
	// The timestamp is used by the service layer as the lower-bound cutoff for
	// the Redis time-series query, ensuring that ticks already accounted for
	// inside a Postgres batch are never counted a second time when combining
	// the two stores:
	//
	//   redisSince = max(now - redisSec, latestBatchTs)
	//
	// When no rows match (ErrNoData), latestBatchTs is the zero time.Time.
	GetAverageWithCount(ctx context.Context, f *domain.TickerFilter) (avg float64, count int64, latestBatchTs time.Time, err error)
	Ping(ctx context.Context) error
}

type ExchangeSource interface {
	Dial() error
	Stream(ctx context.Context, out chan<- domain.Ticker) error
	SourceName() string
	Close() error
}
