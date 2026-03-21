package service

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/sulte4/marketflow/internal/core/domain"
	"github.com/sulte4/marketflow/internal/ports"
)

const (
	batchSec    int64 = 60
	redisTTLSec int64 = 70 // maximum Redis ZSET coverage window
)

type Service struct {
	repo  ports.TickerRepository
	cache ports.TickerCache
	exch  []ports.ExchangeSource
}

func New(repo ports.TickerRepository, cache ports.TickerCache, exch []ports.ExchangeSource) ports.MarketService {
	return &Service{
		repo: repo, cache: cache, exch: exch,
	}
}

// Health checks PostgreSQL and Redis independently and always returns a
// *domain.HealthResult describing the status of each component.
//
// Critically, both components are checked even when the first one fails, so
// the caller always receives the full picture.  The overall Status field is:
//   - "ok"       – every component is healthy
//   - "degraded" – at least one component is down but not all
//   - "down"     – every component is down
func (s *Service) Health(ctx context.Context) *domain.HealthResult {
	slog.Info("performing health check")

	result := &domain.HealthResult{}
	downCount := 0

	// ── PostgreSQL ────────────────────────────────────────────────────────────
	if err := s.repo.Ping(ctx); err != nil {
		slog.Error("postgres health check failed", slog.String("error", err.Error()))
		result.Postgres = domain.ComponentHealth{
			Status:  domain.StatusDown,
			Message: err.Error(),
		}
		downCount++
	} else {
		slog.Info("postgres health check passed")
		result.Postgres = domain.ComponentHealth{Status: domain.StatusOK}
	}

	// ── Redis ─────────────────────────────────────────────────────────────────
	if err := s.cache.Ping(ctx); err != nil {
		slog.Error("redis health check failed", slog.String("error", err.Error()))
		result.Redis = domain.ComponentHealth{
			Status:  domain.StatusDown,
			Message: err.Error(),
		}
		downCount++
	} else {
		slog.Info("redis health check passed")
		result.Redis = domain.ComponentHealth{Status: domain.StatusOK}
	}

	// ── Overall status ────────────────────────────────────────────────────────
	switch downCount {
	case 0:
		result.Status = domain.StatusOK
	case 2:
		result.Status = domain.StatusDown
	default:
		result.Status = domain.StatusDegraded
	}

	slog.Info("health check completed", slog.String("status", result.Status))
	return result
}

func (s *Service) GetLatestPrice(ctx context.Context, exchange, symbol string) (*domain.Ticker, error) {
	// slog.Info("getting latest price",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange))

	if exchange == "" {
		ticker, err := s.cache.GetLatest(ctx, symbol)
		if err != nil {
			slog.Error("failed to get latest price from cache",
				slog.String("symbol", symbol),
				slog.String("error", err.Error()))
			return nil, err
		}

		slog.Info("latest price retrieved successfully",
			slog.String("symbol", symbol),
			slog.String("exchange", ticker.Source),
			slog.Float64("price", float64(ticker.Price)))
		return ticker, nil
	}

	ticker, err := s.cache.GetLatestByExchange(ctx, symbol, exchange)
	if err != nil {
		slog.Error("failed to get latest price by exchange from cache",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.String("error", err.Error()))
		return nil, err
	}

	// slog.Info("latest price by exchange retrieved successfully",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.Float64("price", float64(ticker.Price)))
	return ticker, nil
}

// splitPeriod divides a query window (in seconds) between the two stores.
// Rule: completed minutes → Postgres (aggregated batch rows)
//
//	      leftover seconds  → Redis   (raw ZSET ticks)
//
//		period = 0   → (pgSec=0, redisSec=70)  all-time Postgres + Redis current batch
//		period = 5   → (pgSec=0, redisSec=5)   Redis only
//		period = 30  → (pgSec=0, redisSec=30)  Redis only
//		period = 60  → (pgSec=60, redisSec=0)  Postgres only
//		period = 90  → (pgSec=60, redisSec=30) HYBRID
//		period = 120 → (pgSec=120, redisSec=0) Postgres only
//		period = 150 → (pgSec=120, redisSec=30) HYBRID
func splitPeriod(period int64) (pgSec, redisSec int64) {
	if period == 0 {
		return 0, redisTTLSec
	}
	return (period / batchSec) * batchSec, period % batchSec
}

func (s *Service) GetHighestPrice(ctx context.Context, exchange, symbol string, period int64) (*domain.Ticker, error) {
	// slog.Info("getting highest price",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.Int64("period_sec", period))

	pgSec, redisSec := splitPeriod(period)

	var pgResult *domain.Ticker
	if pgSec > 0 || period == 0 {
		f := &domain.TickerFilter{Symbol: symbol, Source: exchange, Period: pgSec}
		r, err := s.repo.GetMaximum(ctx, f)
		if err != nil && !isNoData(err) {
			slog.Error("postgres GetMaximum failed",
				slog.String("symbol", symbol),
				slog.String("exchange", exchange),
				slog.Int64("pg_period_sec", pgSec),
				slog.String("error", err.Error()))
			return nil, domain.ErrInternalError
		}
		pgResult = r
	}

	var redisResult *domain.Ticker
	if redisSec > 0 || period == 0 {
		sinceMs := time.Now().Add(-time.Duration(redisSec) * time.Second).UnixMilli()
		tickers, err := s.cache.GetTickersInRange(ctx, symbol, exchange, sinceMs)
		if err != nil {
			slog.Error("redis GetTickersInRange failed",
				slog.String("symbol", symbol),
				slog.String("exchange", exchange),
				slog.Int64("redis_period_sec", redisSec),
				slog.String("error", err.Error()))
			// Non-fatal: fall back to Postgres-only result.
		} else if len(tickers) > 0 {
			redisResult = maxTicker(tickers)
		}
	}

	result := pickHigher(pgResult, redisResult)
	if result == nil {
		slog.Info("no highest price data found",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.Int64("period_sec", period))
		return nil, domain.ErrNoData
	}

	// slog.Info("highest price retrieved",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.Int64("period_sec", period),
	// 	slog.Float64("price", float64(result.Price)))
	return result, nil
}

func (s *Service) GetLowestPrice(ctx context.Context, exchange, symbol string, period int64) (*domain.Ticker, error) {
	// slog.Info("getting lowest price",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.Int64("period_sec", period))

	pgSec, redisSec := splitPeriod(period)

	var pgResult *domain.Ticker
	if pgSec > 0 || period == 0 {
		f := &domain.TickerFilter{Symbol: symbol, Source: exchange, Period: pgSec}
		r, err := s.repo.GetMinimum(ctx, f)
		if err != nil && !isNoData(err) {
			slog.Error("postgres GetMinimum failed",
				slog.String("symbol", symbol),
				slog.String("exchange", exchange),
				slog.Int64("pg_period_sec", pgSec),
				slog.String("error", err.Error()))
			return nil, domain.ErrInternalError
		}
		pgResult = r
	}

	var redisResult *domain.Ticker
	if redisSec > 0 || period == 0 {
		sinceMs := time.Now().Add(-time.Duration(redisSec) * time.Second).UnixMilli()
		tickers, err := s.cache.GetTickersInRange(ctx, symbol, exchange, sinceMs)
		if err != nil {
			slog.Error("redis GetTickersInRange failed",
				slog.String("symbol", symbol),
				slog.String("exchange", exchange),
				slog.Int64("redis_period_sec", redisSec),
				slog.String("error", err.Error()))
			// Non-fatal: fall back to Postgres-only result.
		} else if len(tickers) > 0 {
			redisResult = minTicker(tickers)
		}
	}

	result := pickLower(pgResult, redisResult)
	if result == nil {
		// slog.Info("no lowest price data found",
		// 	slog.String("symbol", symbol),
		// 	slog.String("exchange", exchange),
		// 	slog.Int64("period_sec", period))
		return nil, domain.ErrNoData
	}

	// slog.Info("lowest price retrieved",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.Int64("period_sec", period),
	// 	slog.Float64("price", float64(result.Price)))
	return result, nil
}

// GetAveragePrice returns a true tick-count-weighted average across both stores.
//
// Combination formula:
//
//	combined_avg = (pgAvg * pgCount + redisSum) / (pgCount + redisCount)
//
// Overlap elimination:
//
// The Redis ZSET stores every individual tick for the last 70 seconds,
// including ticks that were already flushed into the most-recent Postgres
// batch.  If we naively used "now - redisSec" as the Redis lower bound, the
// overlap window (ticks that appear in both the Postgres batch AND the ZSET)
// would be counted twice, biasing the average.
//
// To eliminate the overlap we use latestBatchTs — the MAX(timestamp) of the
// Postgres batch rows that matched the query — as an additional lower bound:
//
//	redisCutoff = max(now - redisSec, latestBatchTs)
//
// Because the worker only calls batched.Reset() after a successful Postgres
// INSERT, latestBatchTs is the moment the last batch was committed.  Ticks
// pushed to the ZSET before that moment are already counted inside the batch;
// ticks pushed after it are not yet in any Postgres row.  Using latestBatchTs
// as the cutoff therefore gives us exactly the non-overlapping remainder.
//
// Edge cases:
//   - No Postgres rows (fresh start / very short period): latestBatchTs is
//     the zero time, so we fall back to "now - redisSec" unchanged.
//   - latestBatchTs older than "now - redisSec" (batch saved long ago):
//     "now - redisSec" is more recent, so we use that — same as before.
func (s *Service) GetAveragePrice(ctx context.Context, exchange, symbol string, period int64) (*domain.Ticker, error) {
	// slog.Info("getting average price",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.Int64("period_sec", period))

	pgSec, redisSec := splitPeriod(period)

	// ── Postgres: completed 60-second batch rows ──────────────────────────────
	var pgAvg float64
	var pgCount int64
	var latestBatchTs time.Time // zero value = no Postgres rows matched
	if pgSec > 0 || period == 0 {
		f := &domain.TickerFilter{Symbol: symbol, Source: exchange, Period: pgSec}
		avg, cnt, batchTs, err := s.repo.GetAverageWithCount(ctx, f)
		if err != nil && !isNoData(err) {
			slog.Error("postgres GetAverageWithCount failed",
				slog.String("symbol", symbol),
				slog.String("exchange", exchange),
				slog.Int64("pg_period_sec", pgSec),
				slog.String("error", err.Error()))
			return nil, domain.ErrInternalError
		}
		pgAvg, pgCount, latestBatchTs = avg, cnt, batchTs
	}

	// ── Redis: sub-minute live ticks (overlap-free) ───────────────────────────
	var redisSum float64
	var redisCount int64
	if redisSec > 0 || period == 0 {
		// Start with the plain sliding-window cutoff.
		nowMinusRedisSec := time.Now().Add(-time.Duration(redisSec) * time.Second)
		cutoff := nowMinusRedisSec

		// If the last Postgres batch was committed MORE recently than the
		// sliding-window cutoff, advance the cutoff to the batch timestamp.
		// This skips all ticks that are already counted inside that batch,
		// eliminating the double-counting overlap entirely.
		if !latestBatchTs.IsZero() && latestBatchTs.After(nowMinusRedisSec) {
			cutoff = latestBatchTs
		}

		sinceMs := cutoff.UnixMilli()
		slog.Info("querying redis time-series for average",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.Int64("redis_period_sec", redisSec),
			slog.Time("cutoff", cutoff),
			slog.Bool("cutoff_from_batch_ts", !latestBatchTs.IsZero() && latestBatchTs.After(nowMinusRedisSec)))

		tickers, err := s.cache.GetTickersInRange(ctx, symbol, exchange, sinceMs)
		if err != nil {
			slog.Error("redis GetTickersInRange failed",
				slog.String("symbol", symbol),
				slog.String("exchange", exchange),
				slog.String("error", err.Error()))
			// Non-fatal: fall back to Postgres-only result.
		} else {
			for _, t := range tickers {
				redisSum += float64(t.Price)
				redisCount++
			}
		}
	}

	// ── Weighted combination ──────────────────────────────────────────────────
	totalCount := pgCount + redisCount
	if totalCount == 0 {
		slog.Info("no average price data found",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.Int64("period_sec", period))
		return nil, domain.ErrNoData
	}

	combinedAvg := (pgAvg*float64(pgCount) + redisSum) / float64(totalCount)

	// slog.Info("average price retrieved",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchange),
	// 	slog.Int64("period_sec", period),
	// 	slog.Float64("combined_avg", combinedAvg),
	// 	slog.Int64("pg_count", pgCount),
	// 	slog.Int64("redis_count", redisCount))

	return &domain.Ticker{
		Symbol:    symbol,
		Source:    exchange,
		Price:     float32(combinedAvg),
		Timestamp: time.Now().Unix(),
	}, nil
}

func (s *Service) ModeLive() {
	slog.Info("activating live mode")
}

func (s *Service) ModeTest() {
	slog.Info("activating test mode")
}

func maxTicker(tickers []*domain.Ticker) *domain.Ticker {
	var result *domain.Ticker
	for _, t := range tickers {
		if t == nil {
			continue
		}
		if result == nil || t.Price > result.Price {
			result = t
		}
	}
	return result
}

func minTicker(tickers []*domain.Ticker) *domain.Ticker {
	var result *domain.Ticker
	for _, t := range tickers {
		if t == nil {
			continue
		}
		if result == nil || t.Price < result.Price {
			result = t
		}
	}
	return result
}

func pickHigher(a, b *domain.Ticker) *domain.Ticker {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.Price >= b.Price {
		return a
	}
	return b
}

func pickLower(a, b *domain.Ticker) *domain.Ticker {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if a.Price <= b.Price {
		return a
	}
	return b
}

func isNoData(err error) bool {
	return errors.Is(err, domain.ErrNoData) || errors.Is(err, domain.ErrTickerNotFound)
}
