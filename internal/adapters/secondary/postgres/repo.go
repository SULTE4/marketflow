package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/sulte4/marketflow/internal/core/domain"
	"github.com/sulte4/marketflow/internal/ports"
)

type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) ports.TickerRepository {
	return &Repository{
		db: db,
	}
}

// Save persists one completed 60-second aggregation window.
// The count field (number of raw ticks in the window) is now included so that
// GetAverageWithCount can produce a true weighted average across batches.
func (r *Repository) Save(ctx context.Context, ticker *domain.AggregatedTicker) error {
	start := time.Now()

	// slog.Info("saving aggregated ticker to database",
	// 	slog.String("symbol", ticker.Symbol),
	// 	slog.String("exchange", ticker.Source),
	// 	slog.Float64("avg_price", ticker.Avg),
	// 	slog.Float64("min_price", float64(ticker.Min)),
	// 	slog.Float64("max_price", float64(ticker.Max)),
	// 	slog.Int("count", ticker.Count))

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO aggregated_ticker
			(pair_name, exchange, timestamp, average_price, min_price, max_price, count)
		VALUES ($1, $2, NOW(), $3, $4, $5, $6)
	`, ticker.Symbol, ticker.Source, ticker.Avg, ticker.Min, ticker.Max, ticker.Count)
	if err != nil {
		slog.Error("failed to save aggregated ticker",
			slog.String("symbol", ticker.Symbol),
			slog.String("exchange", ticker.Source),
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(start)))
		return err
	}

	// slog.Info("aggregated ticker saved successfully",
	// 	slog.String("symbol", ticker.Symbol),
	// 	slog.String("exchange", ticker.Source),
	// 	slog.Duration("duration", time.Since(start)))

	return nil
}

// appendExchangeAndPeriod is a small helper that tacks the optional exchange
// and period filters onto an in-progress WHERE clause, returning the updated
// query string and args slice.  startIdx is the next available $N placeholder.
func appendExchangeAndPeriod(query string, args []any, source string, period int64) (string, []any) {
	if source != "" {
		query += fmt.Sprintf(" AND exchange = $%d", len(args)+1)
		args = append(args, source)
	}
	if period != 0 {
		query += fmt.Sprintf(" AND timestamp >= NOW() - ($%d * INTERVAL '1 second')", len(args)+1)
		args = append(args, period)
	}
	return query, args
}

func (r *Repository) GetMaximum(ctx context.Context, f *domain.TickerFilter) (*domain.Ticker, error) {
	start := time.Now()

	// slog.Info("querying maximum price",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Int64("period_seconds", f.Period))

	query := `SELECT max_price, timestamp FROM aggregated_ticker WHERE pair_name = $1`
	args := []any{f.Symbol}
	query, args = appendExchangeAndPeriod(query, args, f.Source, f.Period)
	query += " ORDER BY max_price DESC LIMIT 1"

	row := r.db.QueryRowContext(ctx, query, args...)

	var val sql.NullFloat64
	var ts time.Time
	if err := row.Scan(&val, &ts); err != nil {
		slog.Error("failed to query maximum price",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(start)))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrTickerNotFound
		}
		return nil, err
	}
	if !val.Valid {
		slog.Warn("no data found for maximum price query",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.Duration("duration", time.Since(start)))
		return nil, domain.ErrNoData
	}

	// slog.Info("maximum price retrieved successfully",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Float64("max_price", val.Float64),
	// 	slog.Duration("duration", time.Since(start)))

	return &domain.Ticker{
		Symbol:    f.Symbol,
		Source:    f.Source,
		Price:     float32(val.Float64),
		Timestamp: ts.Unix(),
	}, nil
}

func (r *Repository) GetMinimum(ctx context.Context, f *domain.TickerFilter) (*domain.Ticker, error) {
	start := time.Now()

	// slog.Info("querying minimum price",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Int64("period_seconds", f.Period))

	query := `SELECT min_price, timestamp FROM aggregated_ticker WHERE pair_name = $1`
	args := []any{f.Symbol}
	query, args = appendExchangeAndPeriod(query, args, f.Source, f.Period)
	query += " ORDER BY min_price ASC LIMIT 1"

	row := r.db.QueryRowContext(ctx, query, args...)

	var val sql.NullFloat64
	var ts time.Time
	if err := row.Scan(&val, &ts); err != nil {
		slog.Error("failed to query minimum price",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(start)))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrTickerNotFound
		}
		return nil, err
	}
	if !val.Valid {
		slog.Warn("no data found for minimum price query",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.Duration("duration", time.Since(start)))
		return nil, domain.ErrNoData
	}

	// slog.Info("minimum price retrieved successfully",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Float64("min_price", val.Float64),
	// 	slog.Duration("duration", time.Since(start)))

	return &domain.Ticker{
		Symbol:    f.Symbol,
		Source:    f.Source,
		Price:     float32(val.Float64),
		Timestamp: ts.Unix(),
	}, nil
}

// GetAverage returns the true weighted average price.
// It delegates to GetAverageWithCount and discards the count and timestamp.
//
// GREATEST(count, 1) in the underlying query ensures that legacy rows
// (inserted before migration 000002, which have count=0) are treated as if
// they contain exactly one tick, falling back to a simple unweighted average
// for those rows without discarding them.
func (r *Repository) GetAverage(ctx context.Context, f *domain.TickerFilter) (*domain.Ticker, error) {
	avg, _, _, err := r.GetAverageWithCount(ctx, f)
	if err != nil {
		return nil, err
	}
	return &domain.Ticker{
		Symbol: f.Symbol,
		Source: f.Source,
		Price:  float32(avg),
	}, nil
}

// GetAverageWithCount returns the weighted average price AND the total
// effective tick count for the matching rows.
//
// The count is needed by the service layer to correctly combine the Postgres
// result with a Redis sub-minute result:
//
//	combined = (pgAvg*pgCount + redisSum) / (pgCount + redisCount)
//
// GREATEST(count, 1) handles legacy rows (count = 0 before migration 000002):
//   - New rows  (count > 0): true weighted average; each row weighted by its
//     actual tick count.
//   - Legacy rows (count = 0): treated as weight 1, effectively giving an
//     unweighted average for those rows — the best approximation without
//     stored counts.
func (r *Repository) GetAverageWithCount(ctx context.Context, f *domain.TickerFilter) (avg float64, count int64, latestBatchTs time.Time, err error) {
	start := time.Now()

	// slog.Info("querying weighted average price",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Int64("period_seconds", f.Period))

	// The aggregate query always returns exactly one row (even if no data
	// matches), so sql.ErrNoRows is not possible here.  We detect "no data"
	// by checking whether the result columns are NULL / zero.
	query := `
		SELECT
			SUM(average_price * GREATEST(count, 1))::FLOAT8
				/ NULLIF(SUM(GREATEST(count, 1)), 0),
			COALESCE(SUM(GREATEST(count, 1)), 0)::BIGINT,
			MAX(timestamp)
		FROM aggregated_ticker
		WHERE pair_name = $1`
	args := []any{f.Symbol}
	query, args = appendExchangeAndPeriod(query, args, f.Source, f.Period)

	row := r.db.QueryRowContext(ctx, query, args...)

	var avgVal sql.NullFloat64
	var cntVal sql.NullInt64
	var tsVal sql.NullTime
	if scanErr := row.Scan(&avgVal, &cntVal, &tsVal); scanErr != nil {
		slog.Error("failed to query weighted average price",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.String("error", scanErr.Error()),
			slog.Duration("duration", time.Since(start)))
		if errors.Is(scanErr, sql.ErrNoRows) {
			return 0, 0, time.Time{}, domain.ErrTickerNotFound
		}
		return 0, 0, time.Time{}, scanErr
	}

	// avgVal is NULL when there are no matching rows (SUM of empty set = NULL).
	// cntVal is 0 when COALESCE(NULL, 0) fires, i.e., also no matching rows.
	if !avgVal.Valid || cntVal.Int64 == 0 {
		slog.Warn("no data found for average price query",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.Duration("duration", time.Since(start)))
		return 0, 0, time.Time{}, domain.ErrNoData
	}

	// slog.Info("weighted average price retrieved successfully",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Float64("avg_price", avgVal.Float64),
	// 	slog.Int64("total_count", cntVal.Int64),
	// 	slog.Time("latest_batch_ts", tsVal.Time),
	// 	slog.Duration("duration", time.Since(start)))

	return avgVal.Float64, cntVal.Int64, tsVal.Time, nil
}

func (r *Repository) Ping(ctx context.Context) error {
	slog.Info("pinging PostgreSQL database")

	if err := r.db.PingContext(ctx); err != nil {
		slog.Error("PostgreSQL ping failed", slog.String("error", err.Error()))
		return err
	}

	slog.Info("PostgreSQL ping successful")
	return nil
}
