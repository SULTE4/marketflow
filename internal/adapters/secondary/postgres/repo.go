package postgres

import (
	"context"
	"database/sql"
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

func (r *Repository) Save(ctx context.Context, ticker *domain.AggregatedTicker) error {
	start := time.Now()

	// slog.Info("saving aggregated ticker to database",
	// 	slog.String("symbol", ticker.Symbol),
	// 	slog.String("exchange", ticker.Source),
	// 	slog.Float64("avg_price", ticker.Avg),
	// 	slog.Float64("min_price", float64(ticker.Min)),
	// 	slog.Float64("max_price", float64(ticker.Max)))

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO aggregated_ticker (pair_name, exchange, timestamp, average_price, min_price, max_price)
		VALUES ($1, $2, NOW(), $3, $4, $5)
	`, ticker.Symbol, ticker.Source, ticker.Avg, ticker.Min, ticker.Max)
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

func (r *Repository) GetMaximum(ctx context.Context, f *domain.TickerFilter) (float32, error) {
	start := time.Now()

	// slog.Info("querying maximum price",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Int64("period_seconds", f.Period))

	query := `SELECT MAX(max_price) from
			(SELECT max_price from aggregated_ticker
			WHERE pair_name = $1`

	args := []interface{}{f.Symbol}

	if f.Source != "" {
		query += " AND exchange = $2"
		args = append(args, f.Source)
	}

	if f.Period != 0 {
		query += fmt.Sprintf(" AND timestamp >= NOW() - ($%d * INTERVAL '1 second')", len(args)+1)
		args = append(args, f.Period)
	}

	query += " );"

	row := r.db.QueryRowContext(ctx, query, args...)
	var val sql.NullFloat64
	if err := row.Scan(&val); err != nil {
		slog.Error("failed to query maximum price",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(start)))
		return 0, err
	}
	if !val.Valid {
		slog.Warn("no data found for maximum price query",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.Duration("duration", time.Since(start)))
		return 0, domain.ErrNoData
	}

	// slog.Info("maximum price retrieved successfully",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Float64("max_price", val.Float64),
	// 	slog.Duration("duration", time.Since(start)))

	return float32(val.Float64), nil
}

func (r *Repository) GetMinimum(ctx context.Context, f *domain.TickerFilter) (float32, error) {
	start := time.Now()

	// slog.Info("querying minimum price",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Int64("period_seconds", f.Period))

	query := `SELECT MIN(min_price) from
			(SELECT min_price from aggregated_ticker
			WHERE pair_name = $1`

	args := []interface{}{f.Symbol}

	if f.Source != "" {
		query += " AND exchange = $2"
		args = append(args, f.Source)
	}

	if f.Period != 0 {
		query += fmt.Sprintf(" AND timestamp >= NOW() - ($%d * INTERVAL '1 second')", len(args)+1)
		args = append(args, f.Period)
	}

	query += " );"

	row := r.db.QueryRowContext(ctx, query, args...)
	var val sql.NullFloat64
	if err := row.Scan(&val); err != nil {
		slog.Error("failed to query minimum price",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(start)))
		return 0, err
	}
	if !val.Valid {
		slog.Warn("no data found for minimum price query",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.Duration("duration", time.Since(start)))
		return 0, domain.ErrNoData
	}

	// slog.Info("minimum price retrieved successfully",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Float64("min_price", val.Float64),
	// 	slog.Duration("duration", time.Since(start)))

	return float32(val.Float64), nil
}

func (r *Repository) GetAverage(ctx context.Context, f *domain.TickerFilter) (float32, error) {
	start := time.Now()

	// slog.Info("querying average price",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Int64("period_seconds", f.Period))

	query := `SELECT AVG(average_price) from
			(SELECT average_price from aggregated_ticker
			WHERE pair_name = $1`

	args := []interface{}{f.Symbol}

	if f.Source != "" {
		query += " AND exchange = $2"
		args = append(args, f.Source)
	}

	if f.Period != 0 {
		query += fmt.Sprintf(" AND timestamp >= NOW() - ($%d * INTERVAL '1 second')", len(args)+1)
		args = append(args, f.Period)
	}

	query += " );"

	row := r.db.QueryRowContext(ctx, query, args...)
	var val sql.NullFloat64
	if err := row.Scan(&val); err != nil {
		slog.Error("failed to query average price",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.String("error", err.Error()),
			slog.Duration("duration", time.Since(start)))
		return 0, err
	}
	if !val.Valid {
		slog.Warn("no data found for average price query",
			slog.String("symbol", f.Symbol),
			slog.String("exchange", f.Source),
			slog.Duration("duration", time.Since(start)))
		return 0, domain.ErrNoData
	}

	// slog.Info("average price retrieved successfully",
	// 	slog.String("symbol", f.Symbol),
	// 	slog.String("exchange", f.Source),
	// 	slog.Float64("avg_price", val.Float64),
	// 	slog.Duration("duration", time.Since(start)))

	return float32(val.Float64), nil
}

func (r *Repository) Ping(ctx context.Context) error {
	slog.Info("pinging PostgreSQL database")

	err := r.db.PingContext(ctx)
	if err != nil {
		slog.Error("PostgreSQL ping failed", slog.String("error", err.Error()))
		return err
	}

	slog.Info("PostgreSQL ping successful")
	return nil
}
