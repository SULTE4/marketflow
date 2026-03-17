package postgres

import (
	"context"
	"database/sql"
	"fmt"

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
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO aggregated_ticker (pair_name, exchange, timestamp, average_price, min_price, max_price)
		VALUES ($1, $2, NOW(), $3, $4, $5)
	`, ticker.Symbol, ticker.Source, ticker.Avg, ticker.Min, ticker.Max)

	return err
}

func (r *Repository) GetMaximum(ctx context.Context, f *domain.TickerFilter) (float32, error) {
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
		return 0, err
	}
	if !val.Valid {
		return 0, domain.ErrNoData
	}
	return float32(val.Float64), nil
}

func (r *Repository) GetMinimum(ctx context.Context, f *domain.TickerFilter) (float32, error) {
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
		return 0, err
	}
	if !val.Valid {
		return 0, domain.ErrNoData
	}
	return float32(val.Float64), nil
}

func (r *Repository) GetAverage(ctx context.Context, f *domain.TickerFilter) (float32, error) {
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
		return 0, err
	}
	if !val.Valid {
		return 0, domain.ErrNoData
	}
	return float32(val.Float64), nil
}

func (r *Repository) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}
