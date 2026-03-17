package service

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"

	"github.com/sulte4/marketflow/internal/core/domain"
	"github.com/sulte4/marketflow/internal/ports"
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

func (s *Service) Health(ctx context.Context) error {
	// Проверка коннекта к базе/биржам
	return nil
}

func (s *Service) GetLatestPrice(ctx context.Context, exchange, symbol string) (*domain.Ticker, error) {
	if exchange == "" {
		ticker, err := s.cache.GetLatest(ctx, symbol)
		if err != nil {
			return nil, err
		}
		return ticker, nil
	}
	ticker, err := s.cache.GetLatestByExchange(ctx, symbol, exchange)
	if err != nil {
		return nil, err
	}
	return ticker, nil
}

func (s *Service) GetHighestPrice(ctx context.Context, exchange, symbol string, period int64) (float32, error) {
	f := &domain.TickerFilter{
		Symbol: symbol,
		Source: exchange,
		Period: period,
	}
	price, err := s.repo.GetMaximum(ctx, f)
	if err != nil {
		slog.Error(err.Error())
		if errors.Is(err, sql.ErrNoRows) {
			return 0, domain.ErrTickerNotFound
		} else if errors.Is(err, domain.ErrNoData) {
			return 0, domain.ErrNoData
		} else {
			return 0, domain.ErrInternalError
		}
	}
	return price, nil
}

func (s *Service) GetLowestPrice(ctx context.Context, exchange, symbol string, period int64) (float32, error) {
	f := &domain.TickerFilter{
		Symbol: symbol,
		Source: exchange,
		Period: period,
	}
	price, err := s.repo.GetMinimum(ctx, f)
	if err != nil {
		slog.Error(err.Error())
		if errors.Is(err, sql.ErrNoRows) {
			return 0, domain.ErrTickerNotFound
		} else if errors.Is(err, domain.ErrNoData) {
			return 0, domain.ErrNoData
		} else {
			return 0, domain.ErrInternalError
		}
	}
	return price, nil
}

func (s *Service) GetAveragePrice(ctx context.Context, exchange, symbol string, period int64) (float32, error) {
	f := &domain.TickerFilter{
		Symbol: symbol,
		Source: exchange,
		Period: period,
	}
	price, err := s.repo.GetAverage(ctx, f)
	if err != nil {
		slog.Error(err.Error())
		if errors.Is(err, sql.ErrNoRows) {
			return 0, domain.ErrTickerNotFound
		} else if errors.Is(err, domain.ErrNoData) {
			return 0, domain.ErrNoData
		} else {
			return 0, domain.ErrInternalError
		}
	}
	return price, nil
}

func (s *Service) ModeLive() {
}

func (s *Service) ModeTest() {
}
