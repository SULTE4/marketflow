package service

import (
	"context"
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
	slog.Info("performing health check")

	if err := s.repo.Ping(ctx); err != nil {
		slog.Error("repository health check failed", slog.String("error", err.Error()))
		return err
	}

	if err := s.cache.Ping(ctx); err != nil {
		slog.Error("cache health check failed", slog.String("error", err.Error()))
		return err
	}

	slog.Info("health check passed")
	return nil
}

func (s *Service) GetLatestPrice(ctx context.Context, exchange, symbol string) (*domain.Ticker, error) {
	slog.Info("getting latest price",
		slog.String("symbol", symbol),
		slog.String("exchange", exchange))

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

	slog.Info("latest price by exchange retrieved successfully",
		slog.String("symbol", symbol),
		slog.String("exchange", exchange),
		slog.Float64("price", float64(ticker.Price)))
	return ticker, nil
}

func (s *Service) GetHighestPrice(ctx context.Context, exchange, symbol string, period int64) (*domain.Ticker, error) {
	f := &domain.TickerFilter{
		Symbol: symbol,
		Source: exchange,
		Period: period,
	}
	ticker, err := s.repo.GetMaximum(ctx, f)
	if err != nil {
		slog.Error("failed to get highest price",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.Int64("period", period),
			slog.String("error", err.Error()))
		if errors.Is(err, domain.ErrTickerNotFound) {
			return nil, domain.ErrTickerNotFound
		} else if errors.Is(err, domain.ErrNoData) {
			return nil, domain.ErrNoData
		} else {
			return nil, domain.ErrInternalError
		}
	}
	slog.Info("retrieved highest price",
		slog.String("symbol", symbol),
		slog.String("exchange", exchange),
		slog.Any("ticker", ticker),
	)
	return ticker, nil
}

func (s *Service) GetLowestPrice(ctx context.Context, exchange, symbol string, period int64) (*domain.Ticker, error) {
	f := &domain.TickerFilter{
		Symbol: symbol,
		Source: exchange,
		Period: period,
	}
	ticker, err := s.repo.GetMinimum(ctx, f)
	if err != nil {
		slog.Error("failed to get lowest price",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.Int64("period", period),
			slog.String("error", err.Error()))
		if errors.Is(err, domain.ErrTickerNotFound) {
			return nil, domain.ErrTickerNotFound
		} else if errors.Is(err, domain.ErrNoData) {
			return nil, domain.ErrNoData
		} else {
			return nil, domain.ErrInternalError
		}
	}
	slog.Info("retrieved lowest price",
		slog.String("symbol", symbol),
		slog.String("exchange", exchange),
		slog.Any("ticker", ticker))
	return ticker, nil
}

func (s *Service) GetAveragePrice(ctx context.Context, exchange, symbol string, period int64) (*domain.Ticker, error) {
	f := &domain.TickerFilter{
		Symbol: symbol,
		Source: exchange,
		Period: period,
	}
	ticker, err := s.repo.GetAverage(ctx, f)
	if err != nil {
		slog.Error("failed to get average price",
			slog.String("symbol", symbol),
			slog.String("exchange", exchange),
			slog.Int64("period", period),
			slog.String("error", err.Error()))
		if errors.Is(err, domain.ErrTickerNotFound) {
			return nil, domain.ErrTickerNotFound
		} else if errors.Is(err, domain.ErrNoData) {
			return nil, domain.ErrNoData
		} else {
			return nil, domain.ErrInternalError
		}
	}
	slog.Info("retrieved average price",
		slog.String("symbol", symbol),
		slog.String("exchange", exchange),
		slog.Any("ticker", ticker))
	return ticker, nil
}

func (s *Service) ModeLive() {
	slog.Info("activating live mode")
}

func (s *Service) ModeTest() {
	slog.Info("activating test mode")
}
