package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sulte4/marketflow/internal/core/domain"
	"github.com/sulte4/marketflow/internal/ports"
)

type MarketProcessor struct {
	sources []ports.ExchangeSource
	cache   ports.TickerCache
	repo    ports.TickerRepository
}

func NewMarketProcessor(sources []ports.ExchangeSource, cache ports.TickerCache, repo ports.TickerRepository) *MarketProcessor {
	return &MarketProcessor{
		sources: sources,
		cache:   cache,
		repo:    repo,
	}
}

func (mp *MarketProcessor) Start(ctx context.Context) {
	slog.Info("starting market processor",
		slog.Int("num_sources", len(mp.sources)))

	workersResults := make([]chan domain.AggregatedTicker, 3)
	for index, exchange := range mp.sources {
		err := exchange.Dial()
		if err != nil {
			slog.Error("connection failed to source",
				slog.Int("source_index", index),
				slog.String("error", err.Error()))
			continue
		}

		in := make(chan domain.Ticker)
		go func(exch ports.ExchangeSource) {
			slog.Info("starting stream goroutine",
				slog.String("exchange", exch.SourceName()))
			if err := exch.Stream(ctx, in); err != nil {
				slog.Error("stream ended with error",
					slog.String("exchange", exch.SourceName()),
					slog.String("error", err.Error()))
			}
		}(exchange)

		workerResult := mp.fanOut(ctx, in, exchange.SourceName())
		workersResults[index] = workerResult
	}

	slog.Info("all exchange sources connected and streaming")

	batchedResult := mp.fanIn(ctx, workersResults)
	for val := range batchedResult {
		fmt.Println(val)
	}

	slog.Info("market processor stopped")
}

func (mp *MarketProcessor) fanOut(ctx context.Context, in <-chan domain.Ticker, exchName string) chan domain.AggregatedTicker {
	slog.Info("starting fan-out for exchange",
		slog.String("exchange", exchName))

	workers := make(map[string]chan domain.Ticker)
	workersResult := make(chan domain.AggregatedTicker)

	go func() {
		defer close(workersResult)

		for ticker := range in {
			ch, exists := workers[ticker.Symbol]
			if !exists {
				ch = make(chan domain.Ticker)
				workers[ticker.Symbol] = ch
				slog.Info("spawning new worker for symbol",
					slog.String("symbol", ticker.Symbol),
					slog.String("exchange", exchName),
					slog.Int("total_workers", len(workers)))
				go mp.worker(ctx, ch, workersResult, exchName)
			}
			select {
			case <-ctx.Done():
				slog.Info("fan-out context cancelled",
					slog.String("exchange", exchName))
				return
			case ch <- ticker:
			}
		}

		slog.Info("fan-out closing all workers",
			slog.String("exchange", exchName),
			slog.Int("num_workers", len(workers)))

		for symbol, ch := range workers {
			slog.Info("closing worker channel",
				slog.String("symbol", symbol),
				slog.String("exchange", exchName))
			close(ch)
		}
	}()

	return workersResult
}

func (mp *MarketProcessor) worker(ctx context.Context, in <-chan domain.Ticker, out chan<- domain.AggregatedTicker, exchName string) {
	batched := domain.NewAggregatedTicker()
	ticker := time.NewTicker(time.Second * 60)
	defer close(out)
	defer ticker.Stop()

	var symbol string

	for {
		select {
		case pair, ok := <-in:
			if !ok {
				slog.Info("worker channel closed, stopping worker",
					slog.String("symbol", symbol),
					slog.String("exchange", exchName))
				return
			}

			if symbol == "" {
				symbol = pair.Symbol
				slog.Info("worker started for symbol",
					slog.String("symbol", symbol),
					slog.String("exchange", exchName))
			}

			batched.Calculation(pair)
			batched.Symbol = pair.Symbol
			batched.Source = exchName

			err := mp.cache.Set(ctx, &pair)
			if err != nil {
				slog.Error("failed to set ticker to cache",
					slog.String("symbol", pair.Symbol),
					slog.String("exchange", exchName),
					slog.String("error", err.Error()))
			}

		case <-ticker.C:
			if batched.Count > 0 {
				batched.Avg = batched.Sum / float64(batched.Count)

				// slog.Info("saving batched ticker data",
				// 	slog.String("symbol", batched.Symbol),
				// 	slog.String("exchange", exchName),
				// 	slog.Int("count", batched.Count),
				// 	slog.Float64("avg", batched.Avg),
				// 	slog.Float64("min", float64(batched.Min)),
				// 	slog.Float64("max", float64(batched.Max)))

				error := mp.repo.Save(ctx, batched)
				if error != nil {
					slog.Error("failed to save ticker to repository",
						slog.String("symbol", batched.Symbol),
						slog.String("exchange", exchName),
						slog.String("error", error.Error()))
				}

				batched.Reset()
			}

		case <-ctx.Done():
			if batched.Count > 0 {
				batched.Avg = batched.Sum / float64(batched.Count)

				// slog.Info("saving final batched data before shutdown",
				// 	slog.String("symbol", batched.Symbol),
				// 	slog.String("exchange", exchName),
				// 	slog.Int("count", batched.Count))

				err := mp.repo.Save(ctx, batched)
				if err != nil {
					slog.Error("failed to save final ticker to repository",
						slog.String("symbol", batched.Symbol),
						slog.String("exchange", exchName),
						slog.String("error", err.Error()))
				}
			}

			slog.Info("worker shutting down",
				slog.String("symbol", symbol),
				slog.String("exchange", exchName))
			return
		}
	}
}

func (mp *MarketProcessor) fanIn(ctx context.Context, chans []chan domain.AggregatedTicker) chan domain.AggregatedTicker {
	slog.Info("starting fan-in for all exchanges",
		slog.Int("num_channels", len(chans)))

	out := make(chan domain.AggregatedTicker)

	go func() {
		wg := &sync.WaitGroup{}
		for i, ch := range chans {
			wg.Add(1)
			go func(ch chan domain.AggregatedTicker, index int) {
				defer wg.Done()

				slog.Info("fan-in goroutine started",
					slog.Int("channel_index", index))

				for {
					select {
					case <-ctx.Done():
						slog.Info("fan-in context cancelled",
							slog.Int("channel_index", index))
						return
					case batched, ok := <-ch:
						if !ok {
							slog.Info("fan-in channel closed",
								slog.Int("channel_index", index))
							return
						}
						select {
						case <-ctx.Done():
							return
						case out <- batched:
						}
					}
				}
			}(ch, i)
		}

		wg.Wait()
		slog.Info("all fan-in goroutines completed")
		close(out)
	}()

	return out
}
