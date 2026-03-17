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
	workersResults := make([]chan domain.AggregatedTicker, 3)
	for index, exchange := range mp.sources {
		err := exchange.Dial()
		if err != nil {
			slog.Error("connection failed to source", "error", err.Error())
		}
		in := make(chan domain.Ticker)
		go func() {
			exchange.Stream(ctx, in)
		}()
		workerResult := mp.fanOut(ctx, in, exchange.SourceName())
		workersResults[index] = workerResult
	}
	batchedResult := mp.fanIn(ctx, workersResults)
	for val := range batchedResult {
		fmt.Println(val)
	}
}

func (mp *MarketProcessor) fanOut(ctx context.Context, in <-chan domain.Ticker, exchName string) chan domain.AggregatedTicker {
	workers := make(map[string]chan domain.Ticker)
	workersResult := make(chan domain.AggregatedTicker)
	go func() {
		for ticker := range in {
			ch, exists := workers[ticker.Symbol]
			if !exists {
				ch = make(chan domain.Ticker)
				workers[ticker.Symbol] = ch
				go mp.worker(ctx, ch, workersResult, exchName)
			}
			select {
			case <-ctx.Done():
				return
			case ch <- ticker:
			}
		}
		for _, ch := range workers {
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
	for {
		select {
		case pair, ok := <-in:
			if !ok {
				slog.Info("worker stopped due channel close")
				return
			}
			batched.Calculation(pair)
			batched.Symbol = pair.Symbol
			batched.Source = exchName
			// adding to redis
			err := mp.cache.Set(ctx, &pair)
			if err != nil {
				slog.Error("failed to set ticker to cache", "error", err)
			}
			// fmt.Println("adding to redis", batched.Source)
		case <-ticker.C:
			// adding to postgres
			batched.Avg = batched.Sum / float64(batched.Count)
			error := mp.repo.Save(ctx, batched)
			if error != nil {
				slog.Error("failed to save ticker to repository", "error", error)
			}
			// fmt.Println("adding to postgres", batched.ToString())
			batched.Reset()
		case <-ctx.Done():
			// add rest batched data
			err := mp.repo.Save(ctx, batched)
			if err != nil {
				slog.Error("failed to save ticker to repository", "error", err)
			}
			return
		}
	}
}

func (mp *MarketProcessor) fanIn(ctx context.Context, chans []chan domain.AggregatedTicker) chan domain.AggregatedTicker {
	out := make(chan domain.AggregatedTicker)
	go func() {
		wg := &sync.WaitGroup{}
		for _, ch := range chans {
			wg.Add(1)
			go func(ch chan domain.AggregatedTicker) {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case batched, ok := <-ch:
						if !ok {
							return
						}
						select {
						case <-ctx.Done():
							return
						case out <- batched:
						}
					}
				}
			}(ch)
		}
		wg.Wait()
		close(out)
	}()
	return out
}
