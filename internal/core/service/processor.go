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

const (
	initialReconnectDelay = 1 * time.Second
	maxReconnectDelay     = 30 * time.Second
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

// Start launches the streaming pipeline for every exchange and blocks until
// every goroutine in the pipeline has fully exited.  The caller controls
// shutdown by cancelling ctx and (importantly) closing the underlying exchange
// TCP connections so that the blocking bufio.Scanner.Scan() calls unblock.
//
// Shutdown propagation chain:
//
//	ctx cancelled + exchange.Close()
//	  → streamWithReconnect detects ctx.Done(), exits → defer close(in)
//	  → fanOut for-range exits → defer closes worker channels + workerWg.Wait()
//	  → workers complete final saves → workerWg.Done()
//	  → close(workersResult)
//	  → fanIn for-range exits → wg.Done()
//	  → close(out / batchedResult)
//	  → Start()'s for-range exits → Start() returns
func (mp *MarketProcessor) Start(ctx context.Context) {
	slog.Info("starting market processor",
		slog.Int("num_sources", len(mp.sources)))

	// Use len(mp.sources) — not a hardcoded 3 — so the slice is always the
	// right size.  Every exchange gets a streamWithReconnect goroutine and a
	// corresponding fanOut, so no slots will be nil.
	workersResults := make([]chan domain.AggregatedTicker, len(mp.sources))

	for index, exch := range mp.sources {
		in := make(chan domain.Ticker)

		// streamWithReconnect is the sole writer of `in`. It handles the initial
		// connection and all reconnections internally. It only closes `in` when
		// ctx is cancelled, keeping the fanOut/worker pipeline alive across
		// any number of TCP reconnections.
		go mp.streamWithReconnect(ctx, exch, in)

		workerResult := mp.fanOut(ctx, in, exch.SourceName())
		workersResults[index] = workerResult
	}

	slog.Info("all exchange sources connected and streaming")

	batchedResult := mp.fanIn(ctx, workersResults)
	for val := range batchedResult {
		fmt.Println(val)
	}

	slog.Info("market processor stopped, all goroutines complete")
}

// streamWithReconnect is the sole writer of the `in` channel for one exchange.
// It handles both the initial connection and all subsequent reconnections after
// a dropped TCP link, using exponential backoff capped at maxReconnectDelay.
//
// Design invariant: close(in) is called exactly once — via defer — only when
// ctx is cancelled (application shutdown).  A dropped connection does NOT
// close `in`; it triggers a re-dial so the downstream fanOut / worker pipeline
// stays alive across reconnections without losing any in-memory batch state.
func (mp *MarketProcessor) streamWithReconnect(ctx context.Context, exch ports.ExchangeSource, in chan domain.Ticker) {
	defer func() {
		slog.Info("stream goroutine exiting, closing ticker channel",
			slog.String("exchange", exch.SourceName()))
		close(in)
	}()

	backoff := initialReconnectDelay

	for {
		if ctx.Err() != nil {
			return
		}

		slog.Info("dialling exchange",
			slog.String("exchange", exch.SourceName()))

		if err := exch.Dial(); err != nil {
			slog.Error("failed to connect to exchange, will retry",
				slog.String("exchange", exch.SourceName()),
				slog.Duration("retry_in", backoff),
				slog.String("error", err.Error()))

			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxReconnectDelay)
			case <-ctx.Done():
				return
			}
			continue
		}

		if ctx.Err() != nil {
			_ = exch.Close()
			return
		}

		slog.Info("connected to exchange, starting stream",
			slog.String("exchange", exch.SourceName()))

		backoff = initialReconnectDelay
		connectedAt := time.Now()

		err := exch.Stream(ctx, in)

		if ctx.Err() != nil {
			return
		}

		if err != nil {
			slog.Error("stream ended with error, will reconnect",
				slog.String("exchange", exch.SourceName()),
				slog.Duration("was_connected_for", time.Since(connectedAt)),
				slog.Duration("retry_in", backoff),
				slog.String("error", err.Error()))
		} else {
			slog.Warn("stream ended cleanly but unexpectedly, will reconnect",
				slog.String("exchange", exch.SourceName()),
				slog.Duration("was_connected_for", time.Since(connectedAt)),
				slog.Duration("retry_in", backoff))
		}

		_ = exch.Close()

		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, maxReconnectDelay)
		case <-ctx.Done():
			return
		}
	}
}

// fanOut reads tickers from `in` and routes each one to a per-symbol worker.
// It returns a single channel onto which all workers would publish aggregated
// results (reserved for future use; workers currently save directly to the
// repository).
//
// Shutdown guarantee: the returned channel is only closed after every worker
// goroutine has returned, ensuring that no worker is still writing to the DB
// when the caller tears down database connections.
func (mp *MarketProcessor) fanOut(ctx context.Context, in <-chan domain.Ticker, exchName string) chan domain.AggregatedTicker {
	slog.Info("starting fan-out for exchange",
		slog.String("exchange", exchName))

	workers := make(map[string]chan domain.Ticker)
	workersResult := make(chan domain.AggregatedTicker)

	var workerWg sync.WaitGroup

	go func() {
		// On exit — whether `in` was closed (clean stream end) or ctx was
		// cancelled — we must:
		//   1. Close every worker's input channel so workers can drain and exit.
		//   2. Wait for all workers to complete (including deferred final saves).
		//   3. Only then close workersResult so fanIn detects completion.
		defer func() {
			slog.Info("fan-out closing all worker channels",
				slog.String("exchange", exchName),
				slog.Int("num_workers", len(workers)))

			for symbol, ch := range workers {
				slog.Info("closing worker channel",
					slog.String("symbol", symbol),
					slog.String("exchange", exchName))
				close(ch)
			}

			slog.Info("fan-out waiting for all workers to complete",
				slog.String("exchange", exchName))
			workerWg.Wait()

			slog.Info("fan-out all workers done, closing result channel",
				slog.String("exchange", exchName))
			close(workersResult)
		}()

		for ticker := range in {
			ch, exists := workers[ticker.Symbol]
			if !exists {
				ch = make(chan domain.Ticker)
				workers[ticker.Symbol] = ch

				workerWg.Add(1)
				go func(workerCh chan domain.Ticker) {
					defer workerWg.Done()
					mp.worker(ctx, workerCh, workersResult, exchName)
				}(ch)

				slog.Info("spawned new worker for symbol",
					slog.String("symbol", ticker.Symbol),
					slog.String("exchange", exchName),
					slog.Int("total_workers", len(workers)))
			}

			select {
			case <-ctx.Done():
				slog.Info("fan-out context cancelled, stopping",
					slog.String("exchange", exchName))
				return
			case ch <- ticker:
			}
		}

		slog.Info("fan-out input channel exhausted (stream ended)",
			slog.String("exchange", exchName))
	}()

	return workersResult
}

// worker receives tickers for a single symbol, caches each one immediately,
// and persists 60-second aggregations to the repository.
//
// Key design points:
//   - The deferred final-save uses context.Background() with a timeout because
//     the application context (ctx) is already cancelled by the time the defer
//     runs.  Using ctx here would cause every final save to fail instantly.
//   - `defer close(out)` has been intentionally removed.  All workers for a
//     given exchange share the same `out` (workersResult) channel; only the
//     fanOut goroutine may close it, and only after all workers have returned.
//     Multiple goroutines closing the same channel causes a panic.
//   - The final save is placed in a defer so it runs regardless of which exit
//     path is taken (channel closed by fanOut, or ctx.Done()).
func (mp *MarketProcessor) worker(ctx context.Context, in <-chan domain.Ticker, out chan<- domain.AggregatedTicker, exchName string) {
	batched := domain.NewAggregatedTicker()
	ticker := time.NewTicker(time.Second * 60)
	defer ticker.Stop()

	var symbol string

	// Always attempt to flush any accumulated data when the worker exits.
	// A fresh context with a generous timeout is used so the save succeeds
	// even though the application context has already been cancelled.
	defer func() {
		if batched.Count > 0 {
			batched.Avg = batched.Sum / float64(batched.Count)

			slog.Info("saving final batched data on worker exit",
				slog.String("symbol", batched.Symbol),
				slog.String("exchange", exchName),
				slog.Int("count", batched.Count),
				slog.Float64("avg", batched.Avg),
				slog.Float64("min", float64(batched.Min)),
				slog.Float64("max", float64(batched.Max)))

			saveCtx, saveCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer saveCancel()

			if err := mp.repo.Save(saveCtx, batched); err != nil {
				slog.Error("failed to save final ticker to repository",
					slog.String("symbol", batched.Symbol),
					slog.String("exchange", exchName),
					slog.String("error", err.Error()))
			} else {
				slog.Info("final batched data saved successfully",
					slog.String("symbol", batched.Symbol),
					slog.String("exchange", exchName))
			}
		}

		slog.Info("worker shutdown complete",
			slog.String("symbol", symbol),
			slog.String("exchange", exchName))
	}()

	for {
		select {
		case pair, ok := <-in:
			if !ok {
				// fanOut closed our input channel as part of its shutdown
				// sequence.  Return cleanly; the deferred flush above handles
				// any remaining accumulated data.
				slog.Info("worker input channel closed, stopping worker",
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

			if err := mp.cache.Set(ctx, &pair); err != nil {
				slog.Error("failed to set ticker to cache",
					slog.String("symbol", pair.Symbol),
					slog.String("exchange", exchName),
					slog.String("error", err.Error()))
			}

			if err := mp.cache.AddToTimeSeries(ctx, &pair); err != nil {
				slog.Error("failed to add ticker to time-series",
					slog.String("symbol", pair.Symbol),
					slog.String("exchange", exchName),
					slog.String("error", err.Error()))
			}

		case <-ticker.C:
			if batched.Count > 0 {
				batched.Avg = batched.Sum / float64(batched.Count)

				slog.Info("saving periodic batched ticker data",
					slog.String("symbol", batched.Symbol),
					slog.String("exchange", exchName),
					slog.Int("count", batched.Count),
					slog.Float64("avg", batched.Avg),
					slog.Float64("min", float64(batched.Min)),
					slog.Float64("max", float64(batched.Max)))

				if err := mp.repo.Save(ctx, batched); err != nil {
					slog.Error("failed to save ticker to repository",
						slog.String("symbol", batched.Symbol),
						slog.String("exchange", exchName),
						slog.String("error", err.Error()))
				}

				batched.Reset()
			}

		case <-ctx.Done():
			// Application is shutting down.  Return so the deferred flush
			// above can persist any data accumulated since the last periodic
			// save using a fresh, non-cancelled context.
			slog.Info("worker context cancelled, stopping worker",
				slog.String("symbol", symbol),
				slog.String("exchange", exchName))
			return
		}
	}
}

// fanIn merges one result channel per exchange into a single output channel.
// Nil channels (exchanges that failed Dial) are skipped.
//
// Each goroutine uses `for batched := range ch` instead of selecting on
// ctx.Done() for the read side.  This is deliberate: we want fanIn to stay
// alive until fanOut has confirmed that every worker finished (by closing
// workersResult), so that Start() returns only after all final saves are done.
// ctx.Done() is only checked on the write side to avoid a send that would
// block forever if nobody is reading from `out`.
func (mp *MarketProcessor) fanIn(ctx context.Context, chans []chan domain.AggregatedTicker) chan domain.AggregatedTicker {
	slog.Info("starting fan-in for all exchanges",
		slog.Int("num_channels", len(chans)))

	out := make(chan domain.AggregatedTicker)

	go func() {
		wg := &sync.WaitGroup{}

		for i, ch := range chans {
			if ch == nil {
				// This exchange failed Dial(); its slot in the slice is nil.
				slog.Warn("fan-in skipping nil channel (exchange failed to connect)",
					slog.Int("channel_index", i))
				continue
			}

			wg.Add(1)
			go func(ch chan domain.AggregatedTicker, index int) {
				defer wg.Done()

				slog.Info("fan-in goroutine started",
					slog.Int("channel_index", index))

				// Range exits only when ch is closed.  ch is closed by fanOut
				// only after workerWg.Wait() returns, i.e., only after every
				// worker's final save has completed.
				for batched := range ch {
					select {
					case out <- batched:
					case <-ctx.Done():
						// Safety valve: if nobody is consuming `out` (shouldn't
						// happen during normal operation) we don't deadlock.
						slog.Info("fan-in context cancelled during send",
							slog.Int("channel_index", index))
						return
					}
				}

				slog.Info("fan-in channel closed, goroutine exiting",
					slog.Int("channel_index", index))
			}(ch, i)
		}

		wg.Wait()
		slog.Info("all fan-in goroutines completed, closing output channel")
		close(out)
	}()

	return out
}
