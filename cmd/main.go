package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sulte4/marketflow/internal/adapters/primary/web"
	"github.com/sulte4/marketflow/internal/adapters/secondary/exchange"
	"github.com/sulte4/marketflow/internal/adapters/secondary/postgres"
	"github.com/sulte4/marketflow/internal/adapters/secondary/redisadapter"
	"github.com/sulte4/marketflow/internal/core/service"
	"github.com/sulte4/marketflow/internal/ports"
	"github.com/sulte4/marketflow/pkg/config"

	_ "github.com/lib/pq"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		// AddSource: true,
	}))
	slog.SetDefault(logger)

	slog.Info("starting MarketFlow application")

	// Create a single cancellable context that is threaded through every
	// component.  cancel() is the primary shutdown trigger.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error("failed to load configuration", slog.String("error", err.Error()))
		return
	}

	slog.Info("configuration loaded successfully",
		slog.String("addr", cfg.Addr),
		slog.String("db_host", cfg.DBHost),
		slog.String("db_port", cfg.DBPort),
		slog.String("db_name", cfg.DBName),
		slog.String("redis_host", cfg.RedisHost),
		slog.String("redis_port", cfg.RedisPort),
		slog.Int("num_exchanges", len(cfg.Exchanges)))

	addr := flag.String("port", cfg.Addr, "Port number.")
	help := flag.Bool("help", false, "Show this screen.")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  marketflow [--port <N>]\n")
		fmt.Fprintf(os.Stderr, "  marketflow --help\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fmt.Fprintf(os.Stderr, "  --port N     Port number\n")
	}

	flag.Parse()

	if *help {
		flag.Usage()
		os.Exit(0)
	}

	slog.Info("initializing exchange sources",
		slog.Int("count", len(cfg.Exchanges)))

	exchanges := make([]ports.ExchangeSource, len(cfg.Exchanges))
	for index, exch := range cfg.Exchanges {
		exchanges[index] = exchange.NewSource(exch)
		slog.Info("exchange source created",
			slog.Int("index", index),
			slog.String("port", exch))
	}

	slog.Info("connecting to PostgreSQL database",
		slog.String("host", cfg.DBHost),
		slog.String("port", cfg.DBPort),
		slog.String("database", cfg.DBName))

	db, err := sql.Open("postgres", fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName,
	))
	if err != nil {
		slog.Error("failed to open PostgreSQL connection",
			slog.String("error", err.Error()),
			slog.String("host", cfg.DBHost),
			slog.String("port", cfg.DBPort))
		return
	}
	// NOTE: db.Close() is called explicitly later in the shutdown sequence,
	// after the market processor and all workers have fully stopped.  We do
	// NOT use defer here because defer runs before goroutines spawned below
	// have finished, which would close the DB while workers are still saving.

	connected := false
	for i := 0; i < 5; i++ {
		if err := db.PingContext(ctx); err != nil {
			slog.Warn("failed to ping PostgreSQL, retrying...",
				slog.String("error", err.Error()),
				slog.Int("attempt", i+1),
				slog.Int("max_attempts", 5))
			if i < 4 {
				time.Sleep(2 * time.Second)
			}
		} else {
			slog.Info("successfully connected to PostgreSQL",
				slog.Int("attempt", i+1))
			connected = true
			break
		}
	}

	if !connected {
		slog.Error("failed to connect to PostgreSQL after all retries, shutting down application")
		_ = db.Close()
		os.Exit(1)
	}

	rdsAddr := fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort)

	slog.Info("connecting to Redis",
		slog.String("address", rdsAddr))

	rdb := redis.NewClient(&redis.Options{Addr: rdsAddr})
	// Same reasoning as db: closed explicitly after workers are done.

	for i := 0; i < 5; i++ {
		if err := rdb.Ping(ctx).Err(); err != nil {
			slog.Warn("failed to ping Redis, retrying...",
				slog.String("error", err.Error()),
				slog.Int("attempt", i+1),
				slog.Int("max_attempts", 5))
			if i < 4 {
				time.Sleep(2 * time.Second)
			}
		} else {
			slog.Info("successfully connected to Redis",
				slog.Int("attempt", i+1))
			break
		}
	}

	slog.Info("initializing application components")

	cache := redisadapter.New(rdb)
	repo := postgres.NewRepository(db)

	marketProcessor := service.NewMarketProcessor(exchanges, cache, repo)

	// Track the processor goroutine so we can wait for every worker's final
	// DB save to complete before we tear down the database connection.
	var processorWg sync.WaitGroup
	processorWg.Add(1)
	go func() {
		defer processorWg.Done()
		slog.Info("market processor goroutine starting")
		marketProcessor.Start(ctx)
		slog.Info("market processor goroutine exited")
	}()

	marketService := service.New(repo, cache, exchanges)

	handler := web.NewHandler(marketService)
	routes := web.New(handler)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", *addr),
		Handler: routes,
	}

	// Run the HTTP server in a goroutine so main can remain free to handle
	// signals.  Any fatal server error is forwarded through serverErrCh.
	serverErrCh := make(chan error, 1)
	go func() {
		slog.Info("HTTP server starting",
			slog.String("address", srv.Addr),
			slog.String("port", *addr))

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error",
				slog.String("error", err.Error()),
				slog.String("address", srv.Addr))
			serverErrCh <- err
		} else {
			serverErrCh <- nil
		}
	}()

	// ── Wait for a shutdown trigger ──────────────────────────────────────────
	//
	// We block here until one of:
	//   • SIGTERM or SIGINT is delivered by the OS / orchestrator, or
	//   • the HTTP server fails unexpectedly.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigChan:
		slog.Info("shutdown signal received",
			slog.String("signal", sig.String()))
	case err := <-serverErrCh:
		if err != nil {
			slog.Error("HTTP server failed unexpectedly, initiating shutdown",
				slog.String("error", err.Error()))
		}
	}

	shutdownStart := time.Now()
	slog.Info("beginning graceful shutdown sequence")

	// ── Phase 1: Cancel the application context ──────────────────────────────
	//
	// This is the primary shutdown signal.  Every goroutine that selects on
	// ctx.Done() will begin winding down.
	slog.Info("shutdown phase 1/6: cancelling application context")
	cancel()

	// ── Phase 2: Gracefully stop the HTTP server ─────────────────────────────
	//
	// srv.Shutdown() stops the listener and waits for in-flight requests to
	// complete.  We give active requests up to 15 seconds.
	slog.Info("shutdown phase 2/6: shutting down HTTP server")
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer httpCancel()
	if err := srv.Shutdown(httpCtx); err != nil {
		slog.Error("HTTP server shutdown error",
			slog.String("error", err.Error()))
	} else {
		slog.Info("HTTP server stopped")
	}

	// ── Phase 3: Close exchange TCP connections ───────────────────────────────
	//
	// The stream goroutines are blocked inside bufio.Scanner.Scan() on the TCP
	// connection.  Cancelling the context alone is not enough to unblock them;
	// we must close the underlying net.Conn.  Once the connection is closed,
	// Scan() returns false, Stream() returns, and the goroutine closes its
	// output channel — propagating the shutdown signal down the pipeline.
	slog.Info("shutdown phase 3/6: closing exchange TCP connections",
		slog.Int("num_exchanges", len(exchanges)))
	for _, exch := range exchanges {
		if err := exch.Close(); err != nil {
			slog.Error("failed to close exchange connection",
				slog.String("exchange", exch.SourceName()),
				slog.String("error", err.Error()))
		} else {
			slog.Info("exchange connection closed",
				slog.String("exchange", exch.SourceName()))
		}
	}

	// ── Phase 4: Wait for processor and all workers to finish ─────────────────
	//
	// The processor only returns after:
	//   stream exits → fanOut closes worker channels → workers complete their
	//   final DB saves → fanOut WaitGroup done → workersResult closed →
	//   fanIn detects closure → fanIn WaitGroup done → batchedResult closed →
	//   Start()'s for-range exits → processorWg.Done()
	//
	// We MUST wait here before touching the DB or Redis so that workers can
	// still write their final batched records.
	slog.Info("shutdown phase 4/6: waiting for market processor and all workers to complete")
	processorWg.Wait()
	slog.Info("market processor and all workers completed")

	// ── Phase 5: Close PostgreSQL ─────────────────────────────────────────────
	slog.Info("shutdown phase 5/6: closing PostgreSQL connection")
	if err := db.Close(); err != nil {
		slog.Error("failed to close PostgreSQL connection",
			slog.String("error", err.Error()))
	} else {
		slog.Info("PostgreSQL connection closed successfully")
	}

	// ── Phase 6: Close Redis ──────────────────────────────────────────────────
	slog.Info("shutdown phase 6/6: closing Redis connection")
	if err := rdb.Close(); err != nil {
		slog.Error("failed to close Redis client",
			slog.String("error", err.Error()))
	} else {
		slog.Info("Redis connection closed successfully")
	}

	slog.Info("MarketFlow graceful shutdown complete",
		slog.Duration("total_shutdown_time", time.Since(shutdownStart)))
}
