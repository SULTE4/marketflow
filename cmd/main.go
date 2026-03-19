package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
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

	ctx := context.Background()

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

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName))
	if err != nil {
		slog.Error("failed to connect to PostgreSQL",
			slog.String("error", err.Error()),
			slog.String("host", cfg.DBHost),
			slog.String("port", cfg.DBPort))
		return
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.Error("failed to close PostgreSQL connection", slog.String("error", err.Error()))
		} else {
			slog.Info("PostgreSQL connection closed successfully")
		}
	}()

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
		os.Exit(1)
	}

	rdsAddr := fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort)

	slog.Info("connecting to Redis",
		slog.String("address", rdsAddr))

	rdb := redis.NewClient(&redis.Options{Addr: rdsAddr})
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

	defer func() {
		if err := rdb.Close(); err != nil {
			slog.Error("failed to close Redis client", slog.String("error", err.Error()))
		} else {
			slog.Info("Redis client closed successfully")
		}
	}()

	slog.Info("initializing application components")

	cache := redisadapter.New(rdb)
	repo := postgres.NewRepository(db)

	marketProcessor := service.NewMarketProcessor(exchanges, cache, repo)
	slog.Info("starting market processor in background")
	go marketProcessor.Start(ctx)

	marketService := service.New(repo, cache, exchanges)

	handler := web.NewHandler(marketService)
	routes := web.New(handler)

	srv := http.Server{
		Addr:    fmt.Sprintf(":%s", *addr),
		Handler: routes,
	}

	slog.Info("server starting",
		slog.String("address", srv.Addr),
		slog.String("port", *addr))

	if err = srv.ListenAndServe(); err != nil {
		slog.Error("server failed to start",
			slog.String("error", err.Error()),
			slog.String("address", srv.Addr))
	}
}
