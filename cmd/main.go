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
	exchangehandler "github.com/sulte4/marketflow/internal/adapters/secondary/exchangeHandler"
	"github.com/sulte4/marketflow/internal/adapters/secondary/postgres"
	"github.com/sulte4/marketflow/internal/adapters/secondary/redisadapter"
	"github.com/sulte4/marketflow/internal/core/service"
	"github.com/sulte4/marketflow/internal/ports"
	"github.com/sulte4/marketflow/pkg/config"

	_ "github.com/lib/pq"
)

func main() {
	ctx := context.Background()
	cfg, err := config.LoadConfig()
	if err != nil {
		slog.Error(err.Error())
		return
	}
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

	exchanges := make([]ports.ExchangeSource, len(cfg.Exchanges))
	for index, exch := range cfg.Exchanges {
		exchanges[index] = exchangehandler.NewSource(exch)
	}

	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPassword, cfg.DBName))
	if err != nil {
		slog.Error("Failed to connect to PostgreSQL", "error", err)
		return
	}
	defer db.Close()
	for i := 0; i < 5; i++ {
		if err := db.PingContext(ctx); err != nil {
			slog.Error("Failed to connect to PostgreSQL, retrying...", "error", err, "attempt", i+1)
		} else {
			slog.Info("Successfully connected to PostgreSQL")
			break
		}
	}

	rdsAddr := fmt.Sprintf("%s:%s", cfg.RedisHost, cfg.RedisPort)
	rdb := redis.NewClient(&redis.Options{Addr: rdsAddr})
	for i := 0; i < 5; i++ {
		if err := rdb.Ping(ctx).Err(); err != nil {
			slog.Error("Failed to connect to Redis, retrying...", "error", err, "attempt", i+1)
		} else {
			slog.Info("Successfully connected to Redis")
			break
		}
		time.Sleep(2 * time.Second)
	}
	defer func() {
		if err := rdb.Close(); err != nil {
			slog.Error("Failed to close Redis client", "error", err)
		}
	}()

	cache := redisadapter.New(rdb)
	repo := postgres.NewRepository(db)

	marketProcessor := service.NewMarketProcessor(exchanges, cache, repo)
	go marketProcessor.Start(ctx)

	marketService := service.New(repo, cache, exchanges)

	handler := web.NewHandler(marketService)
	routes := web.New(handler)

	srv := http.Server{
		Addr:    fmt.Sprintf(":%s", *addr),
		Handler: routes,
	}

	slog.Info("Server starting")
	if err = srv.ListenAndServe(); err != nil {
		slog.Error("failed to start server", "error", err.Error())
	}
}
