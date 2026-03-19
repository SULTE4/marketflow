package redisadapter

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sulte4/marketflow/internal/core/domain"
	"github.com/sulte4/marketflow/internal/ports"
)

type RedisTickerCache struct {
	client     *redis.Client
	expireTime int
}

func New(client *redis.Client) ports.TickerCache {
	return &RedisTickerCache{client: client, expireTime: 70}
}

func (r *RedisTickerCache) Set(ctx context.Context, ticker *domain.Ticker) error {
	key := fmt.Sprintf("latest:%s:%s", ticker.Source, ticker.Symbol)

	// slog.Info("setting ticker in cache",
	// 	slog.String("key", key),
	// 	slog.String("symbol", ticker.Symbol),
	// 	slog.String("exchange", ticker.Source),
	// 	slog.Float64("price", float64(ticker.Price)),
	// 	slog.Int64("timestamp", ticker.Timestamp))

	err := r.client.HSet(ctx, key, map[string]interface{}{
		"price": ticker.Price,
		"ts":    ticker.Timestamp,
	}).Err()
	if err != nil {
		slog.Error("failed to set ticker in cache",
			slog.String("key", key),
			slog.String("symbol", ticker.Symbol),
			slog.String("exchange", ticker.Source),
			slog.String("error", err.Error()))
		return err
	}

	if err := r.client.Expire(ctx, key, time.Duration(r.expireTime)*time.Second).Err(); err != nil {
		slog.Error("failed to set expiration on cache key",
			slog.String("key", key),
			slog.String("error", err.Error()))
	}

	return nil
}

func (r *RedisTickerCache) GetLatest(ctx context.Context, symbol string) (*domain.Ticker, error) {
	pattern := fmt.Sprintf("latest:*:%s", symbol)

	slog.Info("getting latest ticker from cache",
		slog.String("symbol", symbol),
		slog.String("pattern", pattern))

	keys, err := r.client.Keys(ctx, pattern).Result()
	if err != nil {
		slog.Error("failed to query cache keys",
			slog.String("pattern", pattern),
			slog.String("error", err.Error()))
		return nil, err
	}

	if len(keys) == 0 {
		slog.Warn("no cache keys found for symbol",
			slog.String("symbol", symbol),
			slog.String("pattern", pattern))
		return nil, fmt.Errorf("no data found for symbol: %s", symbol)
	}

	var latest *domain.Ticker
	for _, key := range keys {
		data, err := r.client.HGetAll(ctx, key).Result()
		if err != nil || len(data) == 0 {
			slog.Warn("failed to get data from cache key",
				slog.String("key", key),
				slog.String("error", fmt.Sprintf("%v", err)))
			continue
		}
		price, _ := strconv.ParseFloat(data["price"], 32)
		ts, _ := strconv.ParseInt(data["ts"], 10, 64)
		if latest == nil || ts > latest.Timestamp {
			parts := strings.Split(key, ":")
			latest = &domain.Ticker{Symbol: symbol, Source: parts[1], Price: float32(price), Timestamp: ts}
		}
	}

	if latest == nil {
		slog.Warn("no valid ticker data found in cache",
			slog.String("symbol", symbol))
		return nil, fmt.Errorf("no valid data for symbol: %s", symbol)
	}

	slog.Info("latest ticker retrieved from cache",
		slog.String("symbol", symbol),
		slog.String("exchange", latest.Source),
		slog.Float64("price", float64(latest.Price)),
		slog.Int64("timestamp", latest.Timestamp))

	return latest, nil
}

func (r *RedisTickerCache) GetLatestByExchange(ctx context.Context, symbol, exchangeName string) (*domain.Ticker, error) {
	key := fmt.Sprintf("latest:%s:%s", exchangeName, symbol)

	slog.Info("getting latest ticker by exchange from cache",
		slog.String("symbol", symbol),
		slog.String("exchange", exchangeName),
		slog.String("key", key))

	data, err := r.client.HGetAll(ctx, key).Result()
	if err != nil {
		slog.Error("failed to get ticker from cache",
			slog.String("key", key),
			slog.String("symbol", symbol),
			slog.String("exchange", exchangeName),
			slog.String("error", err.Error()))
		return nil, err
	}

	if len(data) == 0 {
		slog.Warn("no data found in cache for key",
			slog.String("key", key),
			slog.String("symbol", symbol),
			slog.String("exchange", exchangeName))
		return nil, fmt.Errorf("no data found for symbol %s on exchange %s", symbol, exchangeName)
	}

	price, _ := strconv.ParseFloat(data["price"], 32)
	ts, _ := strconv.ParseInt(data["ts"], 10, 64)

	ticker := &domain.Ticker{
		Symbol:    symbol,
		Source:    exchangeName,
		Price:     float32(price),
		Timestamp: ts,
	}

	slog.Info("ticker retrieved from cache by exchange",
		slog.String("symbol", symbol),
		slog.String("exchange", exchangeName),
		slog.Float64("price", float64(ticker.Price)),
		slog.Int64("timestamp", ticker.Timestamp))

	return ticker, nil
}

func (r *RedisTickerCache) Ping(ctx context.Context) error {
	slog.Info("pinging Redis cache")

	err := r.client.Ping(ctx).Err()
	if err != nil {
		slog.Error("Redis ping failed", slog.String("error", err.Error()))
		return err
	}

	slog.Info("Redis ping successful")
	return nil
}
