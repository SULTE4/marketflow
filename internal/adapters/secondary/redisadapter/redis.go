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

// latest:{exchange}:{symbol} hash

func (r *RedisTickerCache) Set(ctx context.Context, ticker *domain.Ticker) error {
	key := fmt.Sprintf("latest:%s:%s", ticker.Source, ticker.Symbol)

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

	// slog.Info("latest ticker retrieved from cache",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", latest.Source),
	// 	slog.Float64("price", float64(latest.Price)),
	// 	slog.Int64("timestamp", latest.Timestamp))

	return latest, nil
}

func (r *RedisTickerCache) GetLatestByExchange(ctx context.Context, symbol, exchangeName string) (*domain.Ticker, error) {
	key := fmt.Sprintf("latest:%s:%s", exchangeName, symbol)

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

	// slog.Info("ticker retrieved from cache by exchange",
	// 	slog.String("symbol", symbol),
	// 	slog.String("exchange", exchangeName),
	// 	slog.Float64("price", float64(ticker.Price)),
	// 	slog.Int64("timestamp", ticker.Timestamp))

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

// ── ts:{exchange}:{symbol} sorted-set ───────────────────────────────────────

func (r *RedisTickerCache) AddToTimeSeries(ctx context.Context, ticker *domain.Ticker) error {
	key := fmt.Sprintf("ts:%s:%s", ticker.Source, ticker.Symbol)
	nowMs := time.Now().UnixMilli()
	cutoffMs := nowMs - int64(r.expireTime)*1000

	// "%.8f:%d" gives consistent float representation and guarantees
	// uniqueness because the millisecond timestamp is included.
	member := fmt.Sprintf("%.8f:%d", ticker.Price, nowMs)

	pipe := r.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(nowMs), Member: member})
	pipe.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(cutoffMs, 10))
	pipe.Expire(ctx, key, time.Duration(r.expireTime)*time.Second)

	if _, err := pipe.Exec(ctx); err != nil {
		slog.Error("failed to add ticker to time-series",
			slog.String("key", key),
			slog.String("symbol", ticker.Symbol),
			slog.String("exchange", ticker.Source),
			slog.String("error", err.Error()))
		return err
	}

	return nil
}

// GetTickersInRange returns every tick whose ingestion timestamp (Unix ms) is
// >= sinceMs for the given symbol and exchange.
//
// When exchange is "" the method fans out across all matching exchange keys
// using a cursor-based SCAN (never the blocking KEYS command).  With the
// small number of exchanges in this system the fan-out is bounded and fast.
func (r *RedisTickerCache) GetTickersInRange(
	ctx context.Context,
	symbol, exchange string,
	sinceMs int64,
) ([]*domain.Ticker, error) {
	var keys []string

	if exchange != "" {
		keys = []string{fmt.Sprintf("ts:%s:%s", exchange, symbol)}
	} else {
		// Cursor-based SCAN — safe for production Redis.
		pattern := fmt.Sprintf("ts:*:%s", symbol)
		var cursor uint64
		for {
			ks, cur, err := r.client.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				slog.Error("failed to scan time-series keys",
					slog.String("pattern", pattern),
					slog.String("error", err.Error()))
				return nil, err
			}
			keys = append(keys, ks...)
			cursor = cur
			if cursor == 0 {
				break
			}
		}
	}

	if len(keys) == 0 {
		return nil, nil
	}

	minScore := strconv.FormatInt(sinceMs, 10)
	var tickers []*domain.Ticker

	for _, key := range keys {
		// key format: ts:{exchange}:{symbol}
		// SplitN(…, 3) is safe even when the symbol itself contains ":"
		// because we only split at the first two colons.
		parts := strings.SplitN(key, ":", 3)
		if len(parts) != 3 {
			slog.Warn("unexpected time-series key format, skipping",
				slog.String("key", key))
			continue
		}
		exchName := parts[1]

		members, err := r.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{
			Min: minScore,
			Max: "+inf",
		}).Result()
		if err != nil {
			slog.Error("failed to get time-series range",
				slog.String("key", key),
				slog.String("error", err.Error()))
			continue
		}

		for _, m := range members {
			// member format: "{price_8dp}:{unix_ms}"
			// LastIndex handles prices that may contain "e" in scientific
			// notation — the timestamp is always the last colon-delimited part.
			idx := strings.LastIndex(m, ":")
			if idx < 0 {
				continue
			}

			price, err := strconv.ParseFloat(m[:idx], 32)
			if err != nil {
				slog.Warn("failed to parse price from time-series member",
					slog.String("member", m),
					slog.String("error", err.Error()))
				continue
			}

			tsMs, err := strconv.ParseInt(m[idx+1:], 10, 64)
			if err != nil {
				slog.Warn("failed to parse timestamp from time-series member",
					slog.String("member", m),
					slog.String("error", err.Error()))
				continue
			}

			tickers = append(tickers, &domain.Ticker{
				Symbol:    symbol,
				Source:    exchName,
				Price:     float32(price),
				Timestamp: tsMs / 1000,
			})
		}
	}

	return tickers, nil
}
