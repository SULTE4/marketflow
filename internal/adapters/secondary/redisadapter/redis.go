package redisadapter

import (
	"context"
	"fmt"
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

	err := r.client.HSet(ctx, key, map[string]interface{}{
		"price": ticker.Price,
		"ts":    ticker.Timestamp,
	}).Err()

	if err == nil {
		r.client.Expire(ctx, key, time.Duration(r.expireTime)*time.Second)
	}

	return err
}

func (r *RedisTickerCache) GetLatest(ctx context.Context, symbol string) (*domain.Ticker, error) {
	pattern := fmt.Sprintf("latest:*:%s", symbol)
	keys, err := r.client.Keys(ctx, pattern).Result()
	if err != nil || len(keys) == 0 {
		return nil, err
	}

	var latest *domain.Ticker
	for _, key := range keys {
		data, err := r.client.HGetAll(ctx, key).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		price, _ := strconv.ParseFloat(data["price"], 32)
		ts, _ := strconv.ParseInt(data["ts"], 10, 64)
		if latest == nil || ts > latest.Timestamp {
			parts := strings.Split(key, ":")
			latest = &domain.Ticker{Symbol: symbol, Source: parts[1], Price: float32(price), Timestamp: ts}
		}
	}
	return latest, nil
}

func (r *RedisTickerCache) GetLatestByExchange(ctx context.Context, symbol, exchangeName string) (*domain.Ticker, error) {
	key := fmt.Sprintf("latest:%s:%s", exchangeName, symbol)
	data, err := r.client.HGetAll(ctx, key).Result()
	if err != nil || len(data) == 0 {
		return nil, err
	}

	price, _ := strconv.ParseFloat(data["price"], 32)
	ts, _ := strconv.ParseInt(data["ts"], 10, 64)

	return &domain.Ticker{
		Symbol:    symbol,
		Source:    exchangeName,
		Price:     float32(price),
		Timestamp: ts,
	}, nil
}

func (r *RedisTickerCache) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}
