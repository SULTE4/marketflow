# MarketFlow

> A high-throughput real-time market data processing system that ingests cryptocurrency price feeds from multiple exchanges concurrently, caches live data in Redis, persists 60-second aggregations to PostgreSQL, and serves a REST API for querying prices and statistics.

## Table of Contents

- [How It Works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Running the Exchanges](#running-the-exchanges)
- [API Reference](#api-reference)
- [Query Period Behaviour](#query-period-behaviour)
- [Health Check](#health-check)
- [Graceful Shutdown](#graceful-shutdown)
- [Project Structure](#project-structure)
- [Database Schema](#database-schema)

---

## How It Works

MarketFlow connects to three exchange data sources over TCP and processes incoming ticker events through a concurrent pipeline:

```
Exchange 1 ──┐                          ┌── Worker (BTCUSDT)  ─┐
Exchange 2 ──┼──► Fan-Out (per symbol) ─┼── Worker (ETHUSDT)  ─┼──► Fan-In ──► Postgres
Exchange 3 ──┘                          └── Worker (SOLUSDT)  ─┘         └──► Redis
```

**Per tick:** each raw price update is written to two Redis structures simultaneously:
- `latest:{exchange}:{symbol}` — hash storing the most-recent price, used by `/prices/latest/*`
- `ts:{exchange}:{symbol}` — sorted set with a 70-second rolling window of individual ticks, used by sub-minute stat queries

**Every 60 seconds:** each per-symbol worker flushes its in-memory accumulator (avg, min, max, count) to PostgreSQL as a single aggregated row.

**Queries** are split at the 60-second boundary — see [Query Period Behaviour](#query-period-behaviour).

---

## Prerequisites

| Requirement | Version |
|---|---|
| Go | 1.21+ |
| PostgreSQL | 14+ |
| Redis | 7+ |
| Docker (for exchange simulators) | 20+ |

---

## Quick Start

### 1. Clone and build

```sh
git clone https://github.com/sulte4/marketflow
cd marketflow
go build -o marketflow .
```

### 2. Set up environment

Create a `.env` file in the project root (or `cmd/.env`):

```env
# Exchange TCP endpoints
EXCHANGE1=40101
EXCHANGE2=40102
EXCHANGE3=40103

# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=secret
DB_NAME=marketflow

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# HTTP server
HTTP_ADDR=8080
```

### 3. Run the database migration

```sh
psql -U postgres -d marketflow -f migrations/000001_create_price_stats.up.sql
```

### 4. Start the exchange simulators

```sh
docker load -i exchanges/exchange1_amd64.tar
docker load -i exchanges/exchange2_amd64.tar
docker load -i exchanges/exchange3_amd64.tar

docker run -p 40101:40101 --name exchange1 -d exchange1-amd64
docker run -p 40102:40102 --name exchange2 -d exchange2-amd64
docker run -p 40103:40103 --name exchange3 -d exchange3-amd64
```

> **Verify** a simulator is running before starting the app:
> ```sh
> nc 127.0.0.1 40101
> # You should see JSON ticker lines scrolling by
> ```

### 5. Run MarketFlow

```sh
./marketflow
# or override the port:
./marketflow --port 9090
```

```sh
./marketflow --help

Usage:
  marketflow [--port <N>]
  marketflow --help

Options:
  --port N     Port number
```

---

## Configuration

All configuration is read from environment variables. MarketFlow automatically loads a `.env` file from the project root, or falls back to `cmd/.env` if the root file is absent.

| Variable | Required | Description |
|---|---|---|
| `EXCHANGE1` | ✅ | TCP port for exchange source 1 (e.g. `40101`) |
| `EXCHANGE2` | ✅ | TCP port for exchange source 2 (e.g. `40102`) |
| `EXCHANGE3` | ✅ | TCP port for exchange source 3 (e.g. `40103`) |
| `DB_HOST` | ✅ | PostgreSQL host |
| `DB_PORT` | ✅ | PostgreSQL port (typically `5432`) |
| `DB_USER` | ✅ | PostgreSQL username |
| `DB_PASSWORD` | ✅ | PostgreSQL password |
| `DB_NAME` | ✅ | PostgreSQL database name |
| `REDIS_HOST` | ✅ | Redis host |
| `REDIS_PORT` | ✅ | Redis port (typically `6379`) |
| `HTTP_ADDR` | ✅ | HTTP server port (e.g. `8080`) |

The `--port` CLI flag overrides `HTTP_ADDR` when provided.

---

## Running the Exchanges

The exchange simulators are provided as Docker images that stream real-time ticker data for the following trading pairs:

- `BTCUSDT`
- `ETHUSDT`
- `SOLUSDT`
- `DOGEUSDT`
- `TONUSDT`

### Load images (choose your architecture)

```sh
# AMD64
docker load -i exchanges/exchange1_amd64.tar
docker load -i exchanges/exchange2_amd64.tar
docker load -i exchanges/exchange3_amd64.tar

# ARM64 (Apple Silicon, AWS Graviton)
docker load -i exchanges/exchange1_arm64.tar
docker load -i exchanges/exchange2_arm64.tar
docker load -i exchanges/exchange3_arm64.tar
```

### Start containers

```sh
docker run -p 40101:40101 --name exchange1 -d exchange1-amd64
docker run -p 40102:40102 --name exchange2 -d exchange2-amd64
docker run -p 40103:40103 --name exchange3 -d exchange3-amd64
```

### Stop and restart (simulates failover)

MarketFlow will automatically attempt to reconnect if an exchange goes down. You can test this by restarting a container:

```sh
docker restart exchange1
```

---

## API Reference

All responses share a common envelope:

```json
{
  "data": { ... },
  "error": "human-readable message (only present on errors)"
}
```

### Latest Price

**Get the latest price for a symbol across all exchanges**

```
GET /prices/latest/{symbol}
```

```sh
curl http://localhost:8080/prices/latest/BTCUSDT
```

```json
{
  "data": {
    "symbol": "BTCUSDT",
    "source": "exchange2",
    "price": 67431.25,
    "timestamp": 1704067245
  }
}
```

Returns the tick with the most-recent timestamp across all exchanges.

---

**Get the latest price from a specific exchange**

```
GET /prices/latest/{exchange}/{symbol}
```

```sh
curl http://localhost:8080/prices/latest/exchange1/BTCUSDT
```

```json
{
  "data": {
    "symbol": "BTCUSDT",
    "source": "exchange1",
    "price": 67389.50,
    "timestamp": 1704067243
  }
}
```

---

### Highest Price

**Highest price across all time (all exchanges)**

```
GET /prices/highest/{symbol}
```

**Highest price within a period**

```
GET /prices/highest/{symbol}?period={duration}
```

**Highest price from a specific exchange within a period**

```
GET /prices/highest/{exchange}/{symbol}?period={duration}
```

```sh
# All-time high across all exchanges
curl http://localhost:8080/prices/highest/BTCUSDT

# Highest in the last 30 seconds (Redis only)
curl "http://localhost:8080/prices/highest/BTCUSDT?period=30s"

# Highest in the last 5 minutes from exchange1
curl "http://localhost:8080/prices/highest/exchange1/BTCUSDT?period=5m"

# Highest in the last 1 minute 30 seconds (hybrid: 60s Postgres + 30s Redis)
curl "http://localhost:8080/prices/highest/BTCUSDT?period=1m30s"
```

```json
{
  "data": {
    "symbol": "BTCUSDT",
    "source": "exchange1",
    "price": 68100.00,
    "timestamp": 1704067120
  }
}
```

When no data exists for the requested period:

```json
{
  "data": null
}
```

---

### Lowest Price

```
GET /prices/lowest/{symbol}
GET /prices/lowest/{symbol}?period={duration}
GET /prices/lowest/{exchange}/{symbol}?period={duration}
```

```sh
curl "http://localhost:8080/prices/lowest/ETHUSDT?period=10m"
curl "http://localhost:8080/prices/lowest/exchange3/SOLUSDT?period=1m"
```

```json
{
  "data": {
    "symbol": "ETHUSDT",
    "source": "exchange3",
    "price": 3201.75,
    "timestamp": 1704066900
  }
}
```

---

### Average Price

```
GET /prices/average/{symbol}
GET /prices/average/{symbol}?period={duration}
GET /prices/average/{exchange}/{symbol}?period={duration}
```

```sh
curl "http://localhost:8080/prices/average/BTCUSDT?period=5m"
curl "http://localhost:8080/prices/average/exchange2/DOGEUSDT?period=1m30s"
```

```json
{
  "data": {
    "symbol": "BTCUSDT",
    "source": "exchange2",
    "price": 67512.88,
    "timestamp": 1704067260
  }
}
```

The average is a true **tick-count-weighted mean** across both stores — not an average of averages.

---

### Mode Switching

**Switch to Test Mode** (use locally generated synthetic data)

```
POST /mode/test
```

```sh
curl -X POST http://localhost:8080/mode/test
```

```json
{"status": "test mode activated"}
```

**Switch to Live Mode** (fetch data from exchange simulators)

```
POST /mode/live
```

```sh
curl -X POST http://localhost:8080/mode/live
```

```json
{"status": "live mode activated"}
```

---

### Health Check

```
GET /health
```

```sh
curl http://localhost:8080/health
```

Returns `200 OK` when all components are healthy:

```json
{
  "data": {
    "status": "ok",
    "postgres": { "status": "ok" },
    "redis":    { "status": "ok" }
  }
}
```

Returns `503 Service Unavailable` when any component is degraded:

```json
{
  "data": {
    "status": "degraded",
    "postgres": { "status": "ok" },
    "redis": {
      "status": "down",
      "message": "dial tcp [::1]:6379: connect: connection refused"
    }
  }
}
```

| `status` value | HTTP code | Meaning |
|---|---|---|
| `"ok"` | 200 | All components healthy |
| `"degraded"` | 503 | One component down |
| `"down"` | 503 | All components down |

Both PostgreSQL and Redis are **always checked independently** — a failure in one never prevents the other from being reported.

---

### Period Format

The `period` query parameter accepts any duration string understood by Go's [`time.ParseDuration`](https://pkg.go.dev/time#ParseDuration):

| Example | Meaning |
|---|---|
| `1s`, `5s`, `30s` | Seconds |
| `1m`, `3m`, `5m` | Minutes |
| `1m30s`, `2m45s` | Mixed |

Omitting `period` returns all-time data from PostgreSQL combined with the live current-batch data from Redis.

---

## Query Period Behaviour

MarketFlow splits every stat query at the **60-second batch boundary**, routing each portion to the optimal store:

```
period = 5s    ──► Redis only          (live 5-second window)
period = 30s   ──► Redis only          (live 30-second window)
period = 60s   ──► Postgres only       (last completed minute batch)
period = 90s   ──► Postgres 60s        (last completed minute)
                   + Redis 30s         (current accumulating batch)
period = 5m    ──► Postgres 300s       (last 5 completed minutes)
period = 5m30s ──► Postgres 300s + Redis 30s
period = 0     ──► Postgres all-time + Redis last 70s (catch live batch)
```

This design means:
- **Sub-minute periods** (`< 60s`) are served entirely from Redis with millisecond-level precision.
- **Minute-boundary periods** are served entirely from PostgreSQL using pre-computed aggregates.
- **Mixed periods** combine both stores — the Postgres portion covers completed minutes, Redis covers the live remainder.

**Weighted average accuracy:** when combining both stores, the service uses the PostgreSQL batch's commit timestamp as the Redis lower-bound cutoff, ensuring that ticks already counted inside a batch are never double-counted in the average calculation.

---

## Graceful Shutdown

MarketFlow handles `SIGTERM` and `SIGINT` with a six-phase ordered shutdown that guarantees no data loss:

```
Signal received
  │
  ├─ Phase 1: Cancel application context  ──► all goroutines begin winding down
  ├─ Phase 2: HTTP server shutdown        ──► drain in-flight requests (15s timeout)
  ├─ Phase 3: Close exchange connections  ──► unblock TCP stream readers
  ├─ Phase 4: Wait for all workers        ──► final 60s batches flushed to Postgres
  ├─ Phase 5: Close PostgreSQL            ──► safe: no writes in flight
  └─ Phase 6: Close Redis                 ──► safe: no writes in flight
```

Sending `Ctrl+C` or `kill <pid>` produces output like:

```
time=... level=INFO msg="shutdown signal received" signal=interrupt
time=... level=INFO msg="shutdown phase 1/6: cancelling application context"
time=... level=INFO msg="shutdown phase 2/6: shutting down HTTP server"
time=... level=INFO msg="shutdown phase 3/6: closing exchange TCP connections"
time=... level=INFO msg="shutdown phase 4/6: waiting for market processor and all workers to complete"
time=... level=INFO msg="saving final batched data on worker exit" symbol=BTCUSDT exchange=exchange1 count=42
time=... level=INFO msg="market processor and all workers completed"
time=... level=INFO msg="shutdown phase 5/6: closing PostgreSQL connection"
time=... level=INFO msg="shutdown phase 6/6: closing Redis connection"
time=... level=INFO msg="MarketFlow graceful shutdown complete" total_shutdown_time=2.341s
```

---

## Project Structure

```
marketflow/
├── cmd/
│   └── main.go                          # Entry point, wiring, shutdown sequence
│
├── internal/
│   ├── core/
│   │   ├── domain/
│   │   │   ├── ticker.go                # Ticker, TickerFilter types
│   │   │   ├── aggregatedTicker.go      # AggregatedTicker — 60s batch accumulator
│   │   │   ├── health.go                # HealthResult, ComponentHealth types
│   │   │   └── errors.go                # Sentinel errors (ErrNoData, etc.)
│   │   └── service/
│   │       ├── processor.go             # MarketProcessor: fan-out/fan-in pipeline
│   │       └── market.go                # MarketService: hybrid query logic
│   │
│   ├── ports/
│   │   ├── input.go                     # MarketService interface
│   │   └── output.go                    # TickerCache, TickerRepository, ExchangeSource interfaces
│   │
│   └── adapters/
│       ├── primary/
│       │   └── web/
│       │       ├── handler.go           # HTTP handlers
│       │       └── router.go            # Route registration
│       └── secondary/
│           ├── exchange/
│           │   └── exchange.go          # TCP exchange adapter
│           ├── postgres/
│           │   └── repo.go              # PostgreSQL repository
│           └── redisadapter/
│               └── redis.go             # Redis cache adapter
│
├── migrations/
│   └── 000001_create_price_stats.up.sql
│
├── exchanges/                           # Exchange simulator Docker images
│   ├── exchange1_amd64.tar
│   ├── exchange1_arm64.tar
│   └── ...
│
├── pkg/
│   └── config/
│       └── config.go                    # Environment variable loading
│
├── go.mod
└── README.md
```

---

## Database Schema

### `aggregated_ticker`

Stores one row per trading pair per exchange per 60-second window.

| Column | Type | Description |
|---|---|---|
| `id` | `BIGSERIAL` | Auto-incrementing primary key |
| `pair_name` | `VARCHAR(20)` | Trading pair symbol (e.g. `BTCUSDT`) |
| `exchange` | `VARCHAR(50)` | Source exchange name (e.g. `exchange1`) |
| `timestamp` | `TIMESTAMPTZ` | When the batch was committed |
| `average_price` | `NUMERIC(20,8)` | Tick-count-weighted average price for the window |
| `min_price` | `NUMERIC(20,8)` | Minimum price observed in the window |
| `max_price` | `NUMERIC(20,8)` | Maximum price observed in the window |
| `count` | `INTEGER` | Number of raw ticks in the window (used for weighted averages) |

**Indexes**

```sql
-- Queries by pair name sorted by time (most common pattern)
CREATE INDEX idx_aggregated_ticker_pair_time
    ON aggregated_ticker (pair_name, timestamp DESC);

-- Queries filtered by both pair name and exchange
CREATE INDEX idx_aggregated_ticker_pair_exchange_time
    ON aggregated_ticker (pair_name, exchange, timestamp DESC);
```

### Redis Key Spaces

| Key pattern | Type | TTL | Contents |
|---|---|---|---|
| `latest:{exchange}:{symbol}` | Hash | 70 s | `price`, `ts` — the single most-recent tick |
| `ts:{exchange}:{symbol}` | Sorted Set | 70 s | All ticks in the last 70 seconds; score = Unix ms |

---

## Dependencies

| Package | Purpose |
|---|---|
| `github.com/lib/pq` | PostgreSQL driver |
| `github.com/redis/go-redis/v9` | Redis client |
| `github.com/joho/godotenv` | `.env` file loading |

No application-level frameworks are used. The HTTP layer is built on the Go standard library's `net/http` with the 1.22+ pattern-matching mux.