# Crypto-Real-Time-Streaming

Real-time crypto trade pipeline: **Binance WebSocket → Kafka → Apache Flink → ClickHouse**.

Two ClickHouse tables make the contrast explicit:

| Table | What it stores | Who writes it |
|-------|----------------|---------------|
| `crypto.raw_trades` | Every trade tick as ingested | Flink passthrough branch |
| `crypto.processed_market_data` | 30s window aggregates + moving average | Flink window branch |

## Architecture

```
Binance WS  →  producer  →  Kafka (raw_trades)
                                ↓
                         Flink market_processor
                          ├─→ raw_trades (ClickHouse)
                          └─→ processed_market_data (ClickHouse)
                                30s tumbling windows per symbol
                                avg_price, vwap, moving_avg_price, OHLC
```

## Prerequisites

- Docker Engine 24+
- Docker Compose v2
- ~4 GB free RAM
- Outbound internet (Binance WebSocket)

## Quick start

```bash
cd docker

# Fresh install (drops old ClickHouse volume if schema changed)
docker compose down -v
docker compose up -d --build
```

Services:

| Service | URL / port |
|---------|------------|
| Kafka UI | http://localhost:8080 |
| Flink Web UI | http://localhost:8081 |
| ClickHouse HTTP | http://localhost:8123 |
| Kafka (host) | localhost:9092 |
| ClickHouse native | localhost:9000 |

Wait ~2 minutes for Zookeeper, Kafka, ClickHouse, Flink, job submission, then the producer.

```bash
docker compose ps
docker logs flink-job-submitter
docker logs crypto-producer --tail 20
```

## Local viewers (pgAdmin-style)

### Kafka — Kafka UI (included)

No extra install. Open **http://localhost:8080**:

1. Cluster **local** is preconfigured.
2. **Topics → raw_trades → Messages** — live JSON trades.
3. Compare message rate with ClickHouse `raw_trades` row count.

Optional desktop client: [Offset Explorer](https://www.kafkatool.com/) or [Redpanda Console](https://github.com/redpanda-data/console) pointing at `localhost:9092`.

### ClickHouse — built-in Play UI

Browser SQL UI (similar spirit to pgAdmin):

1. Open **http://localhost:8123/play**
2. Login: user `admin`, password `secret`
3. Run queries from the validation section below.

### ClickHouse — DBeaver (optional desktop)

1. Install [DBeaver](https://dbeaver.io/download/).
2. New connection → **ClickHouse**.
3. Host `localhost`, port `9000`, database `crypto`, user `admin`, password `secret`.

### ClickHouse — Tabix (optional web UI)

```bash
docker run -d --name tabix -p 8082:80 \
  -e CH_HOST=host.docker.internal \
  -e CH_PORT=8123 \
  spoonest/clickhouse-tabix-web-client
```

Open http://localhost:8082 (Linux: use host IP instead of `host.docker.internal` if needed).

## End-to-end validation

All commands assume you are in the `docker/` directory.

### 1. Kafka has messages

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw_trades \
  --from-beginning \
  --max-messages 5
```

### 2. Flink job is running

```bash
curl -s http://localhost:8081/jobs | python3 -m json.tool

curl -s http://localhost:8081/jobs/overview | python3 -m json.tool
```

In Flink UI: **http://localhost:8081** → job `market_processor` should be **RUNNING**.

### 3. Raw ingested data (ClickHouse)

```bash
docker exec clickhouse clickhouse-client \
  --user admin --password secret \
  --query "SELECT count() AS raw_trade_count FROM crypto.raw_trades"

docker exec clickhouse clickhouse-client \
  --user admin --password secret \
  --query "
    SELECT symbol, price, quantity, event_time
    FROM crypto.raw_trades
    ORDER BY event_time DESC
    LIMIT 10
  "
```

### 4. Processed data (Flink windows)

Allow at least **35 seconds** after trades flow so a 30s window can close.

```bash
docker exec clickhouse clickhouse-client \
  --user admin --password secret \
  --query "SELECT count() AS processed_window_count FROM crypto.processed_market_data"

docker exec clickhouse clickhouse-client \
  --user admin --password secret \
  --query "
    SELECT
      symbol,
      window_start,
      window_end,
      round(avg_price, 2) AS avg_price,
      round(moving_avg_price, 2) AS moving_avg_price,
      round(vwap, 2) AS vwap,
      trade_count,
      round(total_volume, 6) AS total_volume
    FROM crypto.processed_market_data
    ORDER BY window_start DESC
    LIMIT 10
  "
```

### 5. Ingested vs processed contrast

```bash
docker exec clickhouse clickhouse-client \
  --user admin --password secret \
  --query "
    SELECT
      (SELECT count() FROM crypto.raw_trades) AS ingested_trades,
      (SELECT count() FROM crypto.processed_market_data) AS processed_windows,
      round(
        (SELECT count() FROM crypto.raw_trades)
        / greatest((SELECT count() FROM crypto.processed_market_data), 1),
        1
      ) AS avg_trades_per_window
  "
```

Expected: `ingested_trades` grows quickly (every tick); `processed_windows` grows slowly (~2 rows per 30s for BTC + ETH).

Side-by-side per symbol:

```bash
docker exec clickhouse clickhouse-client \
  --user admin --password secret \
  --query "
    SELECT
      symbol,
      count() AS raw_rows,
      max(event_time) AS last_raw_event
    FROM crypto.raw_trades
    GROUP BY symbol
    ORDER BY symbol
  "

docker exec clickhouse clickhouse-client \
  --user admin --password secret \
  --query "
    SELECT
      symbol,
      count() AS window_rows,
      max(window_end) AS last_window,
      round(avg(moving_avg_price), 2) AS latest_moving_avg
    FROM crypto.processed_market_data
    GROUP BY symbol
    ORDER BY symbol
  "
```

## Team demo journey (~10 minutes)

Use this script when presenting to the team.

### Before the meeting

```bash
cd docker
docker compose down -v
docker compose up -d --build
```

Confirm all containers are up: `docker compose ps`.

### Minute 0–1: Story

Explain the flow: live Binance trades → Kafka buffer → Flink processes in real time → ClickHouse stores **raw** and **processed** separately.

### Minute 1–2: Kafka UI

1. Open http://localhost:8080
2. Show topic `raw_trades` and live messages (symbol, price, quantity).
3. Point out this is **ingested** data unchanged from the exchange.

### Minute 2–3: Flink UI

1. Open http://localhost:8081
2. Show running job `market_processor`.
3. Mention two branches: passthrough to `raw_trades`, 30s windows to `processed_market_data`.

### Minute 3–5: Raw vs processed in ClickHouse

Run in terminal (or ClickHouse Play UI):

```bash
docker exec clickhouse clickhouse-client --user admin --password secret --query "
  SELECT count() FROM crypto.raw_trades
"

docker exec clickhouse clickhouse-client --user admin --password secret --query "
  SELECT symbol, round(price, 2) AS price, event_time
  FROM crypto.raw_trades ORDER BY event_time DESC LIMIT 5
"
```

Wait 35s, then:

```bash
docker exec clickhouse clickhouse-client --user admin --password secret --query "
  SELECT symbol, window_start, round(avg_price, 2), round(moving_avg_price, 2), trade_count
  FROM crypto.processed_market_data
  ORDER BY window_start DESC LIMIT 5
"
```

Highlight: raw table has **many** rows per second; processed table has **one row per symbol per 30s window** with `moving_avg_price` computed by Flink.

### Minute 5–7: Contrast query live

```bash
docker exec clickhouse clickhouse-client --user admin --password secret --query "
  SELECT
    (SELECT count() FROM crypto.raw_trades) AS ingested,
    (SELECT count() FROM crypto.processed_market_data) AS processed
"
```

Refresh after 30s — ingested climbs fast, processed climbs slowly.

### Minute 7–10: Q&A / deep dive

- **Re-submit Flink job**: `docker compose run --rm flink-job-submitter`
- **Producer logs**: `docker logs -f crypto-producer`
- **Reset everything**: `docker compose down -v && docker compose up -d --build`

## Run producer locally (optional)

```bash
cp .env.example .env
pip install -r requirements.txt
cd producer
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 python main.py
```

Stack must be up (`docker compose up -d` from `docker/`).

## Project layout

```
├── docker/
│   ├── docker-compose.yml
│   ├── clickhouse/init.sql
│   └── flink/Dockerfile
├── flink_jobs/
│   ├── market_processor.py   # PyFlink job
│   └── submit_job.sh
├── producer/
│   ├── main.py
│   ├── ws_client.py
│   ├── kafka_producer.py
│   └── Dockerfile
└── requirements.txt          # local dev only
```

## Flink processing details

- **Window**: 30-second tumbling processing-time windows per `symbol`
- **Metrics**: open/high/low/close, `avg_price`, `vwap`, `trade_count`, `total_volume`
- **Moving average**: EMA across windows (`alpha = 0.3`) stored as `moving_avg_price`

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Empty ClickHouse tables | Wait 60s; check `docker logs crypto-producer` |
| No `processed_market_data` rows | Wait ≥35s for first window; check Flink job at :8081 |
| Flink job not submitted | `docker logs flink-job-submitter`; re-run `docker compose run --rm flink-job-submitter` |
| Old schema (`market_events`) | `docker compose down -v` then `up -d --build` |
| Kafka UI empty | Producer must be running; verify topic `raw_trades` |
| ClickHouse auth error | Use `--user admin --password secret` |

## Stop

```bash
cd docker
docker compose down
```

Remove volumes (clears ClickHouse data):

```bash
docker compose down -v
```
