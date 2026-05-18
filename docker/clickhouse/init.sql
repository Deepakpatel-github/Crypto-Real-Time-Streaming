-- This runs automatically when ClickHouse starts for the first time

CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.market_events
(
    event_time      DateTime64(3),       -- millisecond precision
    symbol          String,
    price           Float64,
    quantity        Float64,
    trade_id        Int64,
    is_buyer_maker  Bool,
    window_start    Nullable(DateTime64(3)),
    window_end      Nullable(DateTime64(3)),
    avg_price       Nullable(Float64),
    trade_count     Nullable(Int32),
    is_anomaly      Bool DEFAULT false,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (symbol, event_time)
TTL event_time + INTERVAL 7 DAY;   -- auto-purge after 7 days