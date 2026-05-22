-- This runs automatically when ClickHouse starts for the first time

CREATE DATABASE IF NOT EXISTS crypto;

CREATE TABLE IF NOT EXISTS crypto.raw_trades
(
    event_time      DateTime64(3),
    symbol          String,
    price           Float64,
    quantity        Float64,
    trade_id        Int64,
    is_buyer_maker  Bool,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (symbol, event_time)
TTL toDateTime(event_time) + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS crypto.processed_market_data
(
    window_start        DateTime64(3),
    window_end          DateTime64(3),
    symbol              String,
    open_price          Float64,
    close_price         Float64,
    high_price          Float64,
    low_price           Float64,
    avg_price           Float64,
    vwap                Float64,
    moving_avg_price    Float64,
    trade_count         Int32,
    total_volume        Float64,
    processed_at        DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (symbol, window_start)
TTL toDateTime(window_start) + INTERVAL 7 DAY;
