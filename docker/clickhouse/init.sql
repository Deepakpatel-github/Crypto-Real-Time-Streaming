CREATE DATABASE IF NOT EXISTS crypto;

USE crypto;

-- Final Persistent Table

CREATE TABLE IF NOT EXISTS market_trades
(
    symbol String,
    avg_price Float64,
    event_time UInt64,
    trade_count UInt32,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (symbol, event_time);

-- Kafka Source Table

CREATE TABLE IF NOT EXISTS kafka_market_trades
(
    symbol String,
    avg_price Float64,
    event_time UInt64,
    trade_count UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'processed_market_data',
    kafka_group_name = 'clickhouse-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- Materialized View

CREATE MATERIALIZED VIEW IF NOT EXISTS market_trades_mv
TO market_trades
AS
SELECT
    symbol,
    avg_price,
    event_time,
    trade_count
FROM kafka_market_trades;