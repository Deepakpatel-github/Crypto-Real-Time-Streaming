import json
import os
from datetime import datetime, timezone

from clickhouse_driver import Client
from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows

EMA_ALPHA = 0.3
WINDOW_SECONDS = 30

TRADE_TYPE = Types.TUPLE(
    [
        Types.STRING(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.LONG(),
        Types.BOOLEAN(),
        Types.LONG(),
    ]
)

WINDOW_STATS_TYPE = Types.TUPLE(
    [
        Types.STRING(),
        Types.LONG(),
        Types.LONG(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.INT(),
        Types.DOUBLE(),
    ]
)

ENRICHED_STATS_TYPE = Types.TUPLE(
    [
        Types.STRING(),
        Types.LONG(),
        Types.LONG(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.INT(),
        Types.DOUBLE(),
        Types.DOUBLE(),
    ]
)


def _clickhouse_client():
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "admin"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "secret"),
        database=os.getenv("CLICKHOUSE_DB", "crypto"),
    )


def _ms_to_datetime(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


class TradeParser(MapFunction):

    def map(self, value):
        payload = json.loads(value)
        return (
            payload["symbol"],
            float(payload["price"]),
            float(payload["quantity"]),
            int(payload["trade_id"]),
            bool(payload["is_buyer_maker"]),
            int(payload["event_time_ms"]),
        )


class RawClickHouseSink(MapFunction):

    def open(self, runtime_context):
        self._client = _clickhouse_client()
        self._buffer = []
        self._batch_size = 100

    def map(self, value):
        self._buffer.append(
            (
                _ms_to_datetime(value[5]),
                value[0],
                value[1],
                value[2],
                value[3],
                value[4],
            )
        )
        if len(self._buffer) >= self._batch_size:
            self._flush()
        return value

    def _flush(self):
        if not self._buffer:
            return
        self._client.execute(
            "INSERT INTO raw_trades "
            "(event_time, symbol, price, quantity, trade_id, is_buyer_maker) VALUES",
            self._buffer,
        )
        self._buffer = []

    def close(self):
        self._flush()
        self._client.disconnect()


class TradeWindowAggregator(ProcessWindowFunction):

    def process(self, key, context, elements):
        trades = list(elements)
        if not trades:
            return []

        prices = [trade[1] for trade in trades]
        quantities = [trade[2] for trade in trades]
        volume = sum(quantities)
        price_volume = sum(trade[1] * trade[2] for trade in trades)
        avg_price = sum(prices) / len(prices)
        vwap = price_volume / volume if volume > 0 else avg_price

        return [
            (
                key,
                context.window().start,
                context.window().end,
                prices[0],
                prices[-1],
                max(prices),
                min(prices),
                avg_price,
                vwap,
                len(trades),
                volume,
            )
        ]


class MovingAverageEnricher(MapFunction):

    def open(self, runtime_context):
        self._ema_by_symbol = {}

    def map(self, value):
        symbol = value[0]
        avg_price = value[7]
        previous_ema = self._ema_by_symbol.get(symbol, avg_price)
        moving_avg = (EMA_ALPHA * avg_price) + ((1 - EMA_ALPHA) * previous_ema)
        self._ema_by_symbol[symbol] = moving_avg
        return (*value, moving_avg)


class ProcessedClickHouseSink(MapFunction):

    def open(self, runtime_context):
        self._client = _clickhouse_client()

    def map(self, value):
        self._client.execute(
            "INSERT INTO processed_market_data "
            "(window_start, window_end, symbol, "
            "open_price, close_price, high_price, low_price, "
            "avg_price, vwap, moving_avg_price, trade_count, total_volume) VALUES",
            [
                (
                    _ms_to_datetime(value[1]),
                    _ms_to_datetime(value[2]),
                    value[0],
                    value[3],
                    value[4],
                    value[5],
                    value[6],
                    value[7],
                    value[8],
                    value[11],
                    value[9],
                    value[10],
                )
            ],
        )
        return value

    def close(self):
        self._client.disconnect()


def build_pipeline(env):
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "raw_trades")
    group_id = os.getenv("FLINK_KAFKA_GROUP", "flink-market-processor")

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(topic)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    trades = (
        env.from_source(
            kafka_source, WatermarkStrategy.no_watermarks(), "kafka_raw_trades"
        )
        .map(TradeParser(), output_type=TRADE_TYPE)
    )

    trades.map(RawClickHouseSink(), output_type=TRADE_TYPE)

    (
        trades.key_by(lambda trade: trade[0])
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(TradeWindowAggregator(), output_type=WINDOW_STATS_TYPE)
        .map(MovingAverageEnricher(), output_type=ENRICHED_STATS_TYPE)
        .map(ProcessedClickHouseSink(), output_type=ENRICHED_STATS_TYPE)
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "2")))
    env.enable_checkpointing(60_000)
    build_pipeline(env)
    env.execute("market_processor")


if __name__ == "__main__":
    main()
