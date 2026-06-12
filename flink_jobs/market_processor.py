import json
import os
from datetime import datetime, timezone

from clickhouse_driver import Client
from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction, KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor

EMA_ALPHA = 0.3
# trades are grouped into 30-second buckets for aggregation
WINDOW_SECONDS = 30

#Raw parsed trade: (symbol, price, quantity, trade_id, is_buyer_maker, event_time_ms)
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

#WINDOW_STATS_TYPE — after windowing: adds open, close, high, low, VWAP, trade count, volume
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
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.INT(),
    ]
)

ENRICHED_STATS_TYPE = Types.TUPLE(
    [
        Types.STRING(),   # symbol
        Types.LONG(),     # window_start
        Types.LONG(),     # window_end
        Types.DOUBLE(),   # open
        Types.DOUBLE(),   # close
        Types.DOUBLE(),   # high
        Types.DOUBLE(),   # low
        Types.DOUBLE(),   # avg_price
        Types.DOUBLE(),   # vwap
        Types.INT(),      # trade_count
        Types.DOUBLE(),   # volume
        Types.DOUBLE(),   # buy_volume
        Types.DOUBLE(),   # sell_volume
        Types.DOUBLE(),   # buy_sell_ratio
        Types.DOUBLE(),   # trade_imbalance
        Types.DOUBLE(),   # trade_frequency
        Types.DOUBLE(),   # moving_avg
    ]
)

ALERT_TYPE = Types.TUPLE(
    [
        Types.STRING(),   # symbol
        Types.STRING(),   # alert_type
        Types.STRING(),   # severity
        Types.DOUBLE(),   # current_volume
        Types.DOUBLE(),   # historical_avg
        Types.LONG(),     # anomaly_time
        Types.LONG(),     # alert_time
    ]
)

# creates a ClickHouse DB connection, reading credentials from environment variables 
def _clickhouse_client():
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "admin"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "secret"),
        database=os.getenv("CLICKHOUSE_DB", "crypto"),
    )

#  converts Unix millisecond timestamps to Python datetime
def _ms_to_datetime(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)

# first transformation : parses raw JSON trade to a python tuple
class TradeParser(MapFunction):

    def map(self, value):
        payload = json.loads(value)
        return (
            payload["symbol"],
            float(payload["price"]),
            float(payload["quantity"]),
            int(payload["trade_id"]),
            bool(payload["is_buyer_maker"]),   #True = buyer side, False = seller side
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


#Flink collects 30 seconds of trades per symbol, bcoz of line 236 window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS)
class TradeWindowAggregator(ProcessWindowFunction):

    def process(self, key, context, elements):
        trades = list(elements)
        if not trades:
            return []

# creates list
        prices = [trade[1] for trade in trades]
        quantities = [trade[2] for trade in trades]
        volume = sum(quantities)
        price_volume = sum(trade[1] * trade[2] for trade in trades)
        is_buyer_makers = [trade[4] for trade in trades]
        avg_price = sum(prices) / len(prices)
        vwap = price_volume / volume if volume > 0 else avg_price

        # BUY/SELL PRESSURE CALCULATION
        buy_volume = sum(
            quantities[i] for i in range(len(trades)) 
            if is_buyer_makers[i]  # True = buyer
        )
        sell_volume = sum(
            quantities[i] for i in range(len(trades)) 
            if not is_buyer_makers[i]  # False = seller
        )
        buy_sell_ratio = buy_volume / sell_volume if sell_volume > 0 else buy_volume

        total_volume_side = buy_volume + sell_volume

        trade_imbalance = (
            (buy_volume - sell_volume) / total_volume_side
            if total_volume_side > 0
            else 0
        )
        trade_frequency = len(trades) / WINDOW_SECONDS

        return [
            (
                key,
                context.window().start, 
                context.window().end,
                prices[0],  # open & close prices in a window
                prices[-1],
                max(prices),  #5
                min(prices),
                avg_price,
                vwap,
                len(trades),
                volume,   #10
                buy_volume,
                sell_volume,
                buy_sell_ratio,
                trade_imbalance,
                trade_frequency, #15
            )
        ]

# With alpha=0.3, the Exponential Moving Average is: 30% current window price + 70% historical EMA.
class StatefulEMA(KeyedProcessFunction):

    def open(self, runtime_context):

        descriptor = ValueStateDescriptor(
            "ema_state",
            Types.DOUBLE()
        )

        self.ema_state = runtime_context.get_state(
            descriptor
        )

    def process_element(self, value, ctx):

        avg_price = value[7]

        previous_ema = self.ema_state.value()

        if previous_ema is None:
            previous_ema = avg_price

        moving_avg = (
            EMA_ALPHA * avg_price
            + (1 - EMA_ALPHA) * previous_ema
        )

        self.ema_state.update(moving_avg)

        yield (*value, moving_avg)
    
class StatefulVolumeDetector(KeyedProcessFunction):

    def open(self, runtime_context):

        descriptor = ListStateDescriptor(
            "volume_history",
            Types.DOUBLE()
        )

        self.volume_state = runtime_context.get_list_state(
            descriptor
        )

    def process_element(self, value, ctx):
        symbol = value[0]
        current_volume = value[10]
        buy_sell_ratio = value[13]
        history = list(self.volume_state.get())

        avg_volume = sum(history) /len(history ) if history else 0.0  # safe default

        if len(history) >= 3:

            severity = None

            if current_volume > avg_volume * 5:
                severity = "HIGH"

            elif current_volume > avg_volume * 3:
                severity = "MEDIUM"

            elif current_volume > avg_volume * 1.5:
                severity = "LOW"

            if severity:

                yield (
                    symbol,
                    "VOLUME_SPIKE",
                    severity,
                    current_volume,
                    avg_volume,
                    value[2],
                    int(datetime.utcnow().timestamp() * 1000)
                )
        
        if buy_sell_ratio > 2:

            yield (
                symbol,
                "BUY_PRESSURE",
                "MEDIUM",
                current_volume,
                avg_volume,
                value[2],
                int(datetime.utcnow().timestamp() * 1000)
            )

        if buy_sell_ratio < 0.5:

            yield (
                symbol,
                "SELL_PRESSURE",
                "MEDIUM",
                current_volume,
                avg_volume,
                value[2],
                int(datetime.utcnow().timestamp() * 1000)
            )

        history.append(current_volume)
        if len(history) > 10:
            history.pop(0)
        self.volume_state.update(history)

        print(
            f"Symbol={symbol}, "
            f"Current={current_volume}, "
            f"Avg={avg_volume}"
        )



class ProcessedClickHouseSink(MapFunction):

    def open(self, runtime_context):
        self._client = _clickhouse_client()
# it inserts one row per window (every 30s per symbol)
    def map(self, value):
        self._client.execute(
            "INSERT INTO processed_market_data "
            "(window_start, window_end, symbol, "
            "open_price, close_price, high_price, low_price, "
            "avg_price, vwap, moving_avg_price, trade_count, total_volume," 
            "buy_volume, sell_volume, buy_sell_ratio, trade_imbalance, trade_frequency) VALUES",
            [
                (
                    _ms_to_datetime(value[1]), #window start time
                    _ms_to_datetime(value[2]), #window end time
                    value[0], #symbol
                    value[3], #open price
                    value[4], #close price
                    value[5], #high price
                    value[6], #low price
                    value[7], #avg price
                    value[8], #vwap 
                    value[16], #moving average price
                    value[9], #trade count
                    value[10], #total volume
                    value[11], #buy volume
                    value[12], #sell volume
                    value[13], #buy/sell ratio
                    value[14], #trade imbalance
                    value[15], #trade frequency
                )
            ],
        )
        return value

    def close(self):
        self._client.disconnect()

class AlertClickHouseSink(MapFunction):

    def open(self, runtime_context):
        self._client = _clickhouse_client()

    def map(self, value):

        self._client.execute(
            """
            INSERT INTO market_alerts
            (
                symbol,
                alert_type,
                severity,
                current_volume,
                historical_avg,
                anomaly_time,
                alert_time
            )
            VALUES
            """,
            [
                (
                    value[0],
                    value[1],
                    value[2],
                    value[3],
                    value[4],
                    _ms_to_datetime(value[5]),
                    _ms_to_datetime(value[6]),
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

    processed = (
        trades.key_by(lambda trade: trade[0])
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
        .process(TradeWindowAggregator(), output_type=WINDOW_STATS_TYPE)
        .key_by(lambda x: x[0])
        .process(StatefulEMA(), output_type=ENRICHED_STATS_TYPE)
    )

    processed.map(ProcessedClickHouseSink(),output_type=ENRICHED_STATS_TYPE)

    alerts = (
        processed.key_by(lambda x: x[0])
        .process(StatefulVolumeDetector(),output_type=ALERT_TYPE)
    )

    alerts.map(AlertClickHouseSink(),output_type=ALERT_TYPE)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "2")))
    env.enable_checkpointing(60_000)
    build_pipeline(env)
    env.execute("market_processor")


if __name__ == "__main__":
    main()
