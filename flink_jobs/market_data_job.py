import json
import logging

from config.settings import settings

from pyflink.common import Types, Row
from pyflink.common.time import Duration

from pyflink.common.watermark_strategy import (
    WatermarkStrategy,
    TimestampAssigner
)

from pyflink.datastream import (
    StreamExecutionEnvironment
)

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaSink,
    KafkaRecordSerializationSchema
)

from pyflink.datastream.connectors.base import (
    DeliveryGuarantee
)

from pyflink.common.serialization import (
    SimpleStringSchema
)

from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    Time
)

from flink_jobs.transformations.enricher import (
    enrich_trade
)

class EventTimestampAssigner(
    TimestampAssigner
):

    def extract_timestamp(
        self,
        value,
        record_timestamp
    ):

        return value[2]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"

KAFKA_TOPIC_RAW = "raw_market_data"


env = StreamExecutionEnvironment \
    .get_execution_environment()

env.set_parallelism(2)


source = KafkaSource.builder() \
    .set_bootstrap_servers(
        KAFKA_BOOTSTRAP_SERVERS
    ) \
    .set_topics(
        KAFKA_TOPIC_RAW
    ) \
    .set_group_id(
        "market-processor"
    ) \
    .set_starting_offsets(
        KafkaOffsetsInitializer.latest()
    ) \
    .set_value_only_deserializer(
        SimpleStringSchema()
    ) \
    .build()


stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "kafka-source"
)


parsed = stream.map(
    lambda x: json.loads(x)
)


enriched = parsed.map(
    enrich_trade
)


window_input = enriched.map(
    lambda event: Row(
        event.symbol,
        event.price,
        event.event_time,
        1
    ),
    output_type=Types.ROW(
        [
            Types.STRING(),
            Types.FLOAT(),
            Types.LONG(),
            Types.INT()
        ]
    )
)


aggregated = window_input


# json_output = aggregated.map(
#     lambda x: json.dumps({
#         "symbol": x[0],
#         "avg_price": x[1],
#         "event_time": x[2],
#         "trade_count": x[3]
#     })
# )

json_output = aggregated.map(

    lambda x: json.dumps({
        "symbol": x[0],
        "avg_price": x[1],
        "event_time": x[2],
        "trade_count": x[3]
    }),

    output_type=Types.STRING()
)

sink = KafkaSink.builder() \
    .set_bootstrap_servers(
        "kafka:29092"
    ) \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic(
                "processed_market_data"
            )
            .set_value_serialization_schema(
                SimpleStringSchema()
            )
            .build()
    ) \
    .set_delivery_guarantee(
        DeliveryGuarantee.AT_LEAST_ONCE
    ) \
    .build()

json_output.sink_to(sink)

env.execute(
    "crypto-market-stream-job"
)