import json
import logging

from confluent_kafka import Producer

from config import settings


logger = logging.getLogger(__name__)


class KafkaPublisher:

    def __init__(self):

        self.producer = Producer(
            {
                "bootstrap.servers":
                    settings.KAFKA_BOOTSTRAP_SERVERS,

                "acks": "all",

                "retries": 10,

                "enable.idempotence": True,

                "compression.type": "snappy",

                "linger.ms": 50
            }
        )

    def _delivery_report(
        self,
        err,
        msg
    ):

        if err:

            logger.error(
                "delivery_failed=%s",
                err
            )

            return

        logger.info(
            "delivered topic=%s partition=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset()
        )

    def publish(
        self,
        event
    ):

        symbol = event["data"]["s"]

        payload = json.dumps(
            event
        )

        self.producer.produce(
            topic=settings.KAFKA_TOPIC_RAW,

            key=symbol,

            value=payload,

            callback=self._delivery_report
        )

        self.producer.poll(0)

    def flush(self):

        self.producer.flush()