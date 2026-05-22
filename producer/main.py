import asyncio
import logging

from config import settings
from kafka_producer import TradeKafkaProducer
from ws_client import BinanceWSClient

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s",
)


async def main():
    ws_client = BinanceWSClient()
    kafka_producer = TradeKafkaProducer()
    published = 0

    logging.info(
        "producer_started kafka=%s topic=%s",
        settings.KAFKA_BOOTSTRAP_SERVERS,
        settings.KAFKA_TOPIC,
    )

    async for event in ws_client.connect():
        if kafka_producer.publish(event):
            published += 1
            if published % 500 == 0:
                logging.info("trades_published=%s", published)


if __name__ == "__main__":
    asyncio.run(main())
