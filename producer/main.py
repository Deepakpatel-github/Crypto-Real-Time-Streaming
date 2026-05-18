import asyncio
import logging

from ws_client import BinanceWSClient
from kafka_publisher import KafkaPublisher
from config import settings


logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s"
)


async def main():

    ws_client = BinanceWSClient()
    publisher = KafkaPublisher()

    try:
        async for event in ws_client.connect():

            publisher.publish(
                event
            )
    finally:
        publisher.flush()


if __name__ == "__main__":

    asyncio.run(main())