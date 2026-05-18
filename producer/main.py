import asyncio
import logging

from ws_client import BinanceWSClient
from config import settings


logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(message)s"
)


async def main():

    client = BinanceWSClient()

    async for event in client.connect():

        print(event)


if __name__ == "__main__":

    asyncio.run(main())