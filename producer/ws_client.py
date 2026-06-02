import asyncio
import json
import logging
import websockets # type: ignore

from config import settings

logger = logging.getLogger(__name__)


class BinanceWSClient:

# The Trade Streams push raw trade information; each trade has a unique buyer and seller.
# Stream Name: <symbol>@trade
    def __init__(self):
        self.streams = [
            "btcusdt@trade",
            "ethusdt@trade"
        ]

    def _build_url(self):

        stream_string = "/".join(self.streams)

        return (
            f"{settings.BINANCE_WS_URL}"
            f"?streams={stream_string}"
        )

    async def connect(self):

        while True:

            try:

                url = self._build_url()

                logger.info("connecting=%s", url)

                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20
                ) as websocket:

                    logger.info("websocket_connected")

                    async for message in websocket:

                        yield json.loads(message)

            except Exception as e:

                logger.exception(
                    "connection_failed=%s",
                    str(e)
                )

                logger.info(
                    "reconnecting_in=5_seconds"
                )

                await asyncio.sleep(5)