import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer

from config import settings

logger = logging.getLogger(__name__)


class TradeKafkaProducer:

    def __init__(self):
        self._producer = Producer(
            {
                "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
                "client.id": "binance-trade-producer",
            }
        )
        self._topic = settings.KAFKA_TOPIC
        self._pending = 0

    def _delivery_callback(self, err, msg):
        self._pending -= 1
        if err is not None:
            logger.error("delivery_failed topic=%s error=%s", msg.topic(), err)

    @staticmethod
    def _parse_trade_event(event):
        payload = event.get("data", event)
        if payload.get("e") != "trade":
            return None

        return {
            "symbol": payload["s"],
            "price": float(payload["p"]),
            "quantity": float(payload["q"]),
            "trade_id": int(payload["t"]),
            "is_buyer_maker": bool(payload["m"]),
            "event_time_ms": int(payload["E"]),
            "event_time": datetime.fromtimestamp(
                payload["E"] / 1000,
                tz=timezone.utc,
            ).isoformat(),
        }

    def publish(self, event):
        trade = self._parse_trade_event(event)
        if trade is None:
            return False

        payload = json.dumps(trade)
        self._producer.produce(
            self._topic,
            value=payload.encode("utf-8"),
            key=trade["symbol"].encode("utf-8"),
            callback=self._delivery_callback,
        )
        self._pending += 1
        self._producer.poll(0)

        if self._pending >= 1000:
            self._producer.flush(5)
            self._pending = 0

        return True

    def flush(self):
        self._producer.flush(10)
