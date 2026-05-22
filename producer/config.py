import os

from dotenv import load_dotenv

load_dotenv()


class Settings:
    BINANCE_WS_URL = os.getenv(
        "BINANCE_WS_URL",
        "wss://stream.binance.com:9443/stream",
    )
    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_trades")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()
