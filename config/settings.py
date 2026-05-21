import os

from dotenv import load_dotenv


load_dotenv()


class Settings:

    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )

    KAFKA_TOPIC_RAW = os.getenv(
        "KAFKA_TOPIC_RAW",
        "raw_market_data"
    )

    BINANCE_WS_URL = os.getenv(
        "BINANCE_WS_URL",
        "wss://stream.binance.com:9443/stream"
    )

    LOG_LEVEL = os.getenv(
        "LOG_LEVEL",
        "INFO"
    )


settings = Settings()