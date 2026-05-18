import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    BINANCE_WS_URL = os.getenv("BINANCE_WS_URL")

    KAFKA_BOOTSTRAP_SERVERS = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS"
    )

    KAFKA_TOPIC_RAW = os.getenv(
        "KAFKA_TOPIC_RAW"
    )

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()