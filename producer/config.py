import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    BINANCE_WS_URL = os.getenv("BINANCE_WS_URL")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


settings = Settings()