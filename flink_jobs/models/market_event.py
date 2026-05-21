from dataclasses import dataclass


@dataclass
class MarketEvent:

    symbol: str

    price: float

    quantity: float

    trade_id: int

    event_time: int

    is_buyer_maker: bool

    trade_value: float

    is_anomaly: bool