from flink_jobs.models.market_event import MarketEvent

def enrich_trade(event):

    data = event["data"]

    price = float(
        data["p"]
    )

    quantity = float(
        data["q"]
    )

    return MarketEvent(

        symbol=data["s"],

        price=price,

        quantity=quantity,

        trade_id=int(
            data["t"]
        ),

        event_time=int(
            data["T"]
        ),

        is_buyer_maker=bool(
            data["m"]
        ),

        trade_value=
            price * quantity,

        is_anomaly=
            quantity > 5
    )