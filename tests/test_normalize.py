from decimal import Decimal

from normalize import TRADE_FIELDS, normalize_coinbase, normalize_kraken
from producer import coinbase_subscriptions, exchange_error, kraken_subscriptions


def coinbase_match(**overrides):
    msg = {
        "type": "match",
        "trade_id": 123456,
        "product_id": "BTC-USD",
        "price": "97123.45",
        "size": "0.015",
        "side": "sell",  # maker side
        "time": "2026-01-01T12:00:00.123456Z",
    }
    msg.update(overrides)
    return msg


def kraken_update(*trades, **overrides):
    msg = {"channel": "trade", "type": "update", "data": list(trades)}
    msg.update(overrides)
    return msg


def kraken_trade(**overrides):
    trade = {
        "symbol": "ETH/USD",
        "side": "buy",
        "price": Decimal("3456.78"),
        "qty": Decimal("2.5"),
        "ord_type": "market",
        "trade_id": 98765,
        "timestamp": "2026-01-01T12:00:01.654321Z",
    }
    trade.update(overrides)
    return trade


# ---- Coinbase ------------------------------------------------------------------

def test_coinbase_match_is_normalized():
    [trade] = normalize_coinbase(coinbase_match())
    assert list(trade) == TRADE_FIELDS
    assert trade["exchange"] == "coinbase"
    assert trade["symbol"] == "BTC"
    assert trade["trade_id"] == "123456"
    assert trade["price"] == "97123.45"
    assert trade["trade_ts"] == "2026-01-01T12:00:00.123456+00:00"


def test_coinbase_side_is_flipped_to_taker_side():
    assert normalize_coinbase(coinbase_match(side="sell"))[0]["side"] == "buy"
    assert normalize_coinbase(coinbase_match(side="buy"))[0]["side"] == "sell"


def test_coinbase_last_match_is_kept():
    assert len(normalize_coinbase(coinbase_match(type="last_match"))) == 1


def test_coinbase_other_messages_are_ignored():
    assert normalize_coinbase({"type": "subscriptions", "channels": []}) == []
    assert normalize_coinbase({"type": "heartbeat"}) == []


def test_coinbase_non_usd_pairs_are_ignored():
    assert normalize_coinbase(coinbase_match(product_id="BTC-EUR")) == []


def test_coinbase_bad_values_are_rejected():
    assert normalize_coinbase(coinbase_match(price="0")) == []
    assert normalize_coinbase(coinbase_match(size="-1")) == []
    assert normalize_coinbase(coinbase_match(price="NaN")) == []
    assert normalize_coinbase(coinbase_match(time="not-a-time")) == []
    assert normalize_coinbase(coinbase_match(side=None)) == []


# ---- Kraken --------------------------------------------------------------------

def test_kraken_update_with_several_trades():
    trades = normalize_kraken(kraken_update(kraken_trade(), kraken_trade(trade_id=98766)))
    assert [t["trade_id"] for t in trades] == ["98765", "98766"]
    assert trades[0]["exchange"] == "kraken"
    assert trades[0]["symbol"] == "ETH"
    assert trades[0]["side"] == "buy"  # already the taker side
    assert trades[0]["size"] == "2.5"


def test_kraken_keeps_exact_decimal_prices():
    [trade] = normalize_kraken(kraken_update(kraken_trade(price=Decimal("0.123456789"))))
    assert trade["price"] == "0.123456789"


def test_kraken_nanosecond_timestamps_are_parsed():
    msg = kraken_update(kraken_trade(timestamp="2026-01-01T12:00:01.123456789Z"))
    [trade] = normalize_kraken(msg)
    assert trade["trade_ts"] == "2026-01-01T12:00:01.123456+00:00"


def test_kraken_other_messages_are_ignored():
    assert normalize_kraken({"channel": "heartbeat"}) == []
    assert normalize_kraken({"channel": "status", "type": "update", "data": []}) == []
    assert normalize_kraken({"method": "subscribe", "success": True}) == []


def test_kraken_bad_trades_are_dropped_individually():
    good, bad = kraken_trade(), kraken_trade(qty=0, trade_id=1)
    assert len(normalize_kraken(kraken_update(good, bad))) == 1


# ---- producer helpers ----------------------------------------------------------

def test_one_subscription_per_symbol():
    assert len(coinbase_subscriptions(["BTC", "ETH"])) == 2
    assert kraken_subscriptions(["BTC"])[0]["params"]["symbol"] == ["BTC/USD"]


def test_exchange_errors_are_detected():
    assert exchange_error({"type": "error", "message": "Failed", "reason": "bad product"})
    assert exchange_error({"method": "subscribe", "success": False, "error": "Currency pair"})
    assert exchange_error({"type": "match"}) is None
