"""Normalize raw exchange WebSocket messages into one common trade schema."""
from __future__ import annotations

import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

TRADE_FIELDS = ["exchange", "symbol", "trade_id", "price", "size", "side", "trade_ts"]

_OPPOSITE = {"buy": "sell", "sell": "buy"}
_EXTRA_FRACTION = re.compile(r"(\.\d{6})\d+")


def _positive_decimal(value: Any) -> Decimal | None:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return number if number.is_finite() and number > 0 else None


def _iso_utc(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = _EXTRA_FRACTION.sub(r"\1", value.replace("Z", "+00:00"))
    try:
        ts = datetime.fromisoformat(text)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC).isoformat()


def _trade(exchange, symbol, trade_id, price, size, side, ts) -> dict | None:
    price, size, ts = _positive_decimal(price), _positive_decimal(size), _iso_utc(ts)
    if not symbol or trade_id is None or price is None or size is None or ts is None:
        return None
    if side not in _OPPOSITE:
        return None
    return {
        "exchange": exchange,
        "symbol": symbol.upper(),
        "trade_id": str(trade_id),
        "price": str(price),
        "size": str(size),
        "side": side,  # always the TAKER (aggressor) side
        "trade_ts": ts,
    }


def normalize_coinbase(msg: dict) -> list[dict]:
    """Coinbase Exchange `matches` channel.

    Coinbase reports the MAKER order's side, so the taker side is the opposite.
    """
    if msg.get("type") not in {"match", "last_match"}:
        return []
    base, _, quote = str(msg.get("product_id", "")).partition("-")
    if quote != "USD":
        return []
    trade = _trade(
        "coinbase",
        base,
        msg.get("trade_id"),
        msg.get("price"),
        msg.get("size"),
        _OPPOSITE.get(msg.get("side")),
        msg.get("time"),
    )
    return [trade] if trade else []


def normalize_kraken(msg: dict) -> list[dict]:
    """Kraken WebSocket v2 `trade` channel. `side` is already the taker side."""
    if msg.get("channel") != "trade" or msg.get("type") not in {"update", "snapshot"}:
        return []
    trades = []
    for item in msg.get("data") or []:
        base, _, quote = str(item.get("symbol", "")).partition("/")
        if quote != "USD":
            continue
        trade = _trade(
            "kraken",
            base,
            item.get("trade_id"),
            item.get("price"),
            item.get("qty"),
            item.get("side"),
            item.get("timestamp"),
        )
        if trade:
            trades.append(trade)
    return trades
