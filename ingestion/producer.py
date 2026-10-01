"""Stream live trades from Coinbase and Kraken and publish them to Kafka/Redpanda."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import Counter
from collections.abc import Callable
from decimal import Decimal

import websockets
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from normalize import normalize_coinbase, normalize_kraken

logging.basicConfig(level=logging.INFO, format="%(asctime)s [producer] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto.trades")
SYMBOLS = [
    s.strip().upper() for s in os.getenv("SYMBOLS", "BTC,ETH,SOL,PAXG").split(",") if s.strip()
]

COINBASE_URL = "wss://ws-feed.exchange.coinbase.com"
KRAKEN_URL = "wss://ws.kraken.com/v2"


def coinbase_subscriptions(symbols: list[str]) -> list[dict]:
    # One message per product so an unlisted symbol can't break the others.
    return [
        {"type": "subscribe", "product_ids": [f"{s}-USD"], "channels": ["matches"]}
        for s in symbols
    ]


def kraken_subscriptions(symbols: list[str]) -> list[dict]:
    return [
        {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": [f"{s}/USD"], "snapshot": False},
        }
        for s in symbols
    ]


def exchange_error(msg: dict) -> str | None:
    if msg.get("type") == "error":  # Coinbase
        return f"{msg.get('message')}: {msg.get('reason')}"
    if msg.get("success") is False:  # Kraken
        return str(msg.get("error"))
    return None


class KafkaSink:
    def __init__(self) -> None:
        self.producer = Producer(
            {
                "bootstrap.servers": BOOTSTRAP,
                "linger.ms": 20,
                "compression.type": "lz4",
                "enable.idempotence": True,
            }
        )
        self.counts: Counter[str] = Counter()

    def send(self, trade: dict) -> None:
        key = f"{trade['exchange']}:{trade['symbol']}".encode()
        value = json.dumps(trade).encode()
        while True:
            try:
                self.producer.produce(TOPIC, key=key, value=value)
                break
            except BufferError:
                self.producer.poll(0.5)
        self.producer.poll(0)
        self.counts[trade["exchange"]] += 1


def ensure_topic(retries: int = 20) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    for attempt in range(1, retries + 1):
        try:
            futures = admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])
            for name, future in futures.items():
                try:
                    future.result()
                    log.info("created topic %s", name)
                except Exception as exc:  # "already exists" is fine
                    log.info("topic %s: %s", name, exc)
            return
        except Exception as exc:
            log.warning("broker not ready (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(3)
    raise RuntimeError("could not reach Kafka broker")


async def stream(
    name: str,
    url: str,
    subscriptions: list[dict],
    normalize: Callable[[dict], list[dict]],
    sink: KafkaSink,
) -> None:
    """Keep one exchange connection alive forever, reconnecting with backoff."""
    backoff = 1
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                for sub in subscriptions:
                    await ws.send(json.dumps(sub))
                log.info("%s connected, subscribed to %s", name, ", ".join(SYMBOLS))
                backoff = 1
                async for raw in ws:
                    msg = json.loads(raw, parse_float=Decimal)  # keep exact prices
                    if not isinstance(msg, dict):
                        continue
                    if error := exchange_error(msg):
                        log.warning("%s says: %s", name, error)
                        continue
                    for trade in normalize(msg):
                        sink.send(trade)
        except (TimeoutError, OSError, websockets.WebSocketException) as exc:
            log.warning("%s disconnected (%s), retrying in %ss", name, exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def report(sink: KafkaSink) -> None:
    while True:
        await asyncio.sleep(30)
        log.info("trades produced so far: %s", dict(sink.counts))


async def main() -> None:
    ensure_topic()
    sink = KafkaSink()
    await asyncio.gather(
        stream("coinbase", COINBASE_URL, coinbase_subscriptions(SYMBOLS), normalize_coinbase, sink),
        stream("kraken", KRAKEN_URL, kraken_subscriptions(SYMBOLS), normalize_kraken, sink),
        report(sink),
    )


if __name__ == "__main__":
    asyncio.run(main())
