"""Consume normalized trades from Kafka/Redpanda and load them into Postgres in micro-batches."""
from __future__ import annotations

import json
import logging
import os
import signal
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError
from normalize import TRADE_FIELDS
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s [consumer] %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto.trades")
GROUP_ID = os.getenv("KAFKA_GROUP", "trade-loader")
DB_DSN = os.getenv("DATABASE_URL", "postgresql://crypto:crypto@localhost:5433/crypto")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_SECONDS = float(os.getenv("FLUSH_SECONDS", "1"))

INSERT_SQL = (
    f"INSERT INTO raw.trades ({', '.join(TRADE_FIELDS)}) VALUES %s "
    "ON CONFLICT (exchange, symbol, trade_id) DO NOTHING"
)

running = True


def _stop(*_):
    global running
    running = False


def connect_db():
    while True:
        try:
            conn = psycopg2.connect(DB_DSN)
            log.info("connected to Postgres")
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("Postgres not ready: %s", exc)
            time.sleep(3)


def flush(conn, rows):
    """Insert a batch; reconnect and retry if the connection dropped."""
    while True:
        try:
            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, rows, page_size=500)
            conn.commit()
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("lost DB connection (%s), retrying", exc)
            conn = connect_db()


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit only after rows are safely in Postgres
        }
    )
    consumer.subscribe([TOPIC])
    conn = connect_db()

    buffer: list[tuple] = []
    last_flush, last_log, loaded = time.monotonic(), time.monotonic(), 0

    while running:
        msg = consumer.poll(0.5)
        if msg is not None:
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("kafka error: %s", msg.error())
            else:
                try:
                    trade = json.loads(msg.value())
                    buffer.append(tuple(trade[f] for f in TRADE_FIELDS))
                except (json.JSONDecodeError, KeyError, TypeError):
                    log.warning("skipping malformed message at offset %s", msg.offset())

        if buffer and (len(buffer) >= BATCH_SIZE or time.monotonic() - last_flush >= FLUSH_SECONDS):
            conn = flush(conn, buffer)
            consumer.commit(asynchronous=False)
            loaded += len(buffer)
            buffer.clear()
            last_flush = time.monotonic()

        if time.monotonic() - last_log > 30:
            log.info("trades loaded so far: %d", loaded)
            last_log = time.monotonic()

    if buffer:
        flush(conn, buffer)
        consumer.commit(asynchronous=False)
    consumer.close()
    conn.close()


if __name__ == "__main__":
    main()
