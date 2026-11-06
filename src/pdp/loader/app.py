"""Raw loader service: Kafka (all paylater.public.* topics) -> warehouse.raw.

Run with `python -m pdp.loader.app` (needs the `loader` extra).
"""

from __future__ import annotations

import logging
import time

import psycopg
from confluent_kafka import Consumer, KafkaError

from pdp.config import Settings
from pdp.db import connect
from pdp.loader.batcher import Batcher
from pdp.loader.writer import RawMessage, write_batch

log = logging.getLogger("pdp.loader")


def _flush(conn_holder: dict, settings: Settings, consumer: Consumer, batch: list[RawMessage]):
    delay = 1.0
    while True:
        try:
            if conn_holder.get("conn") is None or conn_holder["conn"].closed:
                conn_holder["conn"] = connect(settings.wh_dsn)
            result = write_batch(conn_holder["conn"], batch)
            consumer.commit(asynchronous=False)
            log.info("batch stored: %s", result)
            return
        except psycopg.Error as exc:  # database down or restarting: keep the batch, retry
            log.warning("warehouse write failed (%s); retrying in %.0fs", exc, delay)
            try:
                conn_holder["conn"].close()
            except Exception:  # noqa: BLE001
                pass
            conn_holder["conn"] = None
            time.sleep(delay)
            delay = min(delay * 2, 30.0)


def run(settings: Settings | None = None) -> None:
    settings = settings or Settings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": "pdp-raw-loader",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([f"^{settings.topic_prefix}\\.public\\..*"])
    batcher = Batcher()
    conn_holder: dict = {}
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if batcher.due():
                    _flush(conn_holder, settings, consumer, batcher.peek())
                    batcher.drain()
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("kafka error: %s", msg.error())
                continue
            full = batcher.add(
                RawMessage(msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.value())
            )
            if full or batcher.due():
                _flush(conn_holder, settings, consumer, batcher.peek())
                batcher.drain()
    finally:
        consumer.close()


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run()
