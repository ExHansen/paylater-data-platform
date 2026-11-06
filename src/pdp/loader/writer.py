"""Write raw CDC messages into warehouse.raw: append-only, idempotent on the Kafka offset."""

from __future__ import annotations

from dataclasses import dataclass

from psycopg.types.json import Jsonb

from pdp.loader.envelope import DeadLetter, parse
from pdp.ops import chaos_enabled, set_chaos

TABLES = ("users", "credit_accounts", "merchants", "transactions", "installments", "repayments")


@dataclass(frozen=True)
class RawMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None


@dataclass
class WriteResult:
    inserted: int = 0
    dead_letters: int = 0
    skipped: int = 0


def _dead_letter(conn, msg: RawMessage, error: str) -> None:
    payload = None if msg.value is None else msg.value.decode("utf-8", errors="replace")
    conn.execute(
        "INSERT INTO raw.cdc_dead_letter (kafka_topic, kafka_partition, kafka_offset, payload, "
        "error) VALUES (%s, %s, %s, %s, %s)",
        (msg.topic, msg.partition, msg.offset, payload, error),
    )


def _jsonb(value):
    return None if value is None else Jsonb(value)


def write_batch(conn, messages: list[RawMessage]) -> WriteResult:
    """Store one batch in a single transaction and commit it.

    The caller commits Kafka offsets only after this returns, so a crash in between replays the
    batch; the unique (topic, partition, offset) key turns the replay into a no-op.
    """
    if chaos_enabled(conn, "drop_next_batch"):
        # Chaos hook for the `reconciliation_gap` fault: lose exactly one batch.
        set_chaos(conn, "drop_next_batch", False)
        conn.commit()
        return WriteResult(skipped=len(messages))
    result = WriteResult()
    for msg in messages:
        try:
            event = parse(msg.topic, msg.key, msg.value)
        except DeadLetter as exc:
            _dead_letter(conn, msg, exc.reason)
            result.dead_letters += 1
            continue
        if event is None:
            result.skipped += 1
            continue
        if event.table not in TABLES:
            _dead_letter(conn, msg, f"unknown table {event.table!r}")
            result.dead_letters += 1
            continue
        cur = conn.execute(
            f"INSERT INTO raw.cdc_{event.table} (op, pk, before, after, source_ts_ms, lsn, "
            "kafka_topic, kafka_partition, kafka_offset) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING",
            (event.op, Jsonb(event.pk), _jsonb(event.before), _jsonb(event.after),
             event.source_ts_ms, event.lsn, msg.topic, msg.partition, msg.offset),
        )  # fmt: skip
        result.inserted += cur.rowcount
    conn.commit()
    return result
