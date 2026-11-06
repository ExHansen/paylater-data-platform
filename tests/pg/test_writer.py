from pathlib import Path

import pytest

from pdp.loader.writer import RawMessage, write_batch
from pdp.ops import chaos_enabled, set_chaos

FIX = Path("tests/fixtures/debezium")
TOPIC = "paylater.public.transactions"
KEY = b'{"txn_id": "6d8f4b3c-1111-4aaa-8bbb-000000000001"}'


def raw_msg(offset: int, value: bytes | None, topic: str = TOPIC) -> RawMessage:
    return RawMessage(topic=topic, partition=0, offset=offset, key=KEY, value=value)


def count(conn, table: str) -> int:
    return conn.execute(f"select count(*) from {table}").fetchone()[0]


@pytest.mark.pg
def test_writer_is_idempotent_on_redelivery(fresh_db):  # Review Focus 2
    msgs = [raw_msg(o, (FIX / "create.json").read_bytes()) for o in range(10)]
    assert write_batch(fresh_db, msgs).inserted == 10
    assert write_batch(fresh_db, msgs).inserted == 0
    assert count(fresh_db, "raw.cdc_transactions") == 10


@pytest.mark.pg
def test_tombstones_skipped_and_fields_stored(fresh_db):
    result = write_batch(
        fresh_db, [raw_msg(1, None), raw_msg(2, (FIX / "drift.json").read_bytes())]
    )
    assert result.inserted == 1 and result.skipped == 1
    after = fresh_db.execute(
        "select after->>'promo_code', lsn from raw.cdc_transactions"
    ).fetchone()
    assert after == ("HARBOLNAS", 24023500)


@pytest.mark.pg
def test_dead_letter_and_chaos_drop(fresh_db):
    assert write_batch(fresh_db, [raw_msg(1, b"{bad")]).dead_letters == 1
    assert count(fresh_db, "raw.cdc_dead_letter") == 1
    set_chaos(fresh_db, "drop_next_batch", True)
    fresh_db.commit()
    dropped = write_batch(fresh_db, [raw_msg(2, (FIX / "create.json").read_bytes())])
    assert dropped.inserted == 0 and dropped.skipped == 1
    assert not chaos_enabled(fresh_db, "drop_next_batch")
    assert count(fresh_db, "raw.cdc_transactions") == 0
