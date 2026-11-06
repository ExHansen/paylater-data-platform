import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from pdp.loader.envelope import DeadLetter, decode_row, parse

FIX = Path("tests/fixtures/debezium")
T_TXN = "paylater.public.transactions"
T_INST = "paylater.public.installments"
KEY = json.dumps({"txn_id": "6d8f4b3c-1111-4aaa-8bbb-000000000001"}).encode()


def load(name: str) -> bytes:
    return (FIX / name).read_bytes()


def test_parse_create():
    ev = parse(T_TXN, KEY, load("create.json"))
    assert ev.op == "c" and ev.table == "transactions" and ev.before is None
    assert ev.lsn == 24023128 and ev.source_ts_ms == 1793941200123
    assert ev.pk == {"txn_id": "6d8f4b3c-1111-4aaa-8bbb-000000000001"}


def test_parse_update_and_delete():
    upd = parse(T_TXN, KEY, load("update.json"))
    assert upd.op == "u" and upd.before["channel"] == "app" and upd.after["channel"] == "web"
    dele = parse(T_TXN, KEY, load("delete.json"))
    assert dele.op == "d" and dele.after is None and dele.before is not None


def test_parse_encodings_exactly():  # Review Focus 1
    ev = parse(T_INST, None, load("snapshot.json"))
    assert ev.op == "r"
    row = decode_row("installments", ev.after)
    assert row["principal"] == Decimal("1000000.00") and isinstance(row["principal"], Decimal)
    assert row["due_date"] == date(2026, 6, 30) and row["paid_at"] is None
    assert row["updated_at"] == datetime(2026, 5, 31, 3, 0, tzinfo=UTC)
    txn = decode_row("transactions", parse(T_TXN, KEY, load("create.json")).after)
    assert txn["amount"] == Decimal("750000.00") and txn["monthly_rate"] == Decimal("0.0300")
    assert txn["created_at"].microsecond == 123456 and txn["tenor_months"] == 3


def test_tombstone_is_ignored():
    assert parse(T_TXN, KEY, None) is None


def test_unknown_fields_are_preserved():  # Review Focus 5
    ev = parse(T_TXN, KEY, load("drift.json"))
    assert ev.after["promo_code"] == "HARBOLNAS"
    assert decode_row("transactions", ev.after)["promo_code"] == "HARBOLNAS"


@pytest.mark.parametrize("value", [b"{not json", b"[]", b'"text"'])
def test_malformed_goes_to_dead_letter(value):
    with pytest.raises(DeadLetter):
        parse(T_TXN, KEY, value)


def test_missing_op_goes_to_dead_letter():
    with pytest.raises(DeadLetter, match="op"):
        parse(T_TXN, KEY, load("no_op.json"))
