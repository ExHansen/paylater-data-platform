"""Parse Debezium change events (JSON converter, schemas disabled) and decode column values.

Debezium encodings handled exactly (never via float):
- numeric (decimal.handling.mode=string) -> "750000.00" -> Decimal
- date -> days since 1970-01-01 (int) -> datetime.date
- timestamptz -> ISO-8601 string -> aware datetime
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

EPOCH = date(1970, 1, 1)
OPS = ("c", "u", "d", "r")

NUMERIC, INTEGER, DATE, TIMESTAMP = "numeric", "integer", "date", "timestamp"

COLUMN_TYPES: dict[str, dict[str, str]] = {
    "users": {"created_at": TIMESTAMP, "updated_at": TIMESTAMP},
    "credit_accounts": {"credit_limit": NUMERIC, "created_at": TIMESTAMP, "updated_at": TIMESTAMP},
    "merchants": {"created_at": TIMESTAMP},
    "transactions": {
        "amount": NUMERIC,
        "monthly_rate": NUMERIC,
        "tenor_months": INTEGER,
        "created_at": TIMESTAMP,
        "updated_at": TIMESTAMP,
    },
    "installments": {
        "seq": INTEGER,
        "due_date": DATE,
        "principal": NUMERIC,
        "interest": NUMERIC,
        "paid_at": TIMESTAMP,
        "updated_at": TIMESTAMP,
    },
    "repayments": {"amount": NUMERIC, "paid_at": TIMESTAMP},
}


class DeadLetter(Exception):
    """A message that cannot be understood; the loader stores it in raw.cdc_dead_letter."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class CdcEvent:
    table: str
    op: Literal["c", "u", "d", "r"]
    pk: dict
    before: dict | None
    after: dict | None
    source_ts_ms: int
    lsn: int


def parse(topic: str, key: bytes | None, value: bytes | None) -> CdcEvent | None:
    """Return the event, None for a tombstone, or raise DeadLetter."""
    if value is None:
        return None
    try:
        payload = json.loads(value)
    except (ValueError, UnicodeDecodeError) as exc:
        raise DeadLetter(f"invalid json: {exc}") from exc
    if not isinstance(payload, dict) or "source" not in payload:
        raise DeadLetter("not a Debezium envelope (no source)")
    op = payload.get("op")
    if op not in OPS:
        raise DeadLetter(f"missing or unknown op: {op!r}")
    source = payload["source"] or {}
    if source.get("lsn") is None or source.get("ts_ms") is None:
        raise DeadLetter("missing source.lsn or source.ts_ms")
    try:
        pk = json.loads(key) if key else {}
    except (ValueError, UnicodeDecodeError) as exc:
        raise DeadLetter(f"invalid key: {exc}") from exc
    return CdcEvent(
        table=topic.rsplit(".", 1)[-1],
        op=op,
        pk=pk if isinstance(pk, dict) else {"key": pk},
        before=payload.get("before"),
        after=payload.get("after"),
        source_ts_ms=int(source["ts_ms"]),
        lsn=int(source["lsn"]),
    )


def _decode(kind: str, value: Any) -> Any:
    if value is None:
        return None
    if kind == NUMERIC:
        try:
            return Decimal(str(value))
        except InvalidOperation as exc:
            raise ValueError(f"not a number: {value!r}") from exc
    if kind == INTEGER:
        return int(value)
    if kind == DATE:
        return EPOCH + timedelta(days=int(value))
    if kind == TIMESTAMP:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return value


def decode_row(table: str, row: dict) -> dict:
    """Typed copy of a row; columns not in COLUMN_TYPES (including unknown ones) pass through."""
    kinds = COLUMN_TYPES.get(table, {})
    return {name: _decode(kinds[name], v) if name in kinds else v for name, v in row.items()}
