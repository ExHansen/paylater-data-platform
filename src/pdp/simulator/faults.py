"""Fault injection: realistic pipeline problems, each logged so detection time can be measured."""

from __future__ import annotations

import json
import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

from pdp.ops import log_fault, set_chaos
from pdp.simulator.domain import Transaction
from pdp.simulator.generator import build_installments

WIB = ZoneInfo("Asia/Jakarta")

# The data-quality checks (implemented in pdp.quality) and the fault each one should catch.
CHECK_TYPES = (
    "freshness",
    "raw_duplicate_lsn",
    "lsn_regression",
    "schema_drift",
    "invalid_values",
    "volume_anomaly",
    "reconciliation",
)
FAULT_CHECK = {
    "duplicate_events": "raw_duplicate_lsn",
    "out_of_order": "lsn_regression",
    "schema_drift": "schema_drift",
    "invalid_amount": "invalid_values",
    "invalid_tenor": "invalid_values",
    "loader_stall": "freshness",
    "volume_drop": "volume_anomaly",
    "reconciliation_gap": "reconciliation",
}
FAULTS = tuple(FAULT_CHECK)
# Faults that share a check are told apart by the check's target (one result per rule).
FAULT_TARGET = {
    "invalid_amount": "transactions.amount",
    "invalid_tenor": "transactions.tenor_months",
}


@dataclass
class FaultContext:
    settings: Any
    app_conn: Any
    wh_conn: Any
    compose: Callable[..., Any] | None
    producer_factory: Callable[[], Any] | None = None
    sleep: Callable[[float], None] = field(default=time.sleep)


def rebuild_envelope(table: str, row: dict) -> bytes:
    """A Debezium-shaped value (schemas disabled) rebuilt from a stored raw CDC row."""
    value = {
        "before": row.get("before"),
        "after": row.get("after"),
        "source": {
            "connector": "postgresql",
            "db": "paylater_app",
            "schema": "public",
            "table": table,
            "ts_ms": row["source_ts_ms"],
            "lsn": row["lsn"],
        },
        "op": row["op"],
        "ts_ms": int(time.time() * 1000),
    }
    return json.dumps(value, default=str).encode()


def _sample(conn, sql: str, n: int) -> list:
    return [r[0] for r in conn.execute(sql, (n,)).fetchall()]


def _insert_txn(conn, account_id, merchant_id, amount, tenor, *, promo: str | None = None,
                with_installments: bool = False) -> UUID:  # fmt: skip
    now = datetime.now(WIB)
    rng = random.Random()
    txn = Transaction(UUID(int=rng.getrandbits(128), version=4), account_id, merchant_id,
                      Decimal(amount), tenor, Decimal("0.03"), "approved", "app",
                      "dev-fault", now, now)  # fmt: skip
    columns = ("txn_id, account_id, merchant_id, amount, tenor_months, monthly_rate, status, "
               "channel, device_id, created_at, updated_at")  # fmt: skip
    values = [txn.txn_id, txn.account_id, txn.merchant_id, txn.amount, txn.tenor_months,
              txn.monthly_rate, txn.status, txn.channel, txn.device_id, now, now]  # fmt: skip
    if promo is not None:
        columns += ", promo_code"
        values.append(promo)
    placeholders = ",".join(["%s"] * len(values))
    conn.execute(f"INSERT INTO transactions ({columns}) VALUES ({placeholders})", values)
    if with_installments:
        for inst in build_installments(txn):
            conn.execute(
                "INSERT INTO installments (installment_id, txn_id, account_id, seq, due_date, "
                "principal, interest, status, paid_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (inst.installment_id, inst.txn_id, inst.account_id, inst.seq, inst.due_date,
                 inst.principal, inst.interest, "unpaid", None, now),
            )  # fmt: skip
    return txn.txn_id


def _accounts_and_merchant(conn, n: int):
    accounts = _sample(
        conn, "SELECT account_id FROM credit_accounts WHERE status = 'active' LIMIT %s", n
    )
    merchant = conn.execute("SELECT merchant_id FROM merchants LIMIT 1").fetchone()[0]
    return accounts, merchant


def _invalid_amount(ctx: FaultContext, **_) -> dict:
    accounts, merchant = _accounts_and_merchant(ctx.app_conn, 3)
    amounts = ("0", "-1000", "0")
    ids = [
        _insert_txn(ctx.app_conn, a, merchant, amt, 3)
        for a, amt in zip(accounts, amounts, strict=False)
    ]
    return {"txn_ids": ids}


def _invalid_tenor(ctx: FaultContext, **_) -> dict:
    accounts, merchant = _accounts_and_merchant(ctx.app_conn, 2)
    return {"txn_ids": [_insert_txn(ctx.app_conn, a, merchant, "100000", 24) for a in accounts]}


def _schema_drift(ctx: FaultContext, **_) -> dict:
    ctx.app_conn.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS promo_code text")
    accounts, merchant = _accounts_and_merchant(ctx.app_conn, 5)
    ids = [
        _insert_txn(
            ctx.app_conn, a, merchant, "150000", 3, promo="HARBOLNAS", with_installments=True
        )
        for a in accounts
    ]
    return {"column": "promo_code", "txn_ids": ids}


def _raw_rows(ctx: FaultContext, sql: str, params: tuple) -> list[dict]:
    cur = ctx.wh_conn.execute(sql, params)
    names = [d.name for d in cur.description]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def _produce(ctx: FaultContext, table: str, rows: list[dict]) -> int:
    producer = ctx.producer_factory()
    topic = f"{ctx.settings.topic_prefix}.public.{table}"
    for row in rows:
        producer.produce(
            topic, key=json.dumps(row["pk"]).encode(), value=rebuild_envelope(table, row)
        )
    producer.flush(30)
    return len(rows)


def _duplicate_events(ctx: FaultContext, **_) -> dict:
    rows = _raw_rows(
        ctx,
        "SELECT op, pk, before, after, source_ts_ms, lsn FROM raw.cdc_transactions "
        "ORDER BY id DESC LIMIT %s",
        (50,),
    )
    return {"reproduced": _produce(ctx, "transactions", rows)}


def late_version(row: dict) -> dict:
    """An older version of a row that was never delivered: the stored change, one LSN earlier.

    Re-sending the stored change itself would be an exact duplicate (a different fault).
    """
    return {**row, "lsn": row["lsn"] - 1}


def _out_of_order(ctx: FaultContext, wait_s: float = 60, **_) -> dict:
    txn_id = ctx.app_conn.execute(
        "SELECT txn_id FROM transactions ORDER BY created_at DESC LIMIT 1"
    ).fetchone()[0]
    for channel in ("web", "qr"):
        ctx.app_conn.execute(
            "UPDATE transactions SET channel = %s, updated_at = now() WHERE txn_id = %s",
            (channel, txn_id),
        )
        ctx.app_conn.commit()
    deadline = time.monotonic() + wait_s
    while True:
        rows = _raw_rows(
            ctx,
            "SELECT op, pk, before, after, source_ts_ms, lsn FROM raw.cdc_transactions "
            "WHERE op = 'u' AND pk->>'txn_id' = %s ORDER BY lsn",
            (str(txn_id),),
        )
        if len(rows) >= 2 or time.monotonic() > deadline:
            break
        ctx.sleep(2)
    if len(rows) < 2:
        raise RuntimeError("both updates did not reach raw in time; is the loader running?")
    return {"txn_id": txn_id, "reproduced": _produce(ctx, "transactions", [late_version(rows[0])])}


def _stop_service(service: str):
    def run(ctx: FaultContext, minutes: float = 3, **_) -> dict:
        ctx.compose("stop", service)
        ctx.sleep(minutes * 60)
        ctx.compose("start", service)
        return {"service": service, "minutes": minutes}

    return run


def _reconciliation_gap(ctx: FaultContext, **_) -> dict:
    set_chaos(ctx.wh_conn, "drop_next_batch", True)
    return {"chaos": "drop_next_batch"}


INJECTORS: dict[str, Callable[..., dict]] = {
    "duplicate_events": _duplicate_events,
    "out_of_order": _out_of_order,
    "schema_drift": _schema_drift,
    "invalid_amount": _invalid_amount,
    "invalid_tenor": _invalid_tenor,
    "loader_stall": _stop_service("loader"),
    "volume_drop": _stop_service("simulator"),
    "reconciliation_gap": _reconciliation_gap,
}


def inject(fault: str, ctx: FaultContext, **opts) -> int:
    """Log the fault first (so detection is measured from this moment), then cause it."""
    if fault not in INJECTORS:
        raise ValueError(f"unknown fault {fault!r}; choose one of {', '.join(FAULTS)}")
    fault_id = log_fault(ctx.wh_conn, fault, {k: str(v) for k, v in opts.items()})
    ctx.wh_conn.commit()
    details = INJECTORS[fault](ctx, **opts)
    ctx.app_conn.commit()
    ctx.wh_conn.execute(
        "UPDATE ops.fault_log SET details = details || %s::jsonb WHERE fault_id = %s",
        (json.dumps(details, default=str), fault_id),
    )
    ctx.wh_conn.commit()
    return fault_id
