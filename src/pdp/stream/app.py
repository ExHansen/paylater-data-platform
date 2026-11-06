"""Risk stream processor (Quix Streams): CDC -> per-account features -> alerts.

Run with `python -m pdp.stream.app` (needs the `stream` extra).
- transactions: exposure updates; approved purchases are grouped by account for windowed
  features; new purchases (op=c) are checked against the rules and alerts are emitted.
- installments: paid installments reduce exposure.
- credit_accounts: current limits for the limit-based rules.
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from decimal import Decimal

import psycopg

from pdp.config import Settings
from pdp.loader.envelope import CdcEvent, decode_row
from pdp.stream.exposure import apply_delta, exposure_delta
from pdp.stream.features import AccountWindow, Features, from_state, to_state, update
from pdp.stream.rules import evaluate, utilization

log = logging.getLogger("pdp.stream")


def is_envelope(value) -> bool:
    return isinstance(value, dict) and value.get("op") in ("c", "u", "d", "r")


def to_event(table: str, value: dict) -> CdcEvent:
    source = value.get("source") or {}
    return CdcEvent(
        table, value["op"], {}, value.get("before"), value.get("after"),
        int(source.get("ts_ms", 0)), int(source.get("lsn", 0)),
    )  # fmt: skip


def is_purchase(value: dict) -> bool:
    after = value.get("after") or {}
    return value["op"] in ("c", "r") and after.get("status") == "approved"


def purchase_alert(
    value: dict,
    window: AccountWindow,
    credit_limit: Decimal | None,
    exposure: Decimal | None,
    now_ms: int,
) -> tuple[Features, dict | None]:
    """Update the account window with one purchase; build an alert for new purchases only."""
    after = decode_row("transactions", value["after"])
    ts_ms = int(after["created_at"].timestamp() * 1000)
    features = update(window, ts_ms, after["amount"], after["device_id"])
    if value["op"] != "c":
        return features, None
    rules = evaluate(features, after["amount"], credit_limit, exposure)
    if not rules:
        return features, None
    source_ts_ms = int(value["source"]["ts_ms"])
    return features, {
        "alert_id": str(uuid.uuid4()),
        "account_id": after["account_id"],
        "txn_id": after["txn_id"],
        "rules": rules,
        "amount": str(after["amount"]),
        "source_ts_ms": source_ts_ms,
        "latency_ms": max(0, now_ms - source_ts_ms),
    }


class OnlineStore:
    """The online tables in the warehouse (autocommit, reconnects on failure)."""

    def __init__(self, dsn: str):
        self.dsn = dsn
        self._conn: psycopg.Connection | None = None

    def conn(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            self._conn = psycopg.connect(self.dsn, autocommit=True)
        return self._conn

    def apply_exposure(self, table: str, value: dict) -> None:
        delta = exposure_delta(to_event(table, value))
        if delta is not None:
            apply_delta(self.conn(), *delta)

    def upsert_limit(self, value: dict) -> None:
        after = value.get("after")
        if not after or value["op"] == "d":
            return
        self.conn().execute(
            "INSERT INTO online.account_limits (account_id, credit_limit) VALUES (%s, %s) "
            "ON CONFLICT (account_id) DO UPDATE "
            "SET credit_limit = EXCLUDED.credit_limit, updated_at = now()",
            (after["account_id"], Decimal(str(after["credit_limit"]))),
        )

    def limit_and_exposure(self, account_id: str) -> tuple[Decimal | None, Decimal | None]:
        row = (
            self.conn()
            .execute(
                "SELECT l.credit_limit, e.exposure FROM (SELECT %s::uuid AS account_id) k "
                "LEFT JOIN online.account_limits l USING (account_id) "
                "LEFT JOIN online.account_exposure e USING (account_id)",
                (account_id,),
            )
            .fetchone()
        )
        return (row[0], row[1]) if row else (None, None)

    def upsert_features(self, account_id: str, f: Features, ratio: float | None) -> None:
        self.conn().execute(
            "INSERT INTO online.risk_features (account_id, txn_count_10m, txn_amount_10m, "
            "txn_count_1h, txn_amount_1h, utilization, is_new_device) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (account_id) DO UPDATE SET "
            "txn_count_10m = EXCLUDED.txn_count_10m, txn_amount_10m = EXCLUDED.txn_amount_10m, "
            "txn_count_1h = EXCLUDED.txn_count_1h, txn_amount_1h = EXCLUDED.txn_amount_1h, "
            "utilization = EXCLUDED.utilization, is_new_device = EXCLUDED.is_new_device, "
            "updated_at = now()",
            (account_id, f.txn_count_10m, f.txn_amount_10m, f.txn_count_1h, f.txn_amount_1h,
             ratio, f.is_new_device),
        )  # fmt: skip

    def save_alert(self, alert: dict) -> None:
        self.conn().execute(
            "INSERT INTO online.risk_alerts (alert_id, account_id, txn_id, rules, amount, "
            "source_ts_ms, latency_ms) VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (alert["alert_id"], alert["account_id"], alert["txn_id"], alert["rules"],
             Decimal(alert["amount"]), alert["source_ts_ms"], alert["latency_ms"]),
        )  # fmt: skip


def build_app(settings: Settings):
    from quixstreams import Application

    app = Application(
        broker_address=settings.kafka_bootstrap,
        consumer_group="pdp-risk",
        auto_offset_reset="earliest",
        state_dir=os.environ.get("PDP_STATE_DIR", "state"),
    )
    store = OnlineStore(settings.wh_dsn)
    prefix = settings.topic_prefix

    def topic(table: str):
        return app.topic(
            f"{prefix}.public.{table}", key_deserializer="bytes", value_deserializer="json"
        )

    alerts_topic = app.topic("risk.alerts", key_serializer="str", value_serializer="json")

    def on_purchase(value: dict, state) -> dict | None:
        window = from_state(state.get("window"))
        account_id = value["after"]["account_id"]
        limit, exposure = (
            store.limit_and_exposure(account_id) if value["op"] == "c" else (None, None)
        )
        features, alert = purchase_alert(value, window, limit, exposure, int(time.time() * 1000))
        state.set("window", to_state(window))
        if value["op"] == "c":
            store.upsert_features(account_id, features, utilization(exposure, limit))
        return alert

    txns = app.dataframe(topic("transactions")).filter(is_envelope)
    txns = txns.update(lambda v: store.apply_exposure("transactions", v))
    purchases = txns.filter(is_purchase)
    purchases = purchases.group_by(lambda v: v["after"]["account_id"], name="account_id")
    alerts = purchases.apply(on_purchase, stateful=True).filter(lambda a: a is not None)
    alerts = alerts.update(store.save_alert)
    alerts.to_topic(alerts_topic, key=lambda a: a["account_id"])

    installments = app.dataframe(topic("installments")).filter(is_envelope)
    installments.update(lambda v: store.apply_exposure("installments", v))

    accounts = app.dataframe(topic("credit_accounts")).filter(is_envelope)
    accounts.update(store.upsert_limit)
    return app


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    build_app(Settings()).run()
