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
from pdp.stream.features import AccountWindow, Features, from_state, has_seen, to_state, update
from pdp.stream.rules import evaluate, utilization

log = logging.getLogger("pdp.stream")
# A purchase committed more than an hour after it was created is a backfill (e.g. `pdp seed`)
# and raises no alert. Processing delay does not count: after an outage, purchases are still
# screened, with their true latency.
BACKFILL_AGE_MS = 60 * 60 * 1000
ALERT_NAMESPACE = uuid.UUID("4f6b9a52-0c1e-4b7a-9d55-7a3c2e8b1f60")
# Exposure changes are keyed by the changed row and its LSN, so redelivered events apply once.
ROW_ID = {"transactions": "txn_id", "installments": "installment_id"}


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
) -> tuple[Features | None, dict | None]:
    """Update the account window with one purchase; build an alert for new purchases only.

    A purchase already in the window is a redelivery: nothing changes, (None, None).
    """
    after = decode_row("transactions", value["after"])
    txn_id = str(after["txn_id"])
    if has_seen(window, txn_id):
        return None, None
    ts_ms = int(after["created_at"].timestamp() * 1000)
    features = update(window, ts_ms, after["amount"], after["device_id"], txn_id)
    source_ts_ms = int(value["source"]["ts_ms"])
    if value["op"] != "c" or source_ts_ms - ts_ms > BACKFILL_AGE_MS:
        # snapshot rows and purchases dated long before now (backfills) only feed the window
        return features, None
    rules = evaluate(features, after["amount"], credit_limit, exposure)
    if not rules:
        return features, None
    return features, {
        "alert_id": str(uuid.uuid5(ALERT_NAMESPACE, txn_id)),
        "account_id": after["account_id"],
        "txn_id": after["txn_id"],
        "rules": rules,
        "amount": str(after["amount"]),
        "source_ts_ms": source_ts_ms,
        "latency_ms": max(0, now_ms - source_ts_ms),
    }


class SkipBadEvents:
    """Quix `on_processing_error`: skip and count events that cannot be processed.

    Data errors (a missing or malformed field) skip the event so one bad record does not stop
    risk screening; anything else (e.g. the database is down) stays fatal and the container
    restarts and retries from the last committed offset.
    """

    DATA_ERRORS = (KeyError, TypeError, ValueError, ArithmeticError, psycopg.DataError)

    def __init__(self) -> None:
        self.total = 0

    def __call__(self, exc: Exception, row=None, logger=None) -> bool:
        if not isinstance(exc, self.DATA_ERRORS):
            return False
        self.total += 1
        log.warning("skipped event (%s: %s); stream_skipped_total=%d", type(exc).__name__, exc,
                    self.total)  # fmt: skip
        return True


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
        event = to_event(table, value)
        delta = exposure_delta(event)
        if delta is None:
            return
        row = event.after or event.before or {}
        conn = self.conn()
        with conn.transaction():
            first_time = conn.execute(
                "INSERT INTO online.exposure_changes (source_table, row_id, lsn) "
                "VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                (table, str(row[ROW_ID[table]]), event.lsn),
            ).rowcount
            if first_time:
                apply_delta(conn, *delta)

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
        on_processing_error=SkipBadEvents(),
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
        if features is None:  # redelivered purchase
            return None
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
