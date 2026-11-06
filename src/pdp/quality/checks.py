"""Data-quality checks over the raw CDC layer, staging and the application database.

Each check is `(settings, wh_conn, app_conn) -> list[CheckResult]`. The runner executes them
independently, stores every result in ops.dq_results and raises an alert for each failure.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

from pdp.quality.alerts import raise_alert
from pdp.quality.drift import field_diff, load_contract
from pdp.quality.stats import pct_diff, volume_zscore

Status = Literal["pass", "warn", "fail", "error"]
TABLES = ("users", "credit_accounts", "merchants", "transactions", "installments", "repayments")
# Column that moves whenever a row of the source table changes.
CHANGE_COLUMN = {
    "users": "updated_at",
    "credit_accounts": "updated_at",
    "merchants": "created_at",
    "transactions": "updated_at",
    "installments": "updated_at",
    "repayments": "paid_at",
}
RECENT = "interval '15 minutes'"
VALID_TENORS = ("1", "3", "6", "12")


@dataclass(frozen=True)
class CheckResult:
    check_type: str
    target: str
    status: Status
    observed: str
    expected: str


def _scalar(conn, sql: str, params: Sequence = ()):
    return conn.execute(sql, params).fetchone()[0]


def check_freshness(settings, wh, app) -> list[CheckResult]:
    """Pipeline lag: newest change in the app database vs newest change that reached raw."""
    results = []
    threshold_s = settings.freshness_threshold_min * 60
    for table in TABLES:
        source_latest = _scalar(
            app, f"SELECT extract(epoch FROM max({CHANGE_COLUMN[table]})) FROM {table}"
        )
        raw_latest_ms = _scalar(wh, f"SELECT max(source_ts_ms) FROM raw.cdc_{table}")
        if source_latest is None:
            lag_s = 0.0
        elif raw_latest_ms is None:
            lag_s = float("inf")
        else:
            lag_s = max(0.0, float(source_latest) - raw_latest_ms / 1000)
        status: Status = "fail" if lag_s > threshold_s else "pass"
        observed = "never loaded" if lag_s == float("inf") else f"lag {lag_s / 60:.1f} min"
        results.append(
            CheckResult(
                "freshness", table, status, observed, f"<= {settings.freshness_threshold_min} min"
            )
        )
    return results


def check_raw_duplicate_lsn(settings, wh, app) -> list[CheckResult]:
    """Recently loaded change events that were already loaded once (same change, new offset)."""
    results = []
    for table in TABLES:
        n = _scalar(
            wh,
            f"SELECT count(*) FROM raw.cdc_{table} AS r WHERE r.ingested_at > now() - {RECENT} "
            f"AND EXISTS (SELECT 1 FROM raw.cdc_{table} AS o WHERE o.lsn = r.lsn AND o.pk = r.pk "
            "AND o.op = r.op AND o.id <> r.id)",
        )
        results.append(
            CheckResult("raw_duplicate_lsn", table, "warn" if n else "pass", f"{n} duplicates", "0")
        )
    return results


def check_lsn_regression(settings, wh, app) -> list[CheckResult]:
    """Recently loaded events older (lower lsn) than a version already loaded for the same row."""
    results = []
    for table in TABLES:
        n = _scalar(
            wh,
            f"SELECT count(*) FROM raw.cdc_{table} AS r WHERE r.ingested_at > now() - {RECENT} "
            f"AND EXISTS (SELECT 1 FROM raw.cdc_{table} AS o WHERE o.pk = r.pk AND o.id < r.id "
            "AND o.lsn > r.lsn) "
            # a change loaded before is a duplicate (raw_duplicate_lsn), not a late version
            f"AND NOT EXISTS (SELECT 1 FROM raw.cdc_{table} AS d WHERE d.pk = r.pk "
            "AND d.lsn = r.lsn AND d.id < r.id)",
        )
        results.append(
            CheckResult("lsn_regression", table, "warn" if n else "pass", f"{n} out-of-order", "0")
        )
    return results


def check_schema_drift(settings, wh, app) -> list[CheckResult]:
    """Fields in recent CDC events vs the table contract (infra/contracts/<table>.yml)."""
    results = []
    for table in TABLES:
        rows = wh.execute(
            f"SELECT DISTINCT jsonb_object_keys(after) FROM raw.cdc_{table} "
            f"WHERE after IS NOT NULL AND ingested_at > now() - {RECENT}"
        ).fetchall()
        observed = {r[0] for r in rows}
        if not observed:
            results.append(
                CheckResult("schema_drift", table, "pass", "no recent events", "contract")
            )
            continue
        added, removed = field_diff(observed, load_contract(table))
        status: Status = "fail" if added or removed else "pass"
        detail = (
            f"added={sorted(added)} removed={sorted(removed)}" if status == "fail" else "matches"
        )
        results.append(CheckResult("schema_drift", table, status, detail, "contract fields"))
    return results


def check_invalid_values(settings, wh, app) -> list[CheckResult]:
    """Recent purchases breaking a validity rule, one result per rule (column)."""
    tenors = ",".join(f"'{t}'" for t in VALID_TENORS)
    rules = {
        "transactions.amount": "(after->>'amount')::numeric <= 0",
        "transactions.tenor_months": f"after->>'tenor_months' NOT IN ({tenors})",
    }
    results = []
    for target, invalid in rules.items():
        n = _scalar(
            wh,
            "SELECT count(DISTINCT pk) FROM raw.cdc_transactions "
            f"WHERE after IS NOT NULL AND ingested_at > now() - {RECENT} AND {invalid}",
        )
        status: Status = "fail" if n else "pass"
        results.append(CheckResult("invalid_values", target, status, f"{n} invalid rows", "0"))
    return results


def check_volume_anomaly(settings, wh, app) -> list[CheckResult]:
    """New purchases in the last complete window vs the previous windows (z-score)."""
    window_ms = settings.volume_window_min * 60_000
    current = int(time.time() * 1000) // window_ms - 1
    first = current - settings.volume_baseline_windows
    rows = wh.execute(
        "SELECT source_ts_ms / %s AS w, count(*) FROM raw.cdc_transactions "
        "WHERE op = 'c' AND source_ts_ms >= %s AND source_ts_ms < %s GROUP BY 1",
        (window_ms, first * window_ms, (current + 1) * window_ms),
    ).fetchall()
    counts = {int(w): int(n) for w, n in rows}
    z = volume_zscore(counts, current, settings.volume_baseline_windows)
    if z is None:
        return [
            CheckResult("volume_anomaly", "transactions", "pass", "not enough history", "|z| <= 3")
        ]
    status: Status = "fail" if abs(z) > settings.volume_z_threshold else "pass"
    observed = f"{counts.get(current, 0)} events, z={z:.1f}"
    return [
        CheckResult(
            "volume_anomaly",
            "transactions",
            status,
            observed,
            f"|z| <= {settings.volume_z_threshold}",
        )
    ]


# Installments have no creation time of their own; they are created with their purchase and
# counted by its created_at, so the raw tables of both must have caught up to the cut-off.
RECONCILE_CUTOFF_TABLES = {"installments": ("installments", "transactions")}


def _count_up_to(table: str, prefix: str, cut: str) -> str:
    """Rows of `table` created up to the cut-off; prefix "" for the app, "staging.stg_" for dbt."""
    if table == "installments":
        return (
            f"SELECT count(*) FROM {prefix}installments AS i "
            f"JOIN {prefix}transactions AS t USING (txn_id) WHERE t.created_at <= {cut}"
        )
    column = "paid_at" if table == "repayments" else "created_at"
    return f"SELECT count(*) FROM {prefix}{table} WHERE {column} <= {cut}"


def check_reconciliation(settings, wh, app) -> list[CheckResult]:
    """Row counts (and purchase amounts) in the app vs staging, up to a common cut-off.

    The cut-off is one minute before the newest change in raw, so rows still in flight do not
    count as missing.
    """
    results = []
    cut = "to_timestamp(%s / 1000.0)"
    for table in TABLES:
        newest = [
            _scalar(wh, f"SELECT max(source_ts_ms) FROM raw.cdc_{source}")
            for source in RECONCILE_CUTOFF_TABLES.get(table, (table,))
        ]
        if None in newest:
            results.append(CheckResult("reconciliation", table, "pass", "nothing loaded yet", "-"))
            continue
        cutoff_ms = min(newest) - 60_000
        app_count = _scalar(app, _count_up_to(table, "", cut), (cutoff_ms,))
        wh_count = _scalar(wh, _count_up_to(table, "staging.stg_", cut), (cutoff_ms,))
        diff = pct_diff(app_count, wh_count)
        observed = f"app {app_count} vs warehouse {wh_count}"
        if table == "transactions":
            app_sum = _scalar(
                app,
                f"SELECT coalesce(sum(amount), 0) FROM transactions WHERE created_at <= {cut}",
                (cutoff_ms,),
            )
            wh_sum = _scalar(
                wh,
                "SELECT coalesce(sum(amount), 0) FROM staging.stg_transactions "
                f"WHERE created_at <= {cut}",
                (cutoff_ms,),
            )
            diff = max(diff, pct_diff(float(app_sum), float(wh_sum)))
            observed += f"; amount {app_sum} vs {wh_sum}"
        status: Status = "fail" if diff > settings.reconciliation_tolerance else "pass"
        results.append(
            CheckResult(
                "reconciliation",
                table,
                status,
                observed,
                f"diff <= {settings.reconciliation_tolerance:.1%}",
            )
        )
    return results


CHECKS: dict[str, Callable] = {
    "freshness": check_freshness,
    "raw_duplicate_lsn": check_raw_duplicate_lsn,
    "lsn_regression": check_lsn_regression,
    "schema_drift": check_schema_drift,
    "invalid_values": check_invalid_values,
    "volume_anomaly": check_volume_anomaly,
    "reconciliation": check_reconciliation,
}


def _store(wh, result: CheckResult, webhook: str | None) -> None:
    wh.execute(
        "INSERT INTO ops.dq_results (check_type, target, status, observed, expected) "
        "VALUES (%s, %s, %s, %s, %s)",
        (result.check_type, result.target, result.status, result.observed, result.expected),
    )
    if result.status == "fail":
        raise_alert(wh, result, webhook)


def run_checks_with(
    settings, wh, app, names: Sequence[str] | None = None, persist: bool = True
) -> list[CheckResult]:
    names = list(names or CHECKS)
    unknown = [n for n in names if n not in CHECKS]
    if unknown:
        raise ValueError(f"unknown check(s): {', '.join(unknown)}; choose from {', '.join(CHECKS)}")
    results: list[CheckResult] = []
    for name in names:
        try:
            produced = CHECKS[name](settings, wh, app)
        except Exception as exc:  # noqa: BLE001 - one broken check must not hide the others
            for conn in (wh, app):
                try:
                    conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
            produced = [
                CheckResult(name, "*", "error", f"{type(exc).__name__}: {exc}"[:500], "no error")
            ]
        results.extend(produced)
        if persist:
            for result in produced:
                _store(wh, result, getattr(settings, "alert_webhook", None))
            wh.commit()
    return results


def run_checks(settings, names: Sequence[str] | None = None) -> list[CheckResult]:
    from pdp.db import connect

    with connect(settings.wh_dsn) as wh, connect(settings.app_dsn) as app:
        return run_checks_with(settings, wh, app, names)
