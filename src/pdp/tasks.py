"""Plain functions the Airflow DAGs call. They hold all logic, so they test without Airflow."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from pdp.config import Settings
from pdp.db import connect
from pdp.quality.alerts import raise_alert
from pdp.quality.checks import CheckResult, run_checks
from pdp.reports.regulatory import build_report

WIB = ZoneInfo("Asia/Jakarta")
FREQUENT_CHECKS = (
    "freshness",
    "raw_duplicate_lsn",
    "lsn_regression",
    "schema_drift",
    "invalid_values",
)
DAILY_CHECKS = ("reconciliation", "volume_anomaly")
DBT_STATUS = {"pass": "pass", "warn": "warn", "fail": "fail", "error": "error"}

log = logging.getLogger("pdp.tasks")


def _summary(results: list[CheckResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    return counts


def quality_frequent() -> dict[str, int]:
    summary = _summary(run_checks(Settings(), FREQUENT_CHECKS))
    log.info("frequent checks: %s", summary)
    return summary


def quality_daily() -> dict[str, int]:
    summary = _summary(run_checks(Settings(), DAILY_CHECKS))
    log.info("daily checks: %s", summary)
    return summary


def dbt_results_to_ops(run_results_path: str | Path, conn=None) -> int:
    """Store every dbt test outcome from run_results.json in ops.dq_results (type dbt_test)."""
    results = json.loads(Path(run_results_path).read_text(encoding="utf-8"))["results"]
    tests = [r for r in results if r["unique_id"].startswith("test.")]

    def store(c) -> int:
        for test in tests:
            name = test["unique_id"].split(".")[2]
            status = DBT_STATUS.get(test["status"], "error")
            failures = test.get("failures") or 0
            result = CheckResult("dbt_test", name, status, f"{failures} failing rows", "0")
            c.execute(
                "INSERT INTO ops.dq_results (check_type, target, status, observed, expected) "
                "VALUES (%s, %s, %s, %s, %s)",
                (result.check_type, result.target, result.status, result.observed, result.expected),
            )
            if status == "fail":
                raise_alert(c, result, Settings().alert_webhook)
        c.commit()
        return len(tests)

    if conn is not None:
        return store(conn)
    with connect(Settings().wh_dsn) as owned:
        return store(owned)


def regulatory_previous_month(logical_date: datetime | None = None) -> Path:
    """Build the report for the month before `logical_date` (WIB)."""
    moment = (logical_date or datetime.now(UTC)).astimezone(WIB)
    previous = moment.date().replace(day=1) - timedelta(days=1)
    return build_report(Settings(), previous.strftime("%Y-%m"))


def record_task_failure(context: dict) -> None:
    """Airflow on_failure_callback: a failed task becomes an ops alert."""
    task = context.get("task_instance")
    target = f"{getattr(task, 'dag_id', '?')}.{getattr(task, 'task_id', '?')}"
    try:
        with connect(Settings().wh_dsn) as conn:
            conn.execute(
                "INSERT INTO ops.alerts (check_type, target, message) VALUES (%s, %s, %s)",
                ("airflow_task", target, f"task failed: {context.get('exception')}"[:500]),
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001 - the callback must never raise
        log.warning("could not record task failure: %s", exc)
