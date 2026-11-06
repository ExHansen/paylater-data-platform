"""End-to-end scenario: full stack, live traffic, every fault, measured detection.

Timeline (minutes after live traffic starts):
  3  invalid_amount, invalid_tenor, schema_drift
  4  duplicate_events, out_of_order
  5  loader_stall (3 min)
  10 volume_drop (2 min; two minutes after the loader is back, so the volume check is clean)
  12 reconciliation_gap (the first batch after the simulator restarts is lost)
Checks start every minute (on a fixed cadence); the run ends at minute 16.

Thresholds are tightened for the run (OVERRIDES) so each fault is visible within minutes.
Reconciliation is exact: with the one-minute cut-off, app and staging agree row for row unless
data was lost, and a single lost batch is far below the default 0.1% tolerance.
"""

from __future__ import annotations

import json
import os
import statistics
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from pdp.config import Settings
from pdp.db import connect
from pdp.quality.checks import run_checks
from pdp.quality.mttd import MttdRow, compute_mttd

WIB = ZoneInfo("Asia/Jakarta")
OVERRIDES = {
    "freshness_threshold_min": 1,
    "volume_window_min": 1,
    "volume_baseline_windows": 10,
    "reconciliation_tolerance": 0.0,
}
SCHEDULE: tuple[tuple[float, str, dict], ...] = (
    (3, "invalid_amount", {}),
    (3, "invalid_tenor", {}),
    (3, "schema_drift", {}),
    (4, "duplicate_events", {}),
    (4, "out_of_order", {}),
    (5, "loader_stall", {"minutes": 3}),
    (10, "volume_drop", {"minutes": 2}),
    (12, "reconciliation_gap", {}),
)
LIVE_MINUTES = 16
# The seed lands in raw as one burst of purchases; keep it out of the volume baseline.
SEED_TO_LIVE_MIN_S = 150
# Live traffic is ~30 messages/s and Quix commits every 5 s, so a caught-up stream lags < 500.
STREAM_CAUGHT_UP_LAG = 500
MARTS = (
    "dim_user",
    "fct_transactions",
    "fct_installments",
    "mart_portfolio_daily",
    "mart_delinquency",
    "mart_collections",
    "mart_merchant_performance",
)


@dataclass
class E2EResult:
    live_started_at: datetime | None = None
    alerts_by_rule: dict[str, int] = field(default_factory=dict)
    latency_p50_ms: float | None = None
    latency_p95_ms: float | None = None
    mttd: list[MttdRow] = field(default_factory=list)
    already_failing: list[str] = field(default_factory=list)
    mart_counts: dict[str, int] = field(default_factory=dict)
    twp90_latest: float | None = None
    report_path: Path | None = None
    airflow_import_errors: str = ""
    airflow_hourly_ok: bool = False
    airflow_hourly_tail: str = ""
    fault_errors: dict[str, str] = field(default_factory=dict)
    check_run_seconds: list[float] = field(default_factory=list)
    timelines: dict[str, list[str]] = field(default_factory=dict)
    summary_path: Path = Path("e2e-summary.md")

    @property
    def airflow_import_errors_ok(self) -> bool:
        text = self.airflow_import_errors.strip().lower()
        return text == "" or "no data found" in text


def wait_for(description: str, condition: Callable[[], bool], timeout_s: float, every_s: float = 5):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if condition():
                return
        except Exception:  # noqa: BLE001 - services may still be starting
            pass
        time.sleep(every_s)
    raise TimeoutError(f"timed out after {timeout_s:.0f}s waiting for: {description}")


def scalar(dsn: str, sql: str, params: tuple = ()):
    with connect(dsn) as conn:
        return conn.execute(sql, params).fetchone()[0]


class QualityLoop(threading.Thread):
    """Starts every data-quality check once a minute (fixed cadence) until stopped."""

    def __init__(self, settings: Settings, log: Callable[[str], None]):
        super().__init__(daemon=True)
        self.settings, self.log, self.stop_event = settings, log, threading.Event()
        self.durations: list[float] = []

    def run(self) -> None:
        started = time.monotonic()
        runs = 0
        while not self.stop_event.is_set():
            begin = time.monotonic()
            try:
                results = run_checks(self.settings)
                failing = [f"{r.check_type}/{r.target}" for r in results if r.status != "pass"]
                self.log(f"checks: {len(failing)} not passing {failing[:6]}")
            except Exception as exc:  # noqa: BLE001
                self.log(f"checks errored: {exc}")
            self.durations.append(time.monotonic() - begin)
            runs += 1
            self.stop_event.wait(max(0.0, started + runs * 60 - time.monotonic()))


def _inject(settings: Settings, fault: str, opts: dict, errors: dict, log) -> None:
    from pdp.cli import compose
    from pdp.simulator.faults import FaultContext, inject

    def producer():
        from confluent_kafka import Producer

        return Producer({"bootstrap.servers": settings.kafka_bootstrap})

    try:
        with connect(settings.app_dsn) as app_conn, connect(settings.wh_dsn) as wh_conn:
            ctx = FaultContext(
                settings=settings,
                app_conn=app_conn,
                wh_conn=wh_conn,
                compose=lambda *args: compose(*args, profile="full"),
                producer_factory=producer,
            )
            fault_id = inject(fault, ctx, **opts)
        log(f"injected {fault} (fault_id {fault_id})")
    except Exception as exc:  # noqa: BLE001 - record and continue with the scenario
        errors[fault] = f"{type(exc).__name__}: {exc}"
        log(f"injecting {fault} failed: {exc}")


def _dbt_build(settings: Settings) -> str | None:
    """Run `dbt build`; None on success, otherwise what failed."""
    from dbt.cli.main import dbtRunner

    from pdp.cli import dbt_env

    for key, value in dbt_env(settings.wh_dsn).items():
        os.environ.setdefault(key, value)
    res = dbtRunner().invoke(["build", "--project-dir", "dbt", "--profiles-dir", "dbt"])
    if res.success:
        return None
    if res.exception is not None:
        return f"{type(res.exception).__name__}: {res.exception}"
    failed = [
        f"{r.node.unique_id}: {r.status} ({r.message})"
        for r in getattr(res.result, "results", [])
        if str(r.status) in ("error", "fail")
    ]
    return "; ".join(failed[:10]) or "dbt build failed"


def consumer_lag(bootstrap: str, group: str) -> int:
    """Messages a consumer group has not committed yet, summed over its partitions."""
    from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": bootstrap})
    request = [ConsumerGroupTopicPartitions(group)]
    committed = admin.list_consumer_group_offsets(request)[group].result(timeout=15)
    if not committed.topic_partitions:
        raise RuntimeError(f"consumer group {group} has not committed yet")
    probe = Consumer({"bootstrap.servers": bootstrap, "group.id": f"{group}-lag-probe"})
    try:
        lag = 0
        for tp in committed.topic_partitions:
            low, high = probe.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), 10)
            lag += max(0, high - (tp.offset if tp.offset >= 0 else low))
        return lag
    finally:
        probe.close()


def raw_caught_up(dsn: str, counts: dict[str, int]) -> bool:
    """Every seeded table has at least as many raw rows as were seeded."""
    with connect(dsn) as wh:
        return all(
            wh.execute(f"SELECT count(*) FROM raw.cdc_{table}").fetchone()[0] >= n
            for table, n in counts.items()
        )


def _airflow(*args: str):
    from pdp.cli import compose

    return compose(
        "exec", "-T", "airflow", "airflow", *args, profile="full", check=False, capture=True
    )


SEVERITY = {"pass": 0, "error": 1, "warn": 2, "fail": 3}


def timeline_lines(rows, injected_at: datetime) -> list[str]:
    """One line per check run: offset from the fault, worst status and its observation.

    `rows` are (run_at, status, target, observed) for one check type, ordered by run_at.
    """
    runs: dict[datetime, tuple[str, str, str]] = {}
    for run_at, status, target, observed in rows:
        worst = runs.get(run_at)
        if worst is None or SEVERITY.get(status, 0) > SEVERITY.get(worst[0], 0):
            runs[run_at] = (status, target, observed)  # ties keep the first row
    return [
        f"{(run_at - injected_at).total_seconds() / 60:+.1f} min  {status}  {target}: {observed}"
        for run_at, (status, target, observed) in sorted(runs.items())
    ]


def collect(settings: Settings, result: E2EResult) -> None:
    since = result.live_started_at or datetime.now(UTC) - timedelta(hours=1)
    with connect(settings.wh_dsn) as wh:
        rows = wh.execute(
            "SELECT unnest(rules), count(*) FROM online.risk_alerts WHERE created_at >= %s "
            "GROUP BY 1 ORDER BY 1",
            (since,),
        ).fetchall()
        result.alerts_by_rule = {rule: count for rule, count in rows}
        p50, p95 = wh.execute(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms), "
            "percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) "
            "FROM online.risk_alerts WHERE created_at >= %s",
            (since,),
        ).fetchone()
        result.latency_p50_ms, result.latency_p95_ms = p50, p95
        result.mttd = compute_mttd(wh)
        # A detection only counts if the check was passing just before the fault (one run).
        result.already_failing = [
            m.fault_type
            for m in result.mttd
            if wh.execute(
                "SELECT count(*) FROM ops.dq_results WHERE check_type = %s "
                "AND status IN ('fail', 'warn') "
                "AND run_at >= %s - interval '90 seconds' AND run_at < %s",
                (m.check_type, m.injected_at, m.injected_at),
            ).fetchone()[0]
        ]
        for m in result.mttd:
            rows = wh.execute(
                "SELECT run_at, status, target, observed FROM ops.dq_results "
                "WHERE check_type = %s AND run_at >= %s - interval '2 minutes' "
                "AND run_at < %s + interval '6 minutes' ORDER BY run_at, target",
                (m.check_type, m.injected_at, m.injected_at),
            ).fetchall()
            result.timelines[m.fault_type] = timeline_lines(rows, m.injected_at)
        for mart in MARTS:
            count = wh.execute(f"SELECT count(*) FROM marts.{mart}").fetchone()[0]
            result.mart_counts[mart] = count
        twp90 = wh.execute(
            "SELECT sum(dpd_90_plus) / nullif(sum(total_outstanding), 0) "
            "FROM marts.mart_delinquency "
            "WHERE as_of_date = (SELECT max(as_of_date) FROM marts.mart_delinquency)"
        ).fetchone()[0]
        result.twp90_latest = None if twp90 is None else float(twp90)


def write_summary(result: E2EResult, path: Path) -> Path:
    import_errors = "none" if result.airflow_import_errors_ok else "see below"

    def detected(m: MttdRow) -> str:
        return "not detected" if m.mttd_seconds is None else f"{m.mttd_seconds / 60:.1f} min"

    def before(m: MttdRow) -> str:
        return "already failing" if m.fault_type in result.already_failing else "passing"

    lines = [
        f"# End-to-end run — {datetime.now(WIB):%Y-%m-%d %H:%M} WIB",
        "",
        "| Metric | Value |",
        "|---|---|",
        f"| Alert latency p50 | {_ms(result.latency_p50_ms)} |",
        f"| Alert latency p95 | {_ms(result.latency_p95_ms)} |",
        f"| TWP90 (latest snapshot) | {_pct(result.twp90_latest)} |",
        f"| Airflow DAG import errors | {import_errors} |",
        f"| Airflow `pdp_hourly` test run | {'ok' if result.airflow_hourly_ok else 'failed'} |",
        f"| Regulatory report | {result.report_path or 'not built'} |",
        f"| Data-quality run time | {_run_time(result.check_run_seconds)} |",
        "",
        "## Risk alerts by rule",
        "",
        "| Rule | Alerts |",
        "|---|---|",
        *[f"| {rule} | {count} |" for rule, count in result.alerts_by_rule.items()],
        "",
        "## Injected faults and time to detect",
        "",
        "| Fault | Detected by | Check before the fault | Detected after |",
        "|---|---|---|---|",
        *[
            f"| {m.fault_type} | {m.check_type} | {before(m)} | {detected(m)} |"
            for m in result.mttd
        ],
        "",
        "## Detection timelines",
        "",
        "Every run of the matching check from two minutes before each fault to six after.",
        "",
        *[
            line
            for fault, lines in result.timelines.items()
            for line in (
                "<details>",
                f"<summary>{fault}</summary>",
                "",
                "```",
                *lines,
                "```",
                "</details>",
                "",
            )
        ],
        "## Mart row counts",
        "",
        *[f"- `{name}`: {count}" for name, count in result.mart_counts.items()],
    ]
    if result.fault_errors:
        lines += ["", "## Errors", ""]
        lines += [f"- {name}: {error}" for name, error in result.fault_errors.items()]
    if not result.airflow_import_errors_ok:
        lines += ["", "## Airflow import errors", "", "```", result.airflow_import_errors, "```"]
    if not result.airflow_hourly_ok and result.airflow_hourly_tail:
        lines += [
            "",
            "## Airflow `pdp_hourly` output",
            "",
            "```",
            result.airflow_hourly_tail,
            "```",
        ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _run_time(seconds: list[float]) -> str:
    if not seconds:
        return "n/a"
    median = statistics.median(seconds)
    return f"median {median:.0f} s, max {max(seconds):.0f} s ({len(seconds)} runs)"


def _ms(value) -> str:
    return "n/a" if value is None else f"{value:,.0f} ms"


def _pct(value) -> str:
    return "n/a" if value is None else f"{value:.2%}"


def _sleep_until(moment: datetime) -> None:
    delay = (moment - datetime.now(UTC)).total_seconds()
    if delay > 0:
        time.sleep(delay)


def run(
    settings: Settings, live_minutes: float = LIVE_MINUTES, seed_months: int = 6, log=print
) -> E2EResult:
    from pdp.cli import compose
    from pdp.reports.regulatory import build_report
    from pdp.simulator.seed import seed

    s = settings.model_copy(update=OVERRIDES)
    result = E2EResult()
    # Create the reports folder first: a bind mount would otherwise create it owned by root.
    s.reports_dir.mkdir(parents=True, exist_ok=True)
    log("starting the full stack")
    compose("up", "-d", "--build", "--wait", profile="full")

    counts = seed(s, seed_months, s.seed)
    seeded_at = datetime.now(UTC)  # every seeded row was created before this moment
    log(f"seeded {counts}")
    wait_for("the seeded history to reach raw", lambda: raw_caught_up(s.wh_dsn, counts), 600)
    log("history loaded; first dbt build")
    if failure := _dbt_build(s):
        raise RuntimeError(f"initial dbt build failed: {failure}")
    wait_for(
        "live traffic from the simulator",
        lambda: (
            scalar(
                s.app_dsn, "SELECT count(*) FROM transactions WHERE created_at > %s", (seeded_at,)
            )
            > 0
        ),
        timeout_s=240,
    )
    # Live alerts are only measured once the risk stream has worked through the seeded history.
    wait_for(
        "the risk stream to catch up with the seed",
        lambda: consumer_lag(s.kafka_bootstrap, "pdp-risk") < STREAM_CAUGHT_UP_LAG,
        timeout_s=900,
        every_s=10,
    )
    _sleep_until(seeded_at + timedelta(seconds=SEED_TO_LIVE_MIN_S))
    result.live_started_at = live_start = datetime.now(UTC)
    log(f"live traffic started at {live_start:%H:%M:%S} UTC")

    loop = QualityLoop(s, log)
    loop.start()
    threads = []
    for minute, fault, opts in SCHEDULE:
        _sleep_until(live_start + timedelta(minutes=minute))
        args = (s, fault, opts, result.fault_errors, log)
        thread = threading.Thread(target=_inject, args=args, daemon=True)
        thread.start()
        threads.append(thread)
    _sleep_until(live_start + timedelta(minutes=live_minutes))
    for thread in threads:
        thread.join(timeout=300)
    loop.stop_event.set()
    loop.join(timeout=120)
    result.check_run_seconds = loop.durations
    run_checks(s)  # one last pass

    log("final dbt build, Airflow checks and report")
    if failure := _dbt_build(s):
        result.fault_errors["final_dbt_build"] = failure
    errors = _airflow("dags", "list-import-errors")
    result.airflow_import_errors = (errors.stdout or "") + (errors.stderr or "")
    hourly = _airflow("dags", "test", "pdp_hourly")
    result.airflow_hourly_ok = hourly.returncode == 0
    if not result.airflow_hourly_ok:
        result.airflow_hourly_tail = (hourly.stdout or "")[-3000:] + (hourly.stderr or "")[-3000:]
    previous_month = (datetime.now(WIB).date().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    try:
        result.report_path = build_report(s, previous_month)
    except Exception as exc:  # noqa: BLE001
        result.fault_errors["regulatory_report"] = f"{type(exc).__name__}: {exc}"
    collect(s, result)
    write_summary(result, result.summary_path)
    log(json.dumps({"alerts": result.alerts_by_rule, "p95_ms": result.latency_p95_ms}, default=str))
    return result
