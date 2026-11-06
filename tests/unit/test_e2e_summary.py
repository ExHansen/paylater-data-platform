from datetime import UTC, datetime, timedelta
from pathlib import Path

from pdp.e2e import (
    OVERRIDES,
    SCHEDULE,
    E2EResult,
    paused_dags,
    timeline_lines,
    write_summary,
)
from pdp.quality.mttd import MttdRow
from pdp.simulator.faults import FAULTS

T0 = datetime(2026, 11, 6, 3, 0, tzinfo=UTC)


def test_schedule_injects_every_fault_once_and_overrides_are_settings():
    from pdp.config import Settings

    assert sorted(fault for _, fault, _ in SCHEDULE) == sorted(FAULTS)
    assert set(OVERRIDES) <= set(Settings.model_fields)


def test_airflow_import_errors_ok_accepts_empty_or_no_data():
    assert E2EResult(airflow_import_errors="").airflow_import_errors_ok
    assert E2EResult(airflow_import_errors="No data found\n").airflow_import_errors_ok
    assert not E2EResult(airflow_import_errors="dags/pdp.py: SyntaxError").airflow_import_errors_ok


def test_summary_shows_detection_and_state_before_the_fault(tmp_path: Path):
    result = E2EResult(
        alerts_by_rule={"RR1_velocity": 4},
        latency_p50_ms=310.0,
        latency_p95_ms=1450.5,
        mttd=[
            MttdRow(1, "loader_stall", "freshness", T0, T0 + timedelta(seconds=150), 150.0),
            MttdRow(2, "volume_drop", "volume_anomaly", T0, None, None),
        ],
        already_failing=["volume_drop"],
        airflow_hourly_ok=False,
        airflow_hourly_tail="Task failed: dbt_build",
    )
    text = write_summary(result, tmp_path / "s.md").read_text(encoding="utf-8")
    assert "| loader_stall | freshness | passing | 2.5 min |" in text
    assert "| volume_drop | volume_anomaly | already failing | not detected |" in text
    assert "| Alert latency p95 | 1,450 ms |" in text or "| Alert latency p95 | 1,451 ms |" in text
    assert "| RR1_velocity | 4 |" in text
    assert "Task failed: dbt_build" in text


def test_timeline_shows_each_run_with_its_worst_result():
    rows = [
        # ordered by run_at, target, as the query returns them; ties keep the first row
        (T0 - timedelta(seconds=40), "pass", "transactions", "lag 0.1 min"),
        (T0 - timedelta(seconds=40), "pass", "users", "lag 0.0 min"),
        (T0 + timedelta(seconds=90), "fail", "transactions", "lag 1.4 min"),
        (T0 + timedelta(seconds=90), "pass", "users", "lag 0.0 min"),
    ]
    assert timeline_lines(rows, T0) == [
        "-0.7 min  pass  transactions: lag 0.1 min",
        "+1.5 min  fail  transactions: lag 1.4 min",
    ]


def test_summary_reports_check_run_time_and_timelines(tmp_path: Path):
    result = E2EResult(
        check_run_seconds=[10.0, 14.0, 30.0],
        timelines={"loader_stall": ["+1.5 min  fail  transactions: lag 1.4 min"]},
    )
    text = write_summary(result, tmp_path / "s.md").read_text(encoding="utf-8")
    assert "| Data-quality run time | median 14 s, max 30 s (3 runs) |" in text
    assert "<summary>loader_stall</summary>" in text and "+1.5 min  fail" in text


def test_paused_dags_are_read_from_the_airflow_listing():  # review I3
    listing = (
        "some log line\n"
        '[{"dag_id": "pdp_hourly", "is_paused": "False"}, '
        '{"dag_id": "pdp_daily_dq", "is_paused": "True"}]\n'
    )
    assert paused_dags(listing) == ["pdp_daily_dq"]
    assert paused_dags("no json here") is None


def test_summary_names_the_rule_a_fault_is_matched_on(tmp_path: Path):
    row = MttdRow(3, "invalid_tenor", "invalid_values", T0, T0, 0.0, "transactions.tenor_months")
    text = write_summary(E2EResult(mttd=[row]), tmp_path / "s.md").read_text(encoding="utf-8")
    assert "| invalid_tenor | invalid_values (transactions.tenor_months) |" in text


def test_paused_dags_skip_bracketed_log_lines():
    listing = (
        "[2026-11-06T09:00:00Z] {dagbag.py:1} INFO - Filling up the DagBag\n"
        '[{"dag_id": "pdp_hourly", "is_paused": false}, '
        '{"dag_id": "pdp_daily_dq", "is_paused": true}]\n'
    )
    assert paused_dags(listing) == ["pdp_daily_dq"]
    assert paused_dags("[]") == []


def test_errors_come_before_the_long_sections(tmp_path: Path):
    result = E2EResult(
        fault_errors={"regulatory_report": "PermissionError: denied"},
        timelines={"loader_stall": ["+1.5 min  fail  transactions: lag 1.4 min"]},
    )
    text = write_summary(result, tmp_path / "s.md").read_text(encoding="utf-8")
    assert text.index("PermissionError") < text.index("## Risk alerts by rule")
