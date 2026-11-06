from datetime import UTC, datetime, timedelta
from pathlib import Path

from pdp.e2e import OVERRIDES, SCHEDULE, E2EResult, write_summary
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
