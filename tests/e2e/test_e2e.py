"""Runs the whole stack. Only the e2e workflow runs this (pytest -m e2e)."""

import pytest

from pdp.config import Settings
from pdp.e2e import run

RULES = {"RR1_velocity", "RR2_near_limit", "RR3_new_device_high", "RR4_utilization"}


@pytest.mark.e2e
def test_end_to_end():
    result = run(Settings())
    summary = result.summary_path.read_text(encoding="utf-8")
    assert not result.fault_errors, summary
    assert RULES <= set(result.alerts_by_rule), summary
    assert result.latency_p95_ms is not None and result.latency_p95_ms < 5000, summary
    assert [m.fault_type for m in result.mttd if m.detected_at is None] == [], summary
    assert result.already_failing == [], summary
    assert result.mart_counts["mart_delinquency"] > 0, summary
    assert result.report_path is not None and result.report_path.exists(), summary
    assert result.airflow_import_errors_ok and result.airflow_hourly_ok, summary
