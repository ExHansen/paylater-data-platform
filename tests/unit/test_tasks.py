import ast
import json
from datetime import UTC, datetime
from pathlib import Path

import yaml

from pdp import tasks


class RecordingConn:
    def __init__(self):
        self.rows = []

    def execute(self, sql, params=None):
        self.rows.append((sql, params))

    def commit(self):
        pass


def test_regulatory_previous_month_uses_wib(monkeypatch):
    called = {}
    monkeypatch.setattr(tasks, "build_report", lambda s, m: called.setdefault("month", m))
    tasks.regulatory_previous_month(datetime(2026, 11, 1, 19, 0, tzinfo=UTC))  # 2 Nov 02:00 WIB
    assert called["month"] == "2026-10"
    called.clear()
    tasks.regulatory_previous_month(datetime(2026, 10, 31, 18, 0, tzinfo=UTC))  # 1 Nov 01:00 WIB
    assert called["month"] == "2026-10"


def test_dbt_results_mapping(tmp_path):
    run_results = {
        "results": [
            {
                "unique_id": "test.pdp.positive_stg_transactions_amount.1a",
                "status": "warn",
                "failures": 3,
            },
            {
                "unique_id": "test.pdp.unique_fct_transactions_txn_id.2b",
                "status": "pass",
                "failures": 0,
            },
            {"unique_id": "model.pdp.fct_transactions", "status": "success", "failures": None},
            {"unique_id": "test.pdp.installments_sum_to_amount", "status": "fail", "failures": 2},
        ]
    }
    path = tmp_path / "run_results.json"
    path.write_text(json.dumps(run_results))
    conn = RecordingConn()
    assert tasks.dbt_results_to_ops(path, conn=conn) == 3
    stored = [p for sql, p in conn.rows if "ops.dq_results" in sql]
    assert [(p[1], p[2]) for p in stored] == [
        ("positive_stg_transactions_amount", "warn"),
        ("unique_fct_transactions_txn_id", "pass"),
        ("installments_sum_to_amount", "fail"),
    ]
    assert sum("ops.alerts" in sql for sql, _ in conn.rows) == 1


def test_dag_files_only_call_pdp_tasks():
    files = sorted(Path("airflow/dags").glob("*.py"))
    assert [f.stem for f in files] == [
        "pdp_daily_dq",
        "pdp_hourly",
        "pdp_monthly_regulatory",
        "pdp_quality_5min",
    ]
    for f in files:
        source = f.read_text(encoding="utf-8")
        compile(ast.parse(source), str(f), "exec")
        assert "pdp.tasks" in source and "catchup=False" in source


def test_airflow_service_in_full_profile():
    compose = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    airflow = compose["services"]["airflow"]
    assert airflow["profiles"] == ["full"] and "8080:8080" in airflow["ports"]
    assert airflow["environment"]["AIRFLOW__CORE__DEFAULT_TIMEZONE"] == "Asia/Jakarta"
