import json
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from pdp.config import Settings
from pdp.loader.writer import RawMessage, write_batch
from pdp.ops import log_fault
from pdp.quality.checks import (
    TABLES,
    check_freshness,
    check_invalid_values,
    check_lsn_regression,
    check_raw_duplicate_lsn,
    check_reconciliation,
    check_schema_drift,
    run_checks_with,
)
from pdp.quality.mttd import compute_mttd

FIX = Path("tests/fixtures/debezium")
TOPIC = "paylater.public.transactions"
KEY = b'{"txn_id": "6d8f4b3c-1111-4aaa-8bbb-000000000001"}'
S = Settings(_env_file=None)


def load(name: str, **after_overrides) -> bytes:
    value = json.loads((FIX / name).read_text())
    value["after"] = {**value["after"], **after_overrides}
    return json.dumps(value).encode()


def write(conn, *values: bytes) -> None:
    msgs = [RawMessage(TOPIC, 0, i, KEY, v) for i, v in enumerate(values)]
    write_batch(conn, msgs)


def by_target(results, table="transactions"):
    return [r for r in results if r.target == table]


@pytest.mark.pg
def test_schema_drift_detects_new_field(fresh_db):  # Review Focus 5
    write(fresh_db, load("drift.json"))
    assert [r.status for r in by_target(check_schema_drift(S, fresh_db, None))] == ["fail"]
    assert "promo_code" in by_target(check_schema_drift(S, fresh_db, None))[0].observed


@pytest.mark.pg
def test_schema_matching_contract_passes(fresh_db):
    write(fresh_db, load("create.json"))
    assert [r.status for r in by_target(check_schema_drift(S, fresh_db, None))] == ["pass"]


@pytest.mark.pg
def test_invalid_values(fresh_db):
    write(fresh_db, load("create.json", amount="0.00"), load("create.json", tenor_months=24))
    # both rows share the same txn_id (pk), so they count once
    result = check_invalid_values(S, fresh_db, None)[0]
    assert result.status == "fail" and result.observed == "1 invalid rows"


@pytest.mark.pg
def test_duplicates_and_out_of_order(fresh_db):
    create, update = (FIX / "create.json").read_bytes(), (FIX / "update.json").read_bytes()
    write(fresh_db, create, update, create)  # create re-delivered after the newer update
    assert by_target(check_raw_duplicate_lsn(S, fresh_db, None))[0].status == "warn"
    assert by_target(check_lsn_regression(S, fresh_db, None))[0].status == "warn"


@pytest.mark.pg
def test_freshness_fails_when_raw_lags_the_app(fresh_db):
    fresh_db.execute(
        "INSERT INTO users VALUES (gen_random_uuid(), 'x', '1', '0', 'Bali', 'Denpasar', "
        "'verified', now(), now())"
    )
    users = by_target(check_freshness(S, fresh_db, fresh_db), "users")[0]
    assert users.status == "fail" and users.observed == "never loaded"
    merchants = by_target(check_freshness(S, fresh_db, fresh_db), "merchants")[0]
    assert merchants.status == "pass"  # no source changes, nothing to be late about


@pytest.mark.pg
def test_runner_stores_results_and_alerts(fresh_db):
    write(fresh_db, load("create.json", amount="-5.00"))
    run_checks_with(S, fresh_db, fresh_db, ["invalid_values"])
    stored = fresh_db.execute("select status from ops.dq_results").fetchall()
    alerts = fresh_db.execute("select check_type from ops.alerts").fetchall()
    assert stored == [("fail",)] and alerts == [("invalid_values",)]


@pytest.mark.pg
def test_mttd_pairs_fault_with_first_detection(fresh_db):
    log_fault(fresh_db, "invalid_amount", {})
    fresh_db.execute(
        "INSERT INTO ops.dq_results (check_type, target, status, observed, expected, run_at) "
        "SELECT 'invalid_values', 'transactions', 'fail', '1', '0', injected_at + interval '70 s' "
        "FROM ops.fault_log"
    )
    rows = compute_mttd(fresh_db)
    assert rows[0].check_type == "invalid_values"
    assert rows[0].mttd_seconds == pytest.approx(70, abs=1)


@pytest.mark.pg
def test_reconciliation_is_exact_and_catches_one_lost_installment(fresh_db):
    from pdp.simulator.generator import generate_history
    from pdp.simulator.seed import write_history

    now = datetime.now(ZoneInfo("Asia/Jakarta"))
    write_history(fresh_db, generate_history(7, 2, now, n_users=25, n_merchants=5))
    for table in TABLES:  # staging views straight over the app tables stand in for dbt
        fresh_db.execute(f"CREATE VIEW staging.stg_{table} AS SELECT * FROM public.{table}")
        fresh_db.execute(
            f"INSERT INTO raw.cdc_{table} (op, source_ts_ms, lsn, kafka_topic, kafka_partition, "
            "kafka_offset) VALUES ('c', %s, 1, 't', 0, 0)",
            (int(time.time() * 1000),),
        )
    exact = Settings(_env_file=None, reconciliation_tolerance=0.0)

    def statuses():
        return {r.target: r.status for r in check_reconciliation(exact, fresh_db, fresh_db)}

    assert statuses() == {table: "pass" for table in TABLES}
    lost = fresh_db.execute("SELECT installment_id FROM installments LIMIT 1").fetchone()[0]
    fresh_db.execute(
        "CREATE OR REPLACE VIEW staging.stg_installments AS SELECT * FROM public.installments "
        f"WHERE installment_id <> '{lost}'"
    )
    assert statuses() == {table: "fail" if table == "installments" else "pass" for table in TABLES}
