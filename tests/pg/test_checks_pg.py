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


def other_purchase(value: bytes, n: int) -> bytes:
    data = json.loads(value)
    data["after"]["txn_id"] = f"6d8f4b3c-1111-4aaa-8bbb-00000000000{n}"
    return json.dumps(data).encode()


@pytest.mark.pg
def test_invalid_values_reports_each_rule(fresh_db):  # review I6: distinct rows, per rule
    write(
        fresh_db,
        load("create.json", amount="0.00"),
        other_purchase(load("create.json", tenor_months=24), 2),
    )
    results = {r.target: r for r in check_invalid_values(S, fresh_db, None)}
    assert results["transactions.amount"].status == "fail"
    assert results["transactions.amount"].observed == "1 invalid rows"
    assert results["transactions.tenor_months"].status == "fail"


@pytest.mark.pg
def test_invalid_tenor_alone_leaves_the_amount_rule_passing(fresh_db):
    write(fresh_db, load("create.json", tenor_months=24))
    results = {r.target: r.status for r in check_invalid_values(S, fresh_db, None)}
    assert results == {"transactions.amount": "pass", "transactions.tenor_months": "fail"}


@pytest.mark.pg
def test_an_exact_redelivery_is_a_duplicate_not_a_regression(fresh_db):  # review I6
    create, update = (FIX / "create.json").read_bytes(), (FIX / "update.json").read_bytes()
    write(fresh_db, create, update, create)  # create re-delivered after the newer update
    assert by_target(check_raw_duplicate_lsn(S, fresh_db, None))[0].status == "warn"
    assert by_target(check_lsn_regression(S, fresh_db, None))[0].status == "pass"


@pytest.mark.pg
def test_an_unseen_older_version_is_a_regression_not_a_duplicate(fresh_db):
    create, update = (FIX / "create.json").read_bytes(), (FIX / "update.json").read_bytes()
    late = json.loads(update)
    late["source"]["lsn"] = late["source"]["lsn"] - 1  # older than the update, never seen
    write(fresh_db, create, update, json.dumps(late).encode())
    assert by_target(check_raw_duplicate_lsn(S, fresh_db, None))[0].status == "pass"
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
    stored = fresh_db.execute("select target, status from ops.dq_results order by 1").fetchall()
    alerts = fresh_db.execute("select check_type, target from ops.alerts").fetchall()
    assert stored == [("transactions.amount", "fail"), ("transactions.tenor_months", "pass")]
    assert alerts == [("invalid_values", "transactions.amount")]


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
    assert rows[0].target == "transactions.amount"
    assert rows[0].mttd_seconds is None  # a failure of another target is not this detection
    fresh_db.execute(
        "INSERT INTO ops.dq_results (check_type, target, status, observed, expected, run_at) "
        "SELECT 'invalid_values', 'transactions.amount', 'fail', '1', '0', "
        "injected_at + interval '130 s' FROM ops.fault_log"
    )
    assert compute_mttd(fresh_db)[0].mttd_seconds == pytest.approx(130, abs=1)


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
