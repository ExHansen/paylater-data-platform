from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from pdp.db import apply_sql_file
from pdp.ops import chaos_enabled
from pdp.simulator.faults import FaultContext, inject
from pdp.simulator.generator import generate_history
from pdp.simulator.seed import write_history

NOW = datetime(2026, 11, 6, 12, 0, tzinfo=ZoneInfo("Asia/Jakarta"))


@pytest.fixture
def db(pg_conn):
    apply_sql_file(pg_conn, Path("infra/postgres/10_app.sql"))
    apply_sql_file(pg_conn, Path("infra/postgres/20_warehouse.sql"))
    write_history(pg_conn, generate_history(42, 1, NOW, n_users=30))
    return pg_conn


def ctx(conn):
    return FaultContext(settings=None, app_conn=conn, wh_conn=conn, compose=None)


@pytest.mark.pg
def test_invalid_amount_inserts_bad_rows_and_logs(db):
    inject("invalid_amount", ctx(db))
    assert db.execute("select count(*) from transactions where amount <= 0").fetchone()[0] == 3
    assert db.execute("select fault_type from ops.fault_log").fetchall() == [("invalid_amount",)]


@pytest.mark.pg
def test_invalid_tenor_and_schema_drift(db):
    inject("invalid_tenor", ctx(db))
    inject("schema_drift", ctx(db))
    assert (
        db.execute("select count(*) from transactions where tenor_months = 24").fetchone()[0] == 2
    )
    assert (
        db.execute("select count(*) from transactions where promo_code is not null").fetchone()[0]
        == 5
    )


@pytest.mark.pg
def test_reconciliation_gap_flips_chaos_flag(db):
    inject("reconciliation_gap", ctx(db))
    assert chaos_enabled(db, "drop_next_batch")
