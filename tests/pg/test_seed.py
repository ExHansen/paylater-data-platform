from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from pdp.db import apply_sql_file
from pdp.simulator.generator import generate_history
from pdp.simulator.seed import AlreadySeeded, seed_connection, write_history

NOW = datetime(2026, 11, 6, 12, 0, tzinfo=ZoneInfo("Asia/Jakarta"))


@pytest.fixture
def app_db(pg_conn):
    apply_sql_file(pg_conn, Path("infra/postgres/10_app.sql"))
    return pg_conn


@pytest.mark.pg
def test_write_history_counts(app_db):
    history = generate_history(42, 1, NOW, n_users=50)
    counts = write_history(app_db, history)
    assert counts["users"] == 50
    assert counts["installments"] == len(history.installments)
    assert (
        app_db.execute("select count(*) from installments").fetchone()[0] == counts["installments"]
    )


@pytest.mark.pg
def test_seed_refuses_a_seeded_database(app_db):
    seed_connection(app_db, months=1, seed=42, now=NOW, n_users=20)
    with pytest.raises(AlreadySeeded):
        seed_connection(app_db, months=1, seed=42, now=NOW, n_users=20)
