import pytest


@pytest.mark.pg
def test_pg_marker_runs_only_with_dsn(pg_conn):
    assert pg_conn.execute("select 1").fetchone() == (1,)
