"""Shared pytest setup: `pg` tests run only when PDP_TEST_PG_DSN points at a Postgres database."""

from __future__ import annotations

import os

import pytest

PG_DSN_ENV = "PDP_TEST_PG_DSN"


def pytest_collection_modifyitems(config, items):
    if os.environ.get(PG_DSN_ENV):
        return
    skip_pg = pytest.mark.skip(reason=f"{PG_DSN_ENV} not set (pg tests run in CI)")
    for item in items:
        if "pg" in item.keywords:
            item.add_marker(skip_pg)


@pytest.fixture
def pg_conn():
    """A connection to the test database inside a transaction that is always rolled back."""
    import psycopg

    conn = psycopg.connect(os.environ[PG_DSN_ENV])
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()


APP_DDL = "infra/postgres/10_app.sql"
WH_DDL = "infra/postgres/20_warehouse.sql"


@pytest.fixture
def fresh_db():
    """Empty app + warehouse schemas in the test database, committed.

    For code that commits on purpose (fault injection, loader): every test starts clean.
    """
    from pathlib import Path

    import psycopg

    from pdp.db import apply_sql_file

    admin = psycopg.connect(os.environ[PG_DSN_ENV], autocommit=True)
    admin.execute("DROP PUBLICATION IF EXISTS pdp_pub")
    admin.execute(
        "DROP SCHEMA IF EXISTS public, raw, staging, intermediate, marts, online, ops CASCADE"
    )
    admin.execute("CREATE SCHEMA public")
    admin.close()
    conn = psycopg.connect(os.environ[PG_DSN_ENV])
    apply_sql_file(conn, Path(APP_DDL))
    apply_sql_file(conn, Path(WH_DDL))
    conn.commit()
    try:
        yield conn
    finally:
        conn.rollback()
        conn.close()
