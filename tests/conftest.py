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
