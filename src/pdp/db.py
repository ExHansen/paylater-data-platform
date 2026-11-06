"""Postgres connections and helpers."""

from __future__ import annotations

from pathlib import Path

import psycopg


def connect(dsn: str, autocommit: bool = False) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=autocommit)


def strip_psql_meta(sql: str) -> str:
    r"""Drop psql meta-commands (lines starting with a backslash, e.g. `\connect`)."""
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("\\"))


def apply_sql_file(conn: psycopg.Connection, path: Path) -> None:
    """Run a DDL file on the current connection (used by tests and setup tooling)."""
    conn.execute(strip_psql_meta(Path(path).read_text(encoding="utf-8")))
