"""Write a generated history into the application database with COPY."""

from __future__ import annotations

from dataclasses import astuple, fields
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg

from pdp.config import Settings
from pdp.db import connect
from pdp.simulator.generator import History, generate_history

WIB = ZoneInfo("Asia/Jakarta")

# History attribute -> table, in foreign-key order.
TABLE_ORDER = (
    ("users", "users"),
    ("accounts", "credit_accounts"),
    ("merchants", "merchants"),
    ("transactions", "transactions"),
    ("installments", "installments"),
    ("repayments", "repayments"),
)


class AlreadySeeded(RuntimeError):
    """The application database already holds data."""


def write_history(conn: psycopg.Connection, history: History) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for attr, table in TABLE_ORDER:
            rows = getattr(history, attr)
            if not rows:
                counts[table] = 0
                continue
            columns = ", ".join(f.name for f in fields(rows[0]))
            with cur.copy(f"COPY {table} ({columns}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(astuple(row))
            counts[table] = len(rows)
    return counts


def seed_connection(
    conn: psycopg.Connection,
    months: int,
    seed: int,
    now: datetime | None = None,
    n_users: int = 5000,
) -> dict[str, int]:
    if conn.execute("SELECT EXISTS (SELECT 1 FROM users)").fetchone()[0]:
        raise AlreadySeeded("the application database is already seeded; run `pdp down --volumes`")
    history = generate_history(seed, months, now or datetime.now(WIB), n_users=n_users)
    return write_history(conn, history)


def seed(settings: Settings, months: int, seed: int, now: datetime | None = None) -> dict[str, int]:
    with connect(settings.app_dsn) as conn:
        counts = seed_connection(conn, months, seed, now)
        conn.commit()
    return counts
