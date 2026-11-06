"""Small helpers for the ops schema: fault log and chaos switches."""

from __future__ import annotations

import json
from typing import Any


def log_fault(conn: Any, fault_type: str, details: dict | None = None) -> int:
    row = conn.execute(
        "INSERT INTO ops.fault_log (fault_type, details) VALUES (%s, %s::jsonb) RETURNING fault_id",
        (fault_type, json.dumps(details or {}, default=str)),
    ).fetchone()
    return int(row[0])


def set_chaos(conn: Any, name: str, enabled: bool) -> None:
    conn.execute(
        "INSERT INTO ops.chaos (name, enabled) VALUES (%s, %s) "
        "ON CONFLICT (name) DO UPDATE SET enabled = EXCLUDED.enabled",
        (name, enabled),
    )


def chaos_enabled(conn: Any, name: str) -> bool:
    row = conn.execute("SELECT enabled FROM ops.chaos WHERE name = %s", (name,)).fetchone()
    return bool(row and row[0])
