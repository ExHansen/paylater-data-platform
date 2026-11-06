"""Mean time to detect: from fault injection (ops.fault_log) to the first matching check result."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from pdp.simulator.faults import FAULT_CHECK, FAULT_TARGET


@dataclass(frozen=True)
class MttdRow:
    fault_id: int
    fault_type: str
    check_type: str
    injected_at: datetime
    detected_at: datetime | None
    mttd_seconds: float | None
    target: str | None = None  # set when the check reports one result per rule


def compute_mttd(conn) -> list[MttdRow]:
    rows = []
    faults = conn.execute(
        "SELECT fault_id, fault_type, injected_at FROM ops.fault_log ORDER BY fault_id"
    ).fetchall()
    for fault_id, fault_type, injected_at in faults:
        check_type = FAULT_CHECK.get(fault_type)
        target = FAULT_TARGET.get(fault_type)
        detected_at = None
        if check_type is not None:
            detected_at = conn.execute(
                "SELECT min(run_at) FROM ops.dq_results WHERE check_type = %s "
                "AND (%s::text IS NULL OR target = %s) "
                "AND status IN ('fail', 'warn') AND run_at >= %s",
                (check_type, target, target, injected_at),
            ).fetchone()[0]
        seconds = None if detected_at is None else (detected_at - injected_at).total_seconds()
        rows.append(
            MttdRow(
                fault_id, fault_type, check_type or "-", injected_at, detected_at, seconds, target
            )
        )
    return rows
