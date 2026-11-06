"""Per-account sliding windows: transaction velocity and device history (pure, no Kafka)."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal

WINDOW_10M_MS = 10 * 60 * 1000
WINDOW_1H_MS = 60 * 60 * 1000
DEVICE_TTL_MS = 30 * 24 * 60 * 60 * 1000


@dataclass
class AccountWindow:
    events: deque = field(default_factory=deque)  # (ts_ms, amount, txn_id), oldest first
    devices: dict = field(default_factory=dict)  # device_id -> last seen ts_ms


@dataclass(frozen=True)
class Features:
    txn_count_10m: int
    txn_amount_10m: Decimal
    txn_count_1h: int
    txn_amount_1h: Decimal
    is_new_device: bool


def update(
    state: AccountWindow, ts_ms: int, amount: Decimal, device_id: str, txn_id: str | None = None
) -> Features:
    """Add one approved purchase and return the features as of that purchase.

    Windows include events at most exactly 10 minutes / 1 hour older than `ts_ms`.
    """
    while state.events and state.events[0][0] < ts_ms - WINDOW_1H_MS:
        state.events.popleft()
    for device, seen in list(state.devices.items()):
        if seen < ts_ms - DEVICE_TTL_MS:
            del state.devices[device]
    is_new_device = device_id not in state.devices
    state.devices[device_id] = ts_ms
    state.events.append((ts_ms, amount, txn_id))
    recent = [a for t, a, _ in state.events if t >= ts_ms - WINDOW_10M_MS]
    return Features(
        txn_count_10m=len(recent),
        txn_amount_10m=sum(recent, Decimal(0)),
        txn_count_1h=len(state.events),
        txn_amount_1h=sum((a for _, a, _ in state.events), Decimal(0)),
        is_new_device=is_new_device,
    )


def to_state(window: AccountWindow) -> dict:
    """JSON-serialisable form for the stream processor's state store."""
    return {
        "events": [[t, str(a), txn_id] for t, a, txn_id in window.events],
        "devices": dict(window.devices),
    }


def from_state(data: dict | None) -> AccountWindow:
    if not data:
        return AccountWindow()
    return AccountWindow(
        # entries saved before txn ids were kept have two fields
        events=deque(
            (int(e[0]), Decimal(e[1]), e[2] if len(e) > 2 else None) for e in data.get("events", [])
        ),
        devices={k: int(v) for k, v in data.get("devices", {}).items()},
    )


def has_seen(state: AccountWindow, txn_id: str) -> bool:
    """True if this purchase is already in the window (a redelivered event)."""
    return any(seen == txn_id for _, _, seen in state.events)
