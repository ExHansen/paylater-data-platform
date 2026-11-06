"""Real-time risk rules over the per-account features (pure)."""

from __future__ import annotations

from decimal import Decimal

from pdp.stream.features import Features

VELOCITY_10M = 5
NEAR_LIMIT_SHARE = Decimal("0.8")
NEW_DEVICE_AMOUNT = Decimal("2000000")
MAX_UTILIZATION = 0.95


def utilization(exposure: Decimal | None, credit_limit: Decimal | None) -> float | None:
    if exposure is None or credit_limit is None or credit_limit <= 0:
        return None
    return float(exposure / credit_limit)


def evaluate(
    features: Features,
    amount: Decimal,
    credit_limit: Decimal | None,
    exposure: Decimal | None,
) -> list[str]:
    """Rule ids that fire for one purchase. Limit-based rules need a known limit."""
    fired = []
    if features.txn_count_10m >= VELOCITY_10M:
        fired.append("RR1_velocity")
    if credit_limit is not None and amount > NEAR_LIMIT_SHARE * credit_limit:
        fired.append("RR2_near_limit")
    if features.is_new_device and amount > NEW_DEVICE_AMOUNT:
        fired.append("RR3_new_device_high")
    ratio = utilization(exposure, credit_limit)
    if ratio is not None and ratio > MAX_UTILIZATION:
        fired.append("RR4_utilization")
    return fired
