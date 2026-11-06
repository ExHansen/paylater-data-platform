"""Account exposure (open principal) from CDC events, kept in online.account_exposure."""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID

from pdp.loader.envelope import CdcEvent


def exposure_delta(event: CdcEvent) -> tuple[UUID, Decimal] | None:
    """How one change moves an account's exposure, or None if it does not.

    Purchases (approved, created or snapshotted) add their amount; refunds remove it;
    installments that become paid (or are snapshotted as paid) remove their principal.
    Only the named fields are read, so unknown columns never matter.
    """
    after = event.after or {}
    before = event.before or {}
    if event.table == "transactions":
        if event.op in ("c", "r") and after.get("status") == "approved":
            return UUID(after["account_id"]), Decimal(str(after["amount"]))
        if (
            event.op == "u"
            and before.get("status") == "approved"
            and after.get("status") == "refunded"
        ):
            return UUID(after["account_id"]), -Decimal(str(after["amount"]))
    if event.table == "installments" and after.get("status") == "paid":
        # created/snapshotted already paid (seeded history), or paid by an update
        if event.op in ("c", "r") or (event.op == "u" and before.get("status") == "unpaid"):
            return UUID(after["account_id"]), -Decimal(str(after["principal"]))
    return None


def apply_delta(conn, account_id: UUID, delta: Decimal) -> None:
    conn.execute(
        "INSERT INTO online.account_exposure (account_id, exposure) VALUES (%s, %s) "
        "ON CONFLICT (account_id) DO UPDATE "
        "SET exposure = online.account_exposure.exposure + EXCLUDED.exposure, updated_at = now()",
        (account_id, delta),
    )
