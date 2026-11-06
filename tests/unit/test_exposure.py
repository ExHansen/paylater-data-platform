from decimal import Decimal
from uuid import UUID

from pdp.loader.envelope import CdcEvent
from pdp.stream.exposure import exposure_delta

ACCT = "6d8f4b3c-2222-4aaa-8bbb-000000000001"


def ev_txn(op, status, amount, before_status=None, extra=None):
    after = {
        "txn_id": "t1",
        "account_id": ACCT,
        "amount": amount,
        "status": status,
        **(extra or {}),
    }
    before = None if before_status is None else {**after, "status": before_status}
    return CdcEvent("transactions", op, {"txn_id": "t1"}, before, after, 1, 1)


def ev_inst(op, before_status, after_status, principal="100000"):
    after = {
        "installment_id": "i1",
        "account_id": ACCT,
        "principal": principal,
        "status": after_status,
    }
    before = None if before_status is None else {**after, "status": before_status}
    return CdcEvent("installments", op, {"installment_id": "i1"}, before, after, 1, 1)


def test_exposure_deltas():
    assert exposure_delta(ev_txn("c", "approved", "300000")) == (UUID(ACCT), Decimal("300000"))
    assert exposure_delta(ev_txn("r", "approved", "5"))[1] == Decimal("5")
    assert exposure_delta(ev_txn("u", "refunded", "300000", before_status="approved"))[
        1
    ] == Decimal("-300000")
    assert exposure_delta(ev_inst("u", "unpaid", "paid"))[1] == Decimal("-100000")
    assert exposure_delta(ev_inst("r", None, "paid"))[1] == Decimal("-100000")


def test_no_delta_cases():
    assert exposure_delta(ev_txn("c", "declined", "1")) is None
    assert exposure_delta(ev_inst("r", None, "unpaid")) is None
    assert exposure_delta(ev_inst("u", "paid", "paid")) is None
    assert exposure_delta(ev_txn("d", "approved", "1")) is None


def test_stream_ignores_unknown_fields():  # Review Focus 5
    event = ev_txn("c", "approved", "1", extra={"promo_code": "X"})
    assert exposure_delta(event)[1] == Decimal("1")
