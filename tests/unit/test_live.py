import random
import statistics
from decimal import Decimal
from uuid import UUID

from pdp.simulator.live import AccountView, InstallmentView, plan_tick, poisson


def view(i: int, limit: str = "5000000", available: str = "4000000") -> AccountView:
    return AccountView(
        account_id=UUID(int=i + 1),
        credit_limit=Decimal(limit),
        available=Decimal(available),
        monthly_rate=Decimal("0.03"),
        devices=(f"dev-{i}",),
    )


ACCTS = [view(i) for i in range(500)]


def test_poisson_mean():
    rng = random.Random(0)
    assert 4.85 < statistics.mean(poisson(rng, 5.0) for _ in range(5000)) < 5.15


def test_tick_rate_matches_poisson_mean():
    rng = random.Random(1)
    counts = [
        sum(a.kind == "txn" for a in plan_tick(rng, 5.0, 1.0, ACCTS, [], set()))
        for _ in range(2000)
    ]
    assert 4.8 < statistics.mean(counts) < 5.2


def test_txn_payload_respects_available_limit():
    acts = plan_tick(random.Random(2), 50.0, 1.0, ACCTS, [], set())
    by_id = {a.account_id: a for a in ACCTS}
    for a in acts:
        assert Decimal("50000") <= a.payload["amount"] <= by_id[a.account_id].available
        assert a.payload["tenor_months"] in (1, 3, 6, 12)


def test_naughty_bursts_are_5_to_8_with_one_new_device():
    naughty = {ACCTS[0].account_id}
    for seed in range(200):
        acts = plan_tick(random.Random(seed), 50.0, 1.0, ACCTS, [], naughty)
        burst = [a for a in acts if a.account_id == ACCTS[0].account_id and a.kind == "txn"]
        assert len(burst) == 0 or 5 <= len(burst) <= 8
        if burst:
            assert len({a.payload["device_id"] for a in burst}) == 1
            assert burst[0].payload["device_id"] not in ACCTS[0].devices
            assert all(
                a.payload["amount"] >= Decimal("0.8") * ACCTS[0].available - 1000 for a in burst
            )


def test_due_installments_get_repaid_sometimes():
    due = [
        InstallmentView(UUID(int=10_000 + i), ACCTS[i].account_id, Decimal("1000"), Decimal("30"))
        for i in range(100)
    ]
    acts = plan_tick(random.Random(5), 0.0, 1.0, ACCTS, due, set())
    repays = [a for a in acts if a.kind == "repay"]
    assert 5 < len(repays) < 40
    assert all(a.payload["amount"] == Decimal("1030") for a in repays)
