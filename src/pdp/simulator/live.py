"""Live PayLater traffic: new purchases, repayments and limit changes, one tick per second."""

from __future__ import annotations

import math
import random
import time as clock
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pdp.config import Settings
from pdp.db import connect
from pdp.simulator.domain import CHANNELS, PAYMENT_METHODS, TENOR_WEIGHTS, TENORS, Transaction
from pdp.simulator.generator import build_installments

WIB = ZoneInfo("Asia/Jakarta")
MIN_AMOUNT = Decimal("50000")
NAUGHTY_SHARE = 0.03
REPAY_PROB = 0.2
LIMIT_CHANGE_PROB = 0.001


@dataclass(frozen=True)
class AccountView:
    account_id: UUID
    credit_limit: Decimal
    available: Decimal
    monthly_rate: Decimal
    devices: tuple[str, ...]


@dataclass(frozen=True)
class InstallmentView:
    installment_id: UUID
    account_id: UUID
    principal: Decimal
    interest: Decimal


@dataclass(frozen=True)
class Action:
    kind: Literal["txn", "repay", "limit_change"]
    account_id: UUID
    payload: dict = field(default_factory=dict)


@dataclass
class LiveStats:
    txns: int = 0
    repayments: int = 0
    limit_changes: int = 0


def poisson(rng: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, round(rng.gauss(lam, math.sqrt(lam))))
    threshold, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= threshold:
            return k
        k += 1


def _round_thousand(value: float | Decimal) -> Decimal:
    return Decimal(int(Decimal(value) / 1000) * 1000)


def _txn(rng: random.Random, account: AccountView, amount: Decimal, device: str) -> Action:
    return Action(
        "txn",
        account.account_id,
        {
            "amount": amount,
            "tenor_months": rng.choices(TENORS, TENOR_WEIGHTS)[0],
            "monthly_rate": account.monthly_rate,
            "channel": rng.choice(CHANNELS),
            "device_id": device,
            "merchant_index": rng.randrange(1_000_000),
        },
    )


def plan_tick(
    rng: random.Random,
    rate: float,
    dt_s: float,
    accounts: Sequence[AccountView],
    due: Sequence[InstallmentView],
    naughty: set[UUID],
) -> list[Action]:
    """Decide what happens during one tick. Pure: no I/O, all randomness from `rng`."""
    actions: list[Action] = []
    regular = [a for a in accounts if a.account_id not in naughty and a.available >= MIN_AMOUNT]
    bad = sorted((a for a in accounts if a.account_id in naughty), key=lambda a: a.account_id)
    bursted: set[UUID] = set()
    for _ in range(poisson(rng, rate * dt_s)):
        if bad and rng.random() < NAUGHTY_SHARE:
            account = rng.choice(bad)
            if account.account_id in bursted or account.available < MIN_AMOUNT:
                continue
            bursted.add(account.account_id)
            device = f"dev-new-{rng.getrandbits(32):08x}"
            for _ in range(rng.randint(5, 8)):
                share = Decimal(str(rng.uniform(0.8, 1.0)))
                amount = max(MIN_AMOUNT, _round_thousand(account.available * share))
                actions.append(_txn(rng, account, min(amount, account.available), device))
            continue
        if not regular:
            continue
        account = rng.choice(regular)
        raw = math.exp(rng.gauss(math.log(750_000), 0.8))
        amount = min(max(MIN_AMOUNT, _round_thousand(raw)), _round_thousand(account.available))
        if amount < MIN_AMOUNT:
            continue
        device = (
            rng.choice(account.devices) if account.devices else f"dev-{account.account_id.hex[:8]}"
        )
        actions.append(_txn(rng, account, amount, device))
    for inst in due:
        if rng.random() < REPAY_PROB:
            actions.append(
                Action(
                    "repay",
                    inst.account_id,
                    {
                        "installment_id": inst.installment_id,
                        "amount": inst.principal + inst.interest,
                        "method": rng.choice(PAYMENT_METHODS),
                    },
                )
            )
    if accounts and rng.random() < LIMIT_CHANGE_PROB:
        account = rng.choice(accounts)
        step = Decimal(500_000) * rng.choice((-1, 1, 2))
        actions.append(
            Action(
                "limit_change",
                account.account_id,
                {"credit_limit": max(Decimal(1_000_000), account.credit_limit + step)},
            )
        )
    return actions


def apply_actions(
    conn, actions: Sequence[Action], now: datetime, merchants: Sequence[UUID]
) -> LiveStats:
    """Write one tick's actions inside the caller's transaction."""
    stats = LiveStats()
    rng = random.Random(now.timestamp())
    for action in actions:
        if action.kind == "txn":
            p = action.payload
            txn = Transaction(
                txn_id=UUID(int=rng.getrandbits(128), version=4),
                account_id=action.account_id,
                merchant_id=merchants[p["merchant_index"] % len(merchants)],
                amount=p["amount"],
                tenor_months=p["tenor_months"],
                monthly_rate=p["monthly_rate"],
                status="approved",
                channel=p["channel"],
                device_id=p["device_id"],
                created_at=now,
                updated_at=now,
            )
            conn.execute(
                "INSERT INTO transactions (txn_id, account_id, merchant_id, amount, tenor_months, "
                "monthly_rate, status, channel, device_id, created_at, updated_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (txn.txn_id, txn.account_id, txn.merchant_id, txn.amount, txn.tenor_months,
                 txn.monthly_rate, txn.status, txn.channel, txn.device_id, now, now),
            )  # fmt: skip
            for inst in build_installments(txn):
                conn.execute(
                    "INSERT INTO installments (installment_id, txn_id, account_id, seq, due_date, "
                    "principal, interest, status, paid_at, updated_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (inst.installment_id, inst.txn_id, inst.account_id, inst.seq, inst.due_date,
                     inst.principal, inst.interest, inst.status, None, now),
                )  # fmt: skip
            stats.txns += 1
        elif action.kind == "repay":
            p = action.payload
            updated = conn.execute(
                "UPDATE installments SET status = 'paid', paid_at = %s, updated_at = %s "
                "WHERE installment_id = %s AND status = 'unpaid'",
                (now, now, p["installment_id"]),
            ).rowcount
            if updated:
                conn.execute(
                    "INSERT INTO repayments (repayment_id, installment_id, account_id, amount, "
                    "method, paid_at) VALUES (%s,%s,%s,%s,%s,%s)",
                    (UUID(int=rng.getrandbits(128), version=4), p["installment_id"],
                     action.account_id, p["amount"], p["method"], now),
                )  # fmt: skip
                stats.repayments += 1
        else:
            conn.execute(
                "UPDATE credit_accounts SET credit_limit = %s, updated_at = %s "
                "WHERE account_id = %s",
                (action.payload["credit_limit"], now, action.account_id),
            )
            stats.limit_changes += 1
    return stats


class LiveSimulator:
    """Drives `plan_tick` against the application database once per second."""

    REFRESH_EVERY_S = 60

    def __init__(self, settings: Settings, rate: float):
        self.settings = settings
        self.rate = rate
        self.rng = random.Random(f"{settings.seed}:live")

    def _load(self, conn):
        accounts = [
            AccountView(r[0], r[1], r[1] - r[2], r[3], tuple(r[4] or ()))
            for r in conn.execute(
                """
                SELECT a.account_id, a.credit_limit,
                       coalesce((SELECT sum(i.principal) FROM installments i
                                 WHERE i.account_id = a.account_id AND i.status = 'unpaid'), 0),
                       coalesce((SELECT max(t.monthly_rate) FROM transactions t
                                 WHERE t.account_id = a.account_id), 0.03),
                       (SELECT array_agg(DISTINCT t.device_id) FROM transactions t
                        WHERE t.account_id = a.account_id)
                FROM credit_accounts a WHERE a.status = 'active'
                """
            ).fetchall()
        ]
        today = datetime.now(WIB).date()
        due = [
            InstallmentView(*r)
            for r in conn.execute(
                "SELECT installment_id, account_id, principal, interest FROM installments "
                "WHERE status = 'unpaid' AND due_date <= %s ORDER BY due_date LIMIT 5000",
                (today + timedelta(days=3),),
            ).fetchall()
        ]
        merchants = [r[0] for r in conn.execute("SELECT merchant_id FROM merchants").fetchall()]
        ids = sorted(a.account_id for a in accounts)
        naughty = set(self.rng.sample(ids, k=max(1, len(ids) // 50))) if ids else set()
        return accounts, due, merchants, naughty

    def run(self, duration: timedelta | None = None) -> LiveStats:
        total = LiveStats()
        deadline = None if duration is None else clock.monotonic() + duration.total_seconds()
        with connect(self.settings.app_dsn) as conn:
            accounts, due, merchants, naughty = self._load(conn)
            loaded_at = clock.monotonic()
            while deadline is None or clock.monotonic() < deadline:
                started = clock.monotonic()
                actions = plan_tick(self.rng, self.rate, 1.0, accounts, due, naughty)
                stats = apply_actions(conn, actions, datetime.now(WIB), merchants)
                conn.commit()
                total.txns += stats.txns
                total.repayments += stats.repayments
                total.limit_changes += stats.limit_changes
                spent = {a.account_id: Decimal(0) for a in accounts}
                for a in actions:
                    if a.kind == "txn":
                        spent[a.account_id] += a.payload["amount"]
                accounts = [
                    replace(a, available=a.available - spent[a.account_id]) for a in accounts
                ]
                repaid = {a.payload["installment_id"] for a in actions if a.kind == "repay"}
                due = [d for d in due if d.installment_id not in repaid]
                if clock.monotonic() - loaded_at > self.REFRESH_EVERY_S:
                    accounts, due, merchants, naughty = self._load(conn)
                    loaded_at = clock.monotonic()
                clock.sleep(max(0.0, 1.0 - (clock.monotonic() - started)))
        return total
