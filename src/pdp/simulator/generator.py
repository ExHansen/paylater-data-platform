"""Deterministic PayLater history: users, accounts, merchants, transactions and repayments."""

from __future__ import annotations

import math
import random
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

from pdp.simulator.domain import (
    CHANNELS,
    CITIES,
    FIRST_NAMES,
    GRADE_WEIGHTS,
    LAST_NAMES,
    MERCHANT_CATEGORIES,
    PAYMENT_METHODS,
    PROVINCES,
    RISK_GRADES,
    TENOR_WEIGHTS,
    TENORS,
    CreditAccount,
    Installment,
    Merchant,
    Repayment,
    Transaction,
    User,
)

WIB = ZoneInfo("Asia/Jakarta")
RUPIAH = Decimal("1")
MIN_AMOUNT = Decimal("50000")
MIN_AVAILABLE = Decimal("100000")
MEDIAN_AMOUNT = 750_000
NAMESPACE = uuid.UUID("6f1d2c3a-0000-4000-8000-000000000d00")  # installment ids: uuid5(txn, seq)


@dataclass
class History:
    users: list[User]
    accounts: list[CreditAccount]
    merchants: list[Merchant]
    transactions: list[Transaction]
    installments: list[Installment]
    repayments: list[Repayment]


def _uuid(rng: random.Random) -> uuid.UUID:
    return uuid.UUID(int=rng.getrandbits(128), version=4)


def _digits(rng: random.Random, n: int) -> str:
    return "".join(str(rng.randint(0, 9)) for _ in range(n))


def _between(rng: random.Random, start: datetime, end: datetime) -> datetime:
    seconds = max(0.0, (end - start).total_seconds())
    return (start + timedelta(seconds=rng.uniform(0, seconds))).replace(microsecond=0)


def build_installments(txn: Transaction) -> list[Installment]:
    """Equal principal split (remainder on the last), flat monthly interest, monthly due dates."""
    tenor = txn.tenor_months
    base = (txn.amount / tenor).quantize(RUPIAH, rounding="ROUND_DOWN")
    interest = (txn.amount * txn.monthly_rate).quantize(RUPIAH)
    created = txn.created_at.astimezone(WIB).date()
    schedule = []
    for seq in range(1, tenor + 1):
        principal = base if seq < tenor else txn.amount - base * (tenor - 1)
        schedule.append(
            Installment(
                installment_id=uuid.uuid5(NAMESPACE, f"{txn.txn_id}:{seq}"),
                txn_id=txn.txn_id,
                account_id=txn.account_id,
                seq=seq,
                due_date=created + relativedelta(months=seq),
                principal=principal,
                interest=interest,
                status="unpaid",
                paid_at=None,
                updated_at=txn.created_at,
            )
        )
    return schedule


def _payment_time(due, delay_days: int, rng: random.Random) -> datetime:
    day = due + timedelta(days=delay_days)
    clock = time(rng.randint(7, 22), rng.randint(0, 59))
    return datetime.combine(day, clock, tzinfo=WIB)


def generate_history(
    seed: int,
    months: int,
    now: datetime,
    n_users: int = 5000,
    n_merchants: int = 300,
    txns_per_user_month: float = 1.0,
) -> History:
    rng = random.Random(f"{seed}:history")
    start = now - relativedelta(months=months)
    provinces = [p for p, _ in PROVINCES]
    weights = [w for _, w in PROVINCES]

    merchants = []
    for _ in range(n_merchants):
        province = rng.choices(provinces, weights)[0]
        category = rng.choice(MERCHANT_CATEGORIES)
        city = rng.choice(CITIES[province])
        merchants.append(
            Merchant(
                merchant_id=_uuid(rng),
                name=f"{category.replace('_', ' ').title()} {city} {rng.randint(1, 99)}",
                category=category,
                province=province,
                created_at=start - timedelta(days=rng.randint(30, 365)),
            )
        )

    users, accounts, transactions, installments, repayments = [], [], [], [], []
    for _ in range(n_users):
        province = rng.choices(provinces, weights)[0]
        created = _between(rng, start, now - timedelta(days=1))
        user = User(
            user_id=_uuid(rng),
            full_name=f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
            nik=_digits(rng, 16),
            phone="08" + _digits(rng, 10),
            province=province,
            city=rng.choice(CITIES[province]),
            kyc_status=rng.choices(("verified", "pending", "rejected"), (0.92, 0.05, 0.03))[0],
            created_at=created,
            updated_at=created,
        )
        users.append(user)

        grade_name = rng.choices(list(RISK_GRADES), GRADE_WEIGHTS)[0]
        grade = RISK_GRADES[grade_name]
        low, high = grade.limit_range
        limit = Decimal(round(rng.randint(low, high) / 500_000) * 500_000)
        status = "active" if user.kyc_status == "verified" else "frozen"
        if status == "active" and rng.random() < 0.02:
            status = "closed"
        account = CreditAccount(
            account_id=_uuid(rng),
            user_id=user.user_id,
            credit_limit=limit,
            status=status,
            risk_grade=grade_name,
            created_at=created,
            updated_at=created,
        )
        accounts.append(account)
        if status != "active":
            continue

        roll = rng.random()
        if roll < grade.default_prob:
            behaviour = "default"
        elif roll < grade.default_prob + grade.late_prob:
            behaviour = "late"
        else:
            behaviour = "on_time"
        stop_paying = _between(rng, created, now) if behaviour == "default" else None

        rate_per_day = txns_per_user_month * 12 / 365
        moment = created
        open_installments: list[Installment] = []
        while True:
            moment = moment + timedelta(days=rng.expovariate(rate_per_day))
            if moment > now:
                break
            moment = moment.replace(microsecond=0)
            outstanding = sum(
                (i.principal for i in open_installments if i.paid_at is None or i.paid_at > moment),
                Decimal("0"),
            )
            available = limit - outstanding
            if available < MIN_AVAILABLE:
                continue
            raw = math.exp(rng.gauss(math.log(MEDIAN_AMOUNT), 0.8))
            amount = max(MIN_AMOUNT, Decimal(round(raw / 1000) * 1000))
            amount = min(amount, (available / 1000).quantize(RUPIAH, "ROUND_DOWN") * 1000)
            if amount < MIN_AMOUNT:
                continue
            txn_status = rng.choices(("approved", "declined", "refunded"), (0.97, 0.02, 0.01))[0]
            txn = Transaction(
                txn_id=_uuid(rng),
                account_id=account.account_id,
                merchant_id=rng.choice(merchants).merchant_id,
                amount=amount,
                tenor_months=rng.choices(TENORS, TENOR_WEIGHTS)[0],
                monthly_rate=grade.monthly_rate,
                status=txn_status,
                channel=rng.choice(CHANNELS),
                device_id=f"dev-{account.account_id.hex[:8]}-{rng.randint(1, 2)}",
                created_at=moment,
                updated_at=moment,
            )
            transactions.append(txn)
            if txn_status != "approved":
                continue
            for inst in build_installments(txn):
                if behaviour == "on_time":
                    paid_at = _payment_time(inst.due_date, -rng.randint(0, 5), rng)
                elif behaviour == "late":
                    paid_at = _payment_time(inst.due_date, rng.randint(1, 90), rng)
                else:
                    due_moment = datetime.combine(inst.due_date, time(12), tzinfo=WIB)
                    paid_at = (
                        _payment_time(inst.due_date, -rng.randint(0, 3), rng)
                        if due_moment < stop_paying
                        else None
                    )
                if paid_at is not None and paid_at > now:
                    paid_at = None
                if paid_at is not None:
                    paid_at = max(paid_at, txn.created_at)
                    inst = replace(inst, status="paid", paid_at=paid_at, updated_at=paid_at)
                    repayments.append(
                        Repayment(
                            repayment_id=_uuid(rng),
                            installment_id=inst.installment_id,
                            account_id=account.account_id,
                            amount=inst.principal + inst.interest,
                            method=rng.choice(PAYMENT_METHODS),
                            paid_at=paid_at,
                        )
                    )
                open_installments.append(inst)
                installments.append(inst)

    return History(users, accounts, merchants, transactions, installments, repayments)
