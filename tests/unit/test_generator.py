from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from dateutil.relativedelta import relativedelta

from pdp.simulator.domain import PROVINCES, RISK_GRADES, TENORS
from pdp.simulator.generator import build_installments, generate_history

WIB = ZoneInfo("Asia/Jakarta")
NOW = datetime(2026, 11, 6, 12, 0, tzinfo=WIB)


def overdue_share(h, grade):
    accounts = {a.account_id for a in h.accounts if a.risk_grade == grade}
    cutoff = NOW.date() - timedelta(days=90)
    overdue = {
        i.account_id
        for i in h.installments
        if i.account_id in accounts and i.status == "unpaid" and i.due_date < cutoff
    }
    return len(overdue) / max(1, len(accounts))


def test_constants():
    assert TENORS == (1, 3, 6, 12) and set(RISK_GRADES) == {"A", "B", "C", "D", "E"}
    assert len(PROVINCES) >= 15 and abs(sum(w for _, w in PROVINCES) - 1) < 1e-9
    assert max(PROVINCES, key=lambda p: p[1])[0] == "Jawa Barat"


def test_deterministic():
    assert generate_history(42, 2, NOW, n_users=200) == generate_history(42, 2, NOW, n_users=200)
    assert generate_history(42, 2, NOW, n_users=200) != generate_history(43, 2, NOW, n_users=200)


def test_installments_sum_to_amount():
    h = generate_history(42, 3, NOW, n_users=300)
    by_txn = defaultdict(Decimal)
    for i in h.installments:
        by_txn[i.txn_id] += i.principal
    approved = [t for t in h.transactions if t.status == "approved"]
    assert approved and all(by_txn[t.txn_id] == t.amount for t in approved)
    assert all(t.txn_id not in by_txn for t in h.transactions if t.status != "approved")


def test_build_installments_split_and_due_dates():
    h = generate_history(42, 1, NOW, n_users=50)
    txn = next(t for t in h.transactions if t.status == "approved")
    inst = build_installments(txn)
    assert [i.seq for i in inst] == list(range(1, txn.tenor_months + 1))
    created = txn.created_at.astimezone(WIB).date()
    assert inst[0].due_date == created + relativedelta(months=1)
    assert all(i.interest == (txn.amount * txn.monthly_rate).quantize(Decimal("1")) for i in inst)


def test_tenor_mix_and_references():
    h = generate_history(42, 12, NOW, n_users=2000)
    share = Counter(t.tenor_months for t in h.transactions)
    assert abs(share[1] / len(h.transactions) - 0.4) < 0.03
    accounts = {a.account_id for a in h.accounts}
    merchants = {m.merchant_id for m in h.merchants}
    assert all(t.account_id in accounts and t.merchant_id in merchants for t in h.transactions)
    installments = {i.installment_id for i in h.installments}
    assert all(r.installment_id in installments for r in h.repayments)


def test_worse_grades_default_more():
    h = generate_history(42, 12, NOW, n_users=3000)
    assert overdue_share(h, "E") > 2 * overdue_share(h, "A")


def test_dates_within_window_and_no_future_payments():
    h = generate_history(42, 6, NOW, n_users=300)
    start = NOW - relativedelta(months=6)
    assert all(start <= t.created_at <= NOW for t in h.transactions)
    assert all(r.paid_at <= NOW for r in h.repayments)
    assert all((i.paid_at is None) == (i.status == "unpaid") for i in h.installments)


def test_amounts_respect_limits():
    h = generate_history(42, 6, NOW, n_users=500)
    limits = {a.account_id: a.credit_limit for a in h.accounts}
    assert all(Decimal("50000") <= t.amount <= limits[t.account_id] for t in h.transactions)
    assert all(t.amount % 1000 == 0 for t in h.transactions)
