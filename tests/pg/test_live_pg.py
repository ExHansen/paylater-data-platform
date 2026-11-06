from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from pdp.config import Settings
from pdp.simulator.generator import generate_history
from pdp.simulator.live import LiveSimulator
from pdp.simulator.seed import write_history

WIB = ZoneInfo("Asia/Jakarta")


@pytest.mark.pg
def test_live_repays_only_installments_falling_due_now(fresh_db):  # review I1
    write_history(fresh_db, generate_history(42, 6, datetime.now(WIB), n_users=200))
    _, due, _, _ = LiveSimulator(Settings(_env_file=None), rate=1.0)._load(fresh_db)
    today = datetime.now(WIB).date()
    due_dates = fresh_db.execute(
        "SELECT due_date FROM installments WHERE installment_id = ANY(%s)",
        ([d.installment_id for d in due],),
    ).fetchall()
    assert due_dates
    assert all(today - timedelta(days=3) <= d <= today + timedelta(days=3) for (d,) in due_dates)
    # arrears from the history (late payers, defaulters) stay unpaid, so delinquency survives
    arrears = fresh_db.execute(
        "SELECT count(*) FROM installments WHERE status = 'unpaid' AND due_date < %s",
        (today - timedelta(days=3),),
    ).fetchone()[0]
    assert arrears > 0


@pytest.mark.pg
def test_load_computes_available_limit_per_account(fresh_db):  # review M5: one GROUP BY pass
    write_history(fresh_db, generate_history(42, 3, datetime.now(WIB), n_users=80))
    accounts, _, _, _ = LiveSimulator(Settings(_env_file=None), rate=1.0)._load(fresh_db)
    expected = dict(
        fresh_db.execute(
            "SELECT a.account_id, a.credit_limit - coalesce(sum(i.principal), 0) "
            "FROM credit_accounts a LEFT JOIN installments i "
            "ON i.account_id = a.account_id AND i.status = 'unpaid' "
            "WHERE a.status = 'active' GROUP BY a.account_id, a.credit_limit"
        ).fetchall()
    )
    assert {a.account_id: a.available for a in accounts} == expected
    assert all(a.devices for a in accounts if a.monthly_rate != Decimal("0.03"))
