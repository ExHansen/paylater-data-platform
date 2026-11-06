from decimal import Decimal

import pytest

from tests.dbt.fixture_builder import AS_OF, EXPECTED_TWP90, uid


def one(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


@pytest.mark.pg
def test_twp90_matches_hand_calculation(dbt_built):
    # T1 (DPD 123): 3,000,000 and T3 (DPD 93): 4,000,000 are 90+; T2 800,000 and T4 500,000 current.
    twp90 = one(
        dbt_built,
        "select round(sum(dpd_90_plus) / sum(total_outstanding), 4) from marts.mart_delinquency "
        "where as_of_date = %s",
        (AS_OF,),
    )
    assert twp90 == EXPECTED_TWP90


@pytest.mark.pg
def test_delinquency_buckets_by_province(dbt_built):
    rows = dbt_built.execute(
        "select province, total_outstanding, current_amount, dpd_90_plus "
        "from marts.mart_delinquency "
        "where as_of_date = %s order by province",
        (AS_OF,),
    ).fetchall()
    assert rows == [
        ("Bali", Decimal("4000000.00"), Decimal("0.00"), Decimal("4000000.00")),
        ("DKI Jakarta", Decimal("1300000.00"), Decimal("1300000.00"), Decimal("0.00")),
        ("Jawa Barat", Decimal("3000000.00"), Decimal("0.00"), Decimal("3000000.00")),
    ]


@pytest.mark.pg
def test_scd2_limit_history(dbt_built):
    rows = dbt_built.execute(
        "select credit_limit, valid_to is null from intermediate.int_credit_limit_history "
        "where account_id = %s order by valid_from",
        (uid("A1"),),
    ).fetchall()
    assert rows == [(Decimal("5000000.00"), False), (Decimal("7500000.00"), True)]


@pytest.mark.pg
def test_wib_month_boundary(dbt_built):  # Review Focus 4
    # T4 was created 2026-10-31 23:30 WIB (16:30 UTC): it belongs to October 31.
    disbursed = one(
        dbt_built,
        "select disbursed_amount from marts.mart_portfolio_daily where as_of_date = %s",
        (AS_OF,),
    )
    assert disbursed == Decimal("500000.00")
    outstanding = one(
        dbt_built,
        "select outstanding_principal from marts.mart_portfolio_daily where as_of_date = %s",
        (AS_OF,),
    )
    assert outstanding == Decimal("8300000.00")


@pytest.mark.pg
def test_invalid_transactions_are_quarantined(dbt_built):
    # fct_transactions only keeps approved, positive, valid-tenor purchases.
    assert one(dbt_built, "select count(*) from marts.fct_transactions") == 4
    assert one(dbt_built, "select count(*) from marts.fct_transactions where amount <= 0") == 0


@pytest.mark.pg
def test_collections_on_time_share(dbt_built):
    rows = dbt_built.execute(
        "select sum(n_repayments), sum(amount_collected) from marts.mart_collections"
    ).fetchone()
    # R1 (T2-1: 400,000 + 36,000), R2 and R4 (T3: 1,000,000 + 180,000 each); R3 was deleted
    assert rows == (3, Decimal("2796000.00"))


@pytest.mark.pg
def test_scd2_keeps_every_snapshotted_account(dbt_built):  # review I2: snapshot rows share a LSN
    rows = dbt_built.execute(
        "select account_id::text, credit_limit from intermediate.int_credit_limit_history "
        "where valid_to is null order by credit_limit"
    ).fetchall()
    assert rows == [
        (uid("A1"), Decimal("7500000.00")),
        (uid("A3"), Decimal("8000000.00")),
        (uid("A2"), Decimal("10000000.00")),
    ]
