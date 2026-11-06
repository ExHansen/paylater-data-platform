import pytest

from tests.dbt.fixture_builder import uid


def one(conn, sql, params=()):
    return conn.execute(sql, params).fetchone()[0]


@pytest.mark.pg
def test_staging_picks_latest_lsn(dbt_built):  # Review Focus 3
    sql = "select channel from staging.stg_transactions where txn_id = %s"
    assert one(dbt_built, sql, (uid("T2"),)) == "web"
    assert one(dbt_built, "select count(*) from staging.stg_transactions") == 4


@pytest.mark.pg
def test_deleted_rows_are_gone(dbt_built):
    sql = "select count(*) from staging.stg_repayments where repayment_id = %s"
    assert one(dbt_built, sql, (uid("R3"),)) == 0
    assert one(dbt_built, "select count(*) from staging.stg_repayments") == 3


@pytest.mark.pg
def test_types_are_decoded(dbt_built):
    row = dbt_built.execute(
        "select due_date, principal, status from staging.stg_installments "
        "where installment_id = %s",
        (uid("T3-3"),),
    ).fetchone()
    assert str(row[0]) == "2026-07-30" and str(row[1]) == "1000000.00" and row[2] == "unpaid"


@pytest.mark.pg
def test_pii_is_hashed(dbt_built):
    nik_hash = one(dbt_built, "select nik_hash from staging.stg_users limit 1")
    assert len(nik_hash) == 64 and not nik_hash.isdigit()
    columns = {
        r[0]
        for r in dbt_built.execute(
            "select column_name from information_schema.columns "
            "where table_schema = 'staging' and table_name = 'stg_users'"
        ).fetchall()
    }
    assert not columns & {"nik", "phone", "full_name"}


@pytest.mark.pg
def test_reconciliation_queries_match_the_real_models(dbt_built):
    # The app tables are empty here: every query must still run against the real dbt models.
    from pdp.config import Settings
    from pdp.quality.checks import TABLES, check_reconciliation

    results = check_reconciliation(Settings(_env_file=None), dbt_built, dbt_built)
    assert sorted(r.target for r in results) == sorted(TABLES)
    assert {r.target: r.status for r in results}["transactions"] == "fail"
