import re
from pathlib import Path

TABLES = ("users", "credit_accounts", "merchants", "transactions", "installments", "repayments")
APP = Path("infra/postgres/10_app.sql").read_text(encoding="utf-8")
WH = Path("infra/postgres/20_warehouse.sql").read_text(encoding="utf-8")
DBS = Path("infra/postgres/00_databases.sql").read_text(encoding="utf-8")


def test_publication_and_replica_identity():
    assert "CREATE PUBLICATION pdp_pub" in APP
    for t in TABLES:
        assert f"ALTER TABLE {t} REPLICA IDENTITY FULL" in APP


def test_installments_carry_account_id():
    block = re.search(r"CREATE TABLE IF NOT EXISTS installments \((.*?)\);", APP, re.S).group(1)
    assert "account_id" in block


def test_raw_tables_are_idempotent_on_offset():
    assert WH.count("UNIQUE (kafka_topic, kafka_partition, kafka_offset)") == len(TABLES)
    for t in TABLES:
        assert f"raw.cdc_{t}" in WH


def test_no_check_constraints_on_amount_or_tenor():
    assert not re.search(r"CHECK\s*\(\s*(amount|tenor_months)", APP)


def test_cdc_role_and_databases():
    assert "REPLICATION" in DBS and "pdp_cdc" in DBS
    assert "CREATE DATABASE warehouse" in DBS and "CREATE DATABASE airflow" in DBS


def test_app_and_warehouse_files_need_no_special_roles():
    assert "pdp_cdc" not in APP and "pdp_cdc" not in WH
