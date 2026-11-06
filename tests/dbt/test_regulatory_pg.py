from decimal import Decimal

import openpyxl
import pytest

from pdp.config import Settings
from pdp.reports.regulatory import build_report, load_report_data, validate


@pytest.mark.pg
def test_report_reconciles_with_marts(dbt_built):
    data = load_report_data(dbt_built, "2026-10")
    assert validate(data) == []
    assert data.summary["outstanding"] == Decimal("8300000.00")
    assert data.summary["twp90"] == Decimal("0.8434")
    assert data.summary["disbursed"] == Decimal("500000.00")  # T4 only, created 31 Oct (WIB)


@pytest.mark.pg
def test_build_report_writes_workbook(dbt_built, tmp_path):
    settings = Settings(_env_file=None, reports_dir=tmp_path)
    path = build_report(settings, "2026-10", conn=dbt_built)
    assert path.name == "regulatory_2026-10.xlsx"
    assert openpyxl.load_workbook(path)["Summary"]["B3"].value == "2026-10"
