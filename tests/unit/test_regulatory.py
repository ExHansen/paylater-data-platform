from datetime import date
from decimal import Decimal

import openpyxl
import pytest

from pdp.reports.regulatory import ReportData, month_window, validate, write_excel


def sample(by_province_total: int = 100, buckets_total: int = 100) -> ReportData:
    return ReportData(
        month="2026-10",
        summary={
            "outstanding": Decimal(100),
            "disbursed": Decimal(40),
            "active_borrowers": 3,
            "transactions": 4,
            "twp90": Decimal("0.70"),
        },
        by_province=[
            {
                "province": "Bali",
                "outstanding": Decimal(by_province_total - 30),
                "dpd_90_plus": Decimal(70),
            },
            {"province": "DKI Jakarta", "outstanding": Decimal(30), "dpd_90_plus": Decimal(0)},
        ],
        dpd_buckets={
            "current": Decimal(30),
            "dpd_1_30": Decimal(0),
            "dpd_31_60": Decimal(0),
            "dpd_61_90": Decimal(0),
            "dpd_90_plus": Decimal(buckets_total - 30),
        },
    )


def test_report_month_window_uses_wib():  # Review Focus 4
    assert month_window("2026-10") == (date(2026, 10, 1), date(2026, 10, 31))
    assert month_window("2026-12") == (date(2026, 12, 1), date(2026, 12, 31))
    assert month_window("2028-02") == (date(2028, 2, 1), date(2028, 2, 29))


def test_month_window_rejects_bad_input():
    with pytest.raises(ValueError):
        month_window("2026-13")


def test_validate_passes_consistent_data():
    assert validate(sample()) == []


def test_validate_catches_province_mismatch():
    assert validate(sample(by_province_total=99)) == [
        "by_province total 99 != summary outstanding 100"
    ]


def test_validate_catches_bucket_mismatch():
    assert validate(sample(buckets_total=101)) == [
        "DPD buckets total 101 != summary outstanding 100"
    ]


def test_excel_layout(tmp_path):
    path = write_excel(sample(), tmp_path / "r.xlsx")
    wb = openpyxl.load_workbook(path)
    assert wb.sheetnames == ["Summary", "By province", "DPD buckets"]
    assert "not an official format" in wb["Summary"]["A1"].value
    values = [cell.value for row in wb["By province"].iter_rows() for cell in row]
    assert "Bali" in values and "DKI Jakarta" in values
