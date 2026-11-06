"""Monthly portfolio report in Excel, inspired by OJK indicators (not an official format)."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Font

NOTE = "Inspired by OJK indicators, not an official format. Amounts in IDR; dates in WIB."
MONEY = "#,##0"
PERCENT = "0.00%"
BUCKETS = ("current", "dpd_1_30", "dpd_31_60", "dpd_61_90", "dpd_90_plus")


@dataclass
class ReportData:
    month: str
    summary: dict
    by_province: list[dict]
    dpd_buckets: dict


def month_window(month: str) -> tuple[date, date]:
    """First and last calendar day (WIB business dates) of a YYYY-MM month."""
    year, mon = (int(part) for part in month.split("-"))
    if not 1 <= mon <= 12:
        raise ValueError(f"invalid month: {month!r}")
    return date(year, mon, 1), date(year, mon, calendar.monthrange(year, mon)[1])


def load_report_data(conn, month: str) -> ReportData:
    first, last = month_window(month)
    rows = conn.execute(
        "SELECT province, total_outstanding, current_amount, dpd_1_30, dpd_31_60, dpd_61_90, "
        "dpd_90_plus FROM marts.mart_delinquency WHERE as_of_date = %s ORDER BY province",
        (last,),
    ).fetchall()
    by_province = [{"province": r[0], "outstanding": r[1], "dpd_90_plus": r[6]} for r in rows]
    buckets = {name: sum((r[i + 2] for r in rows), Decimal(0)) for i, name in enumerate(BUCKETS)}
    portfolio = conn.execute(
        "SELECT outstanding_principal, active_borrowers FROM marts.mart_portfolio_daily "
        "WHERE as_of_date = %s",
        (last,),
    ).fetchone()
    if portfolio is None:
        raise ValueError(f"no portfolio snapshot for {last}; run dbt for that date first")
    lending = conn.execute(
        "SELECT coalesce(sum(amount), 0), count(*) FROM marts.fct_transactions "
        "WHERE created_date_wib BETWEEN %s AND %s",
        (first, last),
    ).fetchone()
    total = sum((r[1] for r in rows), Decimal(0))
    ninety = buckets["dpd_90_plus"]
    summary = {
        "outstanding": portfolio[0],
        "disbursed": lending[0],
        "active_borrowers": portfolio[1],
        "transactions": lending[1],
        "twp90": (ninety / total).quantize(Decimal("0.0001")) if total else Decimal(0),
    }
    return ReportData(month, summary, by_province, buckets)


def validate(data: ReportData) -> list[str]:
    """Internal consistency: every breakdown adds up to the summary outstanding."""
    problems = []
    outstanding = data.summary["outstanding"]
    by_province = sum((row["outstanding"] for row in data.by_province), Decimal(0))
    if by_province != outstanding:
        problems.append(f"by_province total {by_province} != summary outstanding {outstanding}")
    buckets = sum(data.dpd_buckets.values(), Decimal(0))
    if buckets != outstanding:
        problems.append(f"DPD buckets total {buckets} != summary outstanding {outstanding}")
    return problems


def write_excel(data: ReportData, path: Path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    summary = wb.active
    summary.title = "Summary"
    summary["A1"] = NOTE
    summary["A1"].font = Font(italic=True)
    summary["A3"] = "Month"
    summary["B3"] = data.month
    labels = (
        ("Outstanding principal (end of month)", "outstanding", MONEY),
        ("Disbursed in month", "disbursed", MONEY),
        ("Active borrowers", "active_borrowers", "#,##0"),
        ("Transactions in month", "transactions", "#,##0"),
        ("TWP90", "twp90", PERCENT),
    )
    for i, (label, key, fmt) in enumerate(labels, start=4):
        summary.cell(row=i, column=1, value=label)
        cell = summary.cell(row=i, column=2, value=data.summary[key])
        cell.number_format = fmt
    summary.column_dimensions["A"].width = 40
    summary.column_dimensions["B"].width = 20

    provinces = wb.create_sheet("By province")
    provinces.append(["Province", "Outstanding", "90+ DPD", "TWP90"])
    for row in data.by_province:
        share = row["dpd_90_plus"] / row["outstanding"] if row["outstanding"] else Decimal(0)
        provinces.append([row["province"], row["outstanding"], row["dpd_90_plus"], share])
    for cells in provinces.iter_rows(min_row=2):
        cells[1].number_format = cells[2].number_format = MONEY
        cells[3].number_format = PERCENT
    provinces.column_dimensions["A"].width = 24

    buckets = wb.create_sheet("DPD buckets")
    buckets.append(["Bucket", "Outstanding"])
    for name in BUCKETS:
        buckets.append([name, data.dpd_buckets.get(name, Decimal(0))])
    for cells in buckets.iter_rows(min_row=2):
        cells[1].number_format = MONEY
    for sheet in wb.worksheets:
        for cell in sheet[1 if sheet.title != "Summary" else 3]:
            cell.font = Font(bold=True)
    wb.save(path)
    return path


def build_report(settings, month: str, conn=None) -> Path:
    """Load, validate and write `reports/regulatory_<month>.xlsx`."""
    from pdp.db import connect

    if conn is None:
        with connect(settings.wh_dsn) as owned:
            data = load_report_data(owned, month)
    else:
        data = load_report_data(conn, month)
    problems = validate(data)
    if problems:
        raise ValueError("report does not reconcile: " + "; ".join(problems))
    return write_excel(data, Path(settings.reports_dir) / f"regulatory_{month}.xlsx")
