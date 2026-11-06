"""Read-only queries behind the dashboard. None of them touch personal data."""

from __future__ import annotations

LATEST_CHECKS = """
SELECT check_type, target, status, observed, expected, run_at
FROM (
    SELECT DISTINCT ON (check_type, target) *
    FROM ops.dq_results
    ORDER BY check_type, target, run_at DESC
) AS latest
ORDER BY status = 'pass', check_type, target
"""

PASS_RATE_24H = """
SELECT count(*) FILTER (WHERE status = 'pass')::float / nullif(count(*), 0) AS pass_rate,
       count(*) AS checks
FROM ops.dq_results
WHERE run_at > now() - interval '24 hours'
"""

RECENT_ALERTS = """
SELECT created_at, check_type, target, message
FROM ops.alerts
ORDER BY created_at DESC
LIMIT 50
"""

RISK_ALERTS = """
SELECT created_at, account_id, rules, amount, latency_ms
FROM online.risk_alerts
ORDER BY created_at DESC
LIMIT %s
"""

LATENCY_1H = """
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_ms) AS p50_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_ms,
       count(*) AS alerts
FROM online.risk_alerts
WHERE created_at > now() - interval '1 hour'
"""

PORTFOLIO_LATEST = """
SELECT as_of_date, outstanding_principal, active_borrowers, disbursed_amount, txn_count
FROM marts.mart_portfolio_daily
ORDER BY as_of_date DESC
LIMIT 1
"""

TWP90_TREND = """
SELECT as_of_date,
       sum(dpd_90_plus) / nullif(sum(total_outstanding), 0) AS twp90,
       sum(total_outstanding) AS total_outstanding
FROM marts.mart_delinquency
GROUP BY as_of_date
ORDER BY as_of_date
"""

BY_PROVINCE = """
SELECT province, total_outstanding, dpd_90_plus, twp90
FROM marts.mart_delinquency
WHERE as_of_date = (SELECT max(as_of_date) FROM marts.mart_delinquency)
ORDER BY total_outstanding DESC
"""

TOP_MERCHANTS = """
SELECT merchant_name, category, province, gmv, txn_count, dpd_90_plus
FROM marts.mart_merchant_performance
ORDER BY gmv DESC
LIMIT 10
"""

ALL_SQL = (
    LATEST_CHECKS,
    PASS_RATE_24H,
    RECENT_ALERTS,
    RISK_ALERTS,
    LATENCY_1H,
    PORTFOLIO_LATEST,
    TWP90_TREND,
    BY_PROVINCE,
    TOP_MERCHANTS,
)


def fetch(conn, sql: str, params: tuple = ()) -> list[dict]:
    cursor = conn.execute(sql, params)
    names = [column.name for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
