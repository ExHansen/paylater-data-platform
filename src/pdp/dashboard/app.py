"""Streamlit dashboard: pipeline health, real-time risk alerts, portfolio KPIs.

Run with `streamlit run src/pdp/dashboard/app.py` (the `dashboard` service in docker compose).
"""

from __future__ import annotations

import pandas as pd
import psycopg
import streamlit as st

from pdp.config import Settings
from pdp.dashboard import queries
from pdp.quality.mttd import compute_mttd

st.set_page_config(page_title="PayLater data platform", layout="wide")
UNREACHABLE = "Warehouse not reachable — is `pdp up` running?"


def connection():
    try:
        return psycopg.connect(Settings().wh_dsn, autocommit=True, connect_timeout=5)
    except psycopg.Error:
        st.warning(UNREACHABLE)
        st.stop()


def table(rows: list[dict]) -> None:
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    else:
        st.caption("No rows yet.")


def pipeline_health() -> None:
    st.title("Pipeline health")
    with connection() as conn:
        rate = queries.fetch(conn, queries.PASS_RATE_24H)[0]
        mttd = compute_mttd(conn)
        c1, c2, c3 = st.columns(3)
        pass_rate = rate["pass_rate"]
        c1.metric("Checks passing (24 h)", "–" if pass_rate is None else f"{pass_rate:.0%}")
        c2.metric("Check runs (24 h)", rate["checks"])
        detected = [m.mttd_seconds for m in mttd if m.mttd_seconds is not None]
        c3.metric(
            "Mean time to detect",
            "–" if not detected else f"{sum(detected) / len(detected) / 60:.1f} min",
        )
        st.subheader("Latest result per check")
        table(queries.fetch(conn, queries.LATEST_CHECKS))
        st.subheader("Injected faults and detection time")
        table(
            [
                {
                    "fault": m.fault_type,
                    "check": m.check_type,
                    "injected_at": m.injected_at,
                    "detected_at": m.detected_at,
                    "mttd_min": None if m.mttd_seconds is None else round(m.mttd_seconds / 60, 1),
                }
                for m in mttd
            ]
        )
        st.subheader("Recent alerts")
        table(queries.fetch(conn, queries.RECENT_ALERTS))


def risk_alerts() -> None:
    st.title("Real-time risk alerts")
    with connection() as conn:
        latency = queries.fetch(conn, queries.LATENCY_1H)[0]
        c1, c2, c3 = st.columns(3)
        c1.metric("Alerts (1 h)", latency["alerts"])
        c2.metric(
            "Latency p50", "–" if latency["p50_ms"] is None else f"{latency['p50_ms']:.0f} ms"
        )
        c3.metric(
            "Latency p95", "–" if latency["p95_ms"] is None else f"{latency['p95_ms']:.0f} ms"
        )
        st.caption(
            "Latency = alert time − commit time of the purchase in the application database."
        )
        table(queries.fetch(conn, queries.RISK_ALERTS, (100,)))


def portfolio() -> None:
    st.title("Portfolio")
    with connection() as conn:
        try:
            latest = queries.fetch(conn, queries.PORTFOLIO_LATEST)
        except psycopg.errors.UndefinedTable:
            st.info("Marts are not built yet — run `pdp dbt-build`.")
            return
        if latest:
            row = latest[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Outstanding principal", f"Rp {row['outstanding_principal']:,.0f}")
            c2.metric("Active borrowers", f"{row['active_borrowers']:,}")
            c3.metric(
                "Disbursed on " + str(row["as_of_date"]), f"Rp {row['disbursed_amount']:,.0f}"
            )
        trend = queries.fetch(conn, queries.TWP90_TREND)
        if trend:
            st.subheader("TWP90 (share of outstanding 90+ days past due)")
            frame = pd.DataFrame(trend).set_index("as_of_date")
            st.line_chart(frame["twp90"].astype(float))
        st.subheader("By province (latest snapshot)")
        table(queries.fetch(conn, queries.BY_PROVINCE))
        st.subheader("Top merchants by GMV")
        table(queries.fetch(conn, queries.TOP_MERCHANTS))


page = st.navigation(
    [
        st.Page(pipeline_health, title="Pipeline health", url_path="health", default=True),
        st.Page(risk_alerts, title="Risk alerts", url_path="risk"),
        st.Page(portfolio, title="Portfolio", url_path="portfolio"),
    ]
)
page.run()
