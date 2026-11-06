import os
from pathlib import Path

import pytest

from pdp.dashboard import queries
from tests.conftest import PG_DSN_ENV


@pytest.mark.pg
def test_portfolio_queries_on_marts(dbt_built):
    by_province = queries.fetch(dbt_built, queries.BY_PROVINCE)
    assert {row["province"] for row in by_province} == {"Bali", "DKI Jakarta", "Jawa Barat"}
    trend = queries.fetch(dbt_built, queries.TWP90_TREND)
    assert trend and all(row["twp90"] is None or 0 <= row["twp90"] <= 1 for row in trend)
    assert queries.fetch(dbt_built, queries.LATENCY_1H)[0]["alerts"] == 0


@pytest.mark.pg
def test_dashboard_renders(dbt_built, monkeypatch):
    from streamlit.testing.v1 import AppTest

    monkeypatch.setenv("PDP_WH_DSN", os.environ[PG_DSN_ENV])
    app_path = Path(__file__).resolve().parents[2] / "src" / "pdp" / "dashboard" / "app.py"
    at = AppTest.from_file(str(app_path), default_timeout=60).run()
    assert not at.exception
    assert any("Pipeline health" in t.value for t in at.title)
