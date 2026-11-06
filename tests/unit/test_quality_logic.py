import pytest

from pdp.quality import CHECK_TYPES
from pdp.quality.checks import CHECKS, CheckResult, run_checks_with
from pdp.quality.drift import field_diff, load_contract
from pdp.quality.stats import pct_diff, volume_zscore, zscore


def test_zscore():
    assert zscore(10, [100, 102, 98, 101]) < -3
    assert zscore(5, [5, 5, 5]) is None and zscore(1, [1, 2]) is None


def test_pct_diff():
    assert pct_diff(100, 99) == pytest.approx(0.01)
    assert pct_diff(0, 0) == 0.0 and pct_diff(0, 5) == 1.0


def test_volume_zscore_ignores_windows_before_traffic_started():
    counts = {7: 300, 8: 310, 9: 290, 10: 305, 11: 0}  # windows 0-6 had no traffic yet
    z = volume_zscore(counts, current_window=11, baseline_windows=10)
    assert z is not None and z < -3
    assert volume_zscore({11: 300}, current_window=11, baseline_windows=10) is None


def test_field_diff_and_contract():
    assert field_diff({"a", "b", "promo_code"}, {"a", "b", "c"}) == ({"promo_code"}, {"c"})
    assert "promo_code" not in load_contract("transactions")
    assert "amount" in load_contract("transactions")


def test_every_check_type_is_implemented():
    assert set(CHECKS) == set(CHECK_TYPES)


class FakeConn:
    def execute(self, *a, **k):
        raise AssertionError("not used")

    def commit(self):
        pass

    def rollback(self):
        pass


def test_runner_isolates_exceptions(monkeypatch):
    def boom(settings, wh, app):
        raise RuntimeError("database exploded")

    def ok(settings, wh, app):
        return [CheckResult("invalid_values", "transactions", "pass", "0", "0")]

    monkeypatch.setitem(CHECKS, "freshness", boom)
    monkeypatch.setitem(CHECKS, "invalid_values", ok)
    results = run_checks_with(
        None, FakeConn(), FakeConn(), ["freshness", "invalid_values"], persist=False
    )
    by_type = {r.check_type: r for r in results}
    assert by_type["freshness"].status == "error" and "exploded" in by_type["freshness"].observed
    assert by_type["invalid_values"].status == "pass"


def test_unknown_check_is_rejected():
    with pytest.raises(ValueError, match="unknown check"):
        run_checks_with(None, FakeConn(), FakeConn(), ["nope"], persist=False)
