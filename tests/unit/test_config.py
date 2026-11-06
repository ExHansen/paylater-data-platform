import os
from pathlib import Path

from pdp.config import Settings


def _clear(monkeypatch):
    for key in list(os.environ):
        if key.startswith("PDP_"):
            monkeypatch.delenv(key)


def test_defaults(monkeypatch):
    _clear(monkeypatch)
    s = Settings(_env_file=None)
    assert s.topic_prefix == "paylater" and s.freshness_threshold_min == 10 and s.seed == 42
    assert s.volume_window_min == 60 and s.volume_baseline_windows == 24
    assert s.reports_dir == Path("reports") and s.alert_webhook is None


def test_env_override(monkeypatch):
    _clear(monkeypatch)
    monkeypatch.setenv("PDP_VOLUME_WINDOW_MIN", "1")
    monkeypatch.setenv("PDP_ALERT_WEBHOOK", "https://hooks.example/x")
    s = Settings(_env_file=None)
    assert s.volume_window_min == 1 and s.alert_webhook == "https://hooks.example/x"
