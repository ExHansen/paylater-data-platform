"""Runtime settings from PDP_* environment variables (and an optional .env file)."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PDP_", env_file=".env", extra="ignore")

    app_dsn: str = "postgresql://pdp:pdp@localhost:5432/paylater_app"
    wh_dsn: str = "postgresql://pdp:pdp@localhost:5432/warehouse"
    kafka_bootstrap: str = "localhost:9092"
    topic_prefix: str = "paylater"
    pii_salt: str = "change-me"
    alert_webhook: str | None = None

    freshness_threshold_min: int = 10
    volume_window_min: int = 60
    volume_baseline_windows: int = 24
    volume_z_threshold: float = 3.0
    reconciliation_tolerance: float = 0.001

    seed: int = 42
    reports_dir: Path = Path("reports")
    compose_file: Path = Path("docker-compose.yml")
