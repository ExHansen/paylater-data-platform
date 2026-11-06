import subprocess
from datetime import timedelta

import pytest
from typer.testing import CliRunner

from pdp import cli
from pdp.cli import app, parse_duration

runner = CliRunner()


class Done:
    returncode = 0
    stdout = ""


@pytest.fixture
def commands(monkeypatch):
    seen = []

    def fake_run(cmd, **kwargs):
        seen.append(cmd)
        return Done()

    monkeypatch.setattr(cli, "_run", fake_run)
    return seen


def test_up_builds_compose_command(commands):
    result = runner.invoke(app, ["up", "--profile", "core"])
    assert result.exit_code == 0, result.output
    assert commands[0][:3] == ["docker", "compose", "-f"]
    assert commands[0][-6:] == ["--profile", "core", "up", "-d", "--build", "--wait"]


def test_down_with_volumes_covers_every_profile(commands):
    assert runner.invoke(app, ["down", "--volumes"]).exit_code == 0
    assert commands[0][-4:] == ["--profile", "full", "down", "--volumes"]


def test_unknown_profile_and_fault_are_usage_errors():
    assert runner.invoke(app, ["up", "--profile", "huge"]).exit_code == 2
    assert runner.invoke(app, ["fault", "nope"]).exit_code == 2


def test_failed_compose_exits_1(monkeypatch):
    def failing(cmd, **kwargs):
        raise subprocess.CalledProcessError(1, cmd)

    monkeypatch.setattr(cli, "_run", failing)
    result = runner.invoke(app, ["up"])
    assert result.exit_code == 1 and "docker compose logs" in result.output


@pytest.mark.parametrize(
    "text,expected",
    [("10m", timedelta(minutes=10)), ("30s", timedelta(seconds=30)), ("2h", timedelta(hours=2))],
)
def test_parse_duration(text, expected):
    assert parse_duration(text) == expected


def test_parse_duration_rejects_garbage():
    with pytest.raises(ValueError):
        parse_duration("ten minutes")


def test_dbt_env_from_dsn(monkeypatch):
    for key in ("PDP_DBT_HOST", "PDP_DBT_DB"):
        monkeypatch.delenv(key, raising=False)
    env = cli.dbt_env("postgresql://pdp:secret@db.example:6543/warehouse")
    assert env["PDP_DBT_HOST"] == "db.example" and env["PDP_DBT_PORT"] == "6543"
    assert env["PDP_DBT_DB"] == "warehouse" and env["PDP_DBT_PASSWORD"] == "secret"
