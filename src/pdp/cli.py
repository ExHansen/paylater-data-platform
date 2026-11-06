"""`pdp` command-line interface."""

from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import timedelta
from enum import StrEnum
from typing import Annotated

import typer
from rich.console import Console
from rich.table import Table

from pdp.config import Settings
from pdp.simulator.faults import FAULTS

app = typer.Typer(
    no_args_is_help=True,
    add_completion=False,
    help="PayLater data platform: run the stack, simulate traffic, inject faults, check quality.",
)
quality_app = typer.Typer(no_args_is_help=True, help="Data-quality checks.")
app.add_typer(quality_app, name="quality")
console = Console()

Profile = StrEnum("Profile", {"core": "core", "full": "full"})
Fault = StrEnum("Fault", {name: name for name in FAULTS})


def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, **kwargs)


def compose(*args: str, profile: str | None = None, check: bool = True, capture: bool = False):
    settings = Settings()
    cmd = ["docker", "compose", "-f", str(settings.compose_file)]
    if profile:
        cmd += ["--profile", profile]
    cmd += list(args)
    try:
        return _run(cmd, check=check, capture_output=capture, text=True)
    except subprocess.CalledProcessError as exc:
        console.print(
            f"[red]Command failed:[/red] {' '.join(cmd)}\n"
            "Inspect a service with `docker compose logs <service>`."
        )
        raise typer.Exit(1) from exc


def parse_duration(text: str) -> timedelta:
    match = re.fullmatch(r"(\d+)\s*([smh])", text.strip())
    if not match:
        raise ValueError(f"duration must look like 30s, 10m or 2h, got {text!r}")
    value, unit = int(match[1]), match[2]
    return timedelta(**{{"s": "seconds", "m": "minutes", "h": "hours"}[unit]: value})


def dbt_env(dsn: str) -> dict[str, str]:
    """PDP_DBT_* variables for dbt/profiles.yml, taken from the warehouse DSN."""
    from psycopg.conninfo import conninfo_to_dict

    info = conninfo_to_dict(dsn)
    return {
        "PDP_DBT_HOST": str(info.get("host", "localhost")),
        "PDP_DBT_PORT": str(info.get("port", 5432)),
        "PDP_DBT_USER": str(info.get("user", "pdp")),
        "PDP_DBT_PASSWORD": str(info.get("password", "")),
        "PDP_DBT_DB": str(info.get("dbname", "warehouse")),
    }


@app.command()
def up(profile: Annotated[Profile, typer.Option(help="core or full")] = Profile.core) -> None:
    """Build and start the stack, waiting until every service is healthy."""
    compose("up", "-d", "--build", "--wait", profile=profile.value)
    console.print(f"Stack is up (profile {profile.value}).")


@app.command()
def down(volumes: Annotated[bool, typer.Option(help="Also delete data volumes.")] = False) -> None:
    """Stop the stack."""
    compose("down", *(["--volumes"] if volumes else []), profile="full")


@app.command()
def seed(
    months: Annotated[int, typer.Option(help="Months of history.")] = 12,
    seed_value: Annotated[int, typer.Option("--seed", help="Random seed.")] = 42,
) -> None:
    """Fill the application database with a deterministic history."""
    from pdp.simulator.seed import AlreadySeeded
    from pdp.simulator.seed import seed as run_seed

    try:
        counts = run_seed(Settings(), months, seed_value)
    except AlreadySeeded as exc:
        console.print(f"[yellow]{exc}[/yellow]")
        raise typer.Exit(1) from exc
    console.print(f"Seeded: {counts}")


@app.command()
def live(
    rate: Annotated[float, typer.Option(help="Average purchases per second.")] = 5.0,
    duration: Annotated[str | None, typer.Option(help="e.g. 10m; default runs forever.")] = None,
) -> None:
    """Generate live PayLater traffic."""
    from pdp.simulator.live import LiveSimulator

    stats = LiveSimulator(Settings(), rate).run(parse_duration(duration) if duration else None)
    console.print(f"Live traffic stopped: {stats}")


@app.command()
def fault(
    kind: Annotated[Fault, typer.Argument(help="Fault to inject.")],
    minutes: Annotated[float, typer.Option(help="Duration for stall/drop faults.")] = 3,
) -> None:
    """Inject a fault (logged in ops.fault_log so its detection time can be measured)."""
    from pdp.db import connect
    from pdp.simulator.faults import FaultContext, inject

    settings = Settings()

    def producer():
        from confluent_kafka import Producer

        return Producer({"bootstrap.servers": settings.kafka_bootstrap})

    with connect(settings.app_dsn) as app_conn, connect(settings.wh_dsn) as wh_conn:
        ctx = FaultContext(
            settings=settings,
            app_conn=app_conn,
            wh_conn=wh_conn,
            compose=lambda *args: compose(*args, profile="full"),
            producer_factory=producer,
        )
        fault_id = inject(kind.value, ctx, minutes=minutes)
    console.print(f"Injected {kind.value} (fault_id {fault_id}).")


@quality_app.command("run")
def quality_run(
    checks: Annotated[str | None, typer.Option(help="Comma-separated checks; default all.")] = None,
) -> None:
    """Run data-quality checks now and store the results in ops.dq_results."""
    from pdp.quality import run_checks

    names = [c.strip() for c in checks.split(",")] if checks else None
    try:
        results = run_checks(Settings(), names)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(2) from exc
    table = Table(title="Data-quality checks")
    for column in ("check", "target", "status", "observed", "expected"):
        table.add_column(column)
    colors = {"pass": "green", "warn": "yellow", "fail": "red", "error": "magenta"}
    for r in results:
        status = f"[{colors[r.status]}]{r.status}[/{colors[r.status]}]"
        table.add_row(r.check_type, r.target, status, r.observed, r.expected)
    console.print(table)


@app.command("dbt-build")
def dbt_build(
    as_of: Annotated[str | None, typer.Option(help="Business date YYYY-MM-DD (WIB).")] = None,
) -> None:
    """Run `dbt build` (models + tests) against the warehouse."""
    from dbt.cli.main import dbtRunner

    for key, value in dbt_env(Settings().wh_dsn).items():
        os.environ.setdefault(key, value)
    args = ["build", "--project-dir", "dbt", "--profiles-dir", "dbt"]
    if as_of:
        args += ["--vars", json.dumps({"as_of_date": as_of})]
    result = dbtRunner().invoke(args)
    if not result.success:
        console.print("[red]dbt build failed[/red]")
        raise typer.Exit(1)


@app.command()
def report(month: Annotated[str, typer.Option(help="Month YYYY-MM.")]) -> None:
    """Build the monthly regulatory report (Excel)."""
    from pdp.reports.regulatory import build_report

    try:
        path = build_report(Settings(), month)
    except ValueError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1) from exc
    console.print(f"Report written to {path}")


@app.command()
def status() -> None:
    """Service health, consumer lag, freshness and open alerts."""
    from pdp.db import connect
    from pdp.quality.checks import check_freshness

    settings = Settings()
    ps = compose("ps", "--format", "json", profile="full", check=False, capture=True)
    services = Table(title="Services")
    services.add_column("service")
    services.add_column("state")
    for line in (ps.stdout or "").splitlines():
        try:
            item = json.loads(line)
        except ValueError:
            continue
        services.add_row(item.get("Service", "?"), f"{item.get('State')} {item.get('Health', '')}")
    console.print(services)
    try:
        with connect(settings.wh_dsn) as wh, connect(settings.app_dsn) as app_conn:
            fresh = Table(title="Freshness (app vs warehouse)")
            fresh.add_column("table")
            fresh.add_column("status")
            fresh.add_column("lag")
            for r in check_freshness(settings, wh, app_conn):
                fresh.add_row(r.target, r.status, r.observed)
            console.print(fresh)
            open_alerts = wh.execute(
                "SELECT count(*) FROM ops.alerts WHERE created_at > now() - interval '1 hour'"
            ).fetchone()[0]
            console.print(f"Alerts in the last hour: {open_alerts}")
    except Exception as exc:  # noqa: BLE001 - status must work while parts are down
        console.print(f"[yellow]Database not reachable: {exc}[/yellow]")


@app.command()
def e2e() -> None:
    """Run the end-to-end scenario and write e2e-summary.md."""
    from pdp.e2e import run as run_e2e

    result = run_e2e(Settings())
    console.print(f"E2E finished; summary in {result.summary_path}")


if __name__ == "__main__":  # pragma: no cover
    app()
