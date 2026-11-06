import json
import re
from pathlib import Path

import yaml

TABLES = ("users", "credit_accounts", "merchants", "transactions", "installments", "repayments")
COMPOSE = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
APP_SQL = Path("infra/postgres/10_app.sql").read_text(encoding="utf-8")


def ddl_columns(table: str) -> set[str]:
    block = re.search(rf"CREATE TABLE IF NOT EXISTS {table} \((.*?)\);", APP_SQL, re.S).group(1)
    return {line.split()[0] for line in block.strip().splitlines() if line.strip()}


def services(profile: str) -> set[str]:
    return {n for n, s in COMPOSE["services"].items() if profile in s.get("profiles", [])}


def test_profiles_and_memory_limits():
    core = {"postgres", "kafka", "connect", "connect-init", "simulator", "loader", "stream"}
    assert core <= services("core")
    assert core | {"dashboard"} <= services("full")
    assert "dashboard" not in services("core")
    assert all("mem_limit" in s for s in COMPOSE["services"].values())


def test_images_are_pinned():
    for name, s in COMPOSE["services"].items():
        if "image" in s and "build" not in s:
            assert ":" in s["image"] and not s["image"].endswith(":latest"), name


def test_postgres_has_logical_wal_and_init_scripts():
    pg = COMPOSE["services"]["postgres"]
    assert "wal_level=logical" in " ".join(pg["command"])
    assert any("docker-entrypoint-initdb.d" in v for v in pg["volumes"])


def test_contracts_match_ddl():
    for t in TABLES:
        contract = yaml.safe_load(Path(f"infra/contracts/{t}.yml").read_text(encoding="utf-8"))
        assert contract["table"] == t and set(contract["columns"]) == ddl_columns(t)


def test_connector_config():
    c = json.loads(Path("infra/connect/postgres-cdc.json").read_text(encoding="utf-8"))["config"]
    assert c["decimal.handling.mode"] == "string" and c["publication.name"] == "pdp_pub"
    assert c["publication.autocreate.mode"] == "disabled" and c["plugin.name"] == "pgoutput"
    assert c["value.converter.schemas.enable"] == "false" and c["topic.prefix"] == "paylater"
    assert set(c["table.include.list"].split(",")) == {f"public.{t}" for t in TABLES}


def test_devcontainer_requests_enough_machine():
    dc = json.loads(Path(".devcontainer/devcontainer.json").read_text(encoding="utf-8"))
    assert dc["hostRequirements"]["cpus"] >= 4 and 8501 in dc["forwardPorts"]


def test_kafka_keeps_its_log_in_a_named_volume():  # review C1: topics + offsets live with pgdata
    kafka = COMPOSE["services"]["kafka"]
    log_dir = kafka["environment"]["KAFKA_LOG_DIRS"]
    mounts = [v.split(":") for v in kafka["volumes"]]
    assert any(target == log_dir and source in COMPOSE["volumes"] for source, target in mounts)


def test_airflow_dags_start_unpaused():  # review I3: otherwise nothing is ever scheduled
    env = COMPOSE["services"]["airflow"]["environment"]
    assert env["AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION"] == "false"
