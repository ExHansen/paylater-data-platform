"""Compare the fields seen in CDC events with the expected contract per table."""

from __future__ import annotations

from functools import cache
from pathlib import Path

import yaml

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "infra" / "contracts"


@cache
def load_contract(table: str, directory: Path | None = None) -> frozenset[str]:
    path = (directory or CONTRACTS_DIR) / f"{table}.yml"
    return frozenset(yaml.safe_load(path.read_text(encoding="utf-8"))["columns"])


def field_diff(
    observed: set[str], contract: set[str] | frozenset[str]
) -> tuple[set[str], set[str]]:
    """(fields added at the source, fields that disappeared)."""
    return set(observed) - set(contract), set(contract) - set(observed)
