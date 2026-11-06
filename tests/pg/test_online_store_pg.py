import json
import os
from decimal import Decimal
from pathlib import Path

import pytest

from tests.conftest import PG_DSN_ENV

CREATE = json.loads(Path("tests/fixtures/debezium/create.json").read_text())


@pytest.mark.pg
def test_exposure_applies_each_change_once(fresh_db):
    from pdp.stream.app import OnlineStore

    store = OnlineStore(os.environ[PG_DSN_ENV])
    store.apply_exposure("transactions", CREATE)
    store.apply_exposure("transactions", CREATE)  # redelivered
    exposure = fresh_db.execute("SELECT exposure FROM online.account_exposure").fetchone()[0]
    assert exposure == Decimal(CREATE["after"]["amount"])
