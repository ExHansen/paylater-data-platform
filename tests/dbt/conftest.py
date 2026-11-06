import os

import pytest

from tests.conftest import PG_DSN_ENV, reset_test_database
from tests.dbt.fixture_builder import AS_OF, load_fixture


def run_dbt(args: list[str]) -> None:
    from dbt.cli.main import dbtRunner

    result = dbtRunner().invoke([*args, "--project-dir", "dbt", "--profiles-dir", "dbt"])
    if not result.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed: {result.exception or result.result}")


@pytest.fixture(scope="session")
def dbt_built():
    """Fixture CDC rows loaded into raw.*, then `dbt build` (models + tests) as of AS_OF."""
    import psycopg
    from psycopg.conninfo import conninfo_to_dict

    dsn = os.environ[PG_DSN_ENV]
    info = conninfo_to_dict(dsn)
    os.environ.update(
        PDP_DBT_HOST=str(info.get("host", "localhost")),
        PDP_DBT_PORT=str(info.get("port", 5432)),
        PDP_DBT_USER=str(info["user"]),
        PDP_DBT_PASSWORD=str(info.get("password", "")),
        PDP_DBT_DB=str(info["dbname"]),
    )
    reset_test_database(dsn)
    with psycopg.connect(dsn) as conn:
        load_fixture(conn)
        conn.commit()
    run_dbt(["build", "--vars", f"{{as_of_date: '{AS_OF}'}}"])
    conn = psycopg.connect(dsn)
    yield conn
    conn.close()
