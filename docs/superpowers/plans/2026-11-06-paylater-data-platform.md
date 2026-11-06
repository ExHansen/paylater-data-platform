# paylater-data-platform Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Docker-based fintech (PayLater) data platform: simulated OLTP in Postgres → Debezium CDC → Kafka → real-time risk features and alerts, plus a dbt warehouse (TWP90, star schema) orchestrated by Airflow, with data-quality checks whose detection time (MTTD) is measured against injected faults. Proven end-to-end in GitHub Actions.

**Architecture:** One Python package `pdp` holds every piece of logic (simulator, loader, stream rules, quality checks, reports, CLI) as plain, testable functions. Thin entry points wrap it: Quix Streams app, Kafka consumer loop, Airflow DAGs, and Streamlit. Postgres hosts the `paylater_app` database (source) and the `warehouse` database (raw, staging, intermediate, marts, online, ops schemas, plus Airflow metadata). Heavy verification runs in CI, never on the author's laptop.

**Tech Stack:**
- Runtime images: Python 3.11, uv, Postgres 16.15, Kafka 4.3.2 (KRaft), Debezium Connect 3.7.0.Final, Airflow 3.3.2 (`apache/airflow:3.3.2-python3.11`).
- Python libraries: quixstreams 3.28, confluent-kafka 2.16, psycopg 3.3, dbt-core 1.12 + dbt-postgres 1.11, Streamlit 1.67, openpyxl 3.1, Typer, pydantic-settings, PyYAML.
- Tooling: pytest, ruff.

**Spec:** `docs/superpowers/specs/2026-11-06-paylater-data-platform-design.md`

## Global Constraints

**Platform and dependencies**
- Python `>=3.11`; dependencies managed with uv only.
- Core runtime dependencies stay light so the Airflow image can install `pdp` under Airflow's constraints. Kafka, stream and dashboard libraries live in extras `loader`, `stream`, `dashboard`; `dev` is a dependency group.
- Image tags are pinned exactly as in Tech Stack. Never use `latest`.

**Where things run**
- The author's laptop runs only unit tests (`pytest -m "not pg and not e2e"`), ruff and `dbt parse`. Docker is never required locally.
- Tests marked `pg` need `PDP_TEST_PG_DSN` and are skipped without it. They run in CI against a Postgres 16 service.
- Tests marked `e2e` run only through `pdp e2e` in the e2e workflow.

**Time and money**
- Store timestamps as `timestamptz` (UTC).
- Business dates (DPD as-of date, month boundaries, schedules) use `Asia/Jakarta` (WIB).
- Money is `numeric(18,2)` in SQL and `Decimal` in Python. Debezium sends decimals as strings (`decimal.handling.mode=string`).

**Determinism**
- Simulator randomness uses `random.Random(f"{seed}:{purpose}")`, never global `random`.
- Default seed `42`.

**Naming**
- Topics: `paylater.public.<table>` for CDC and `risk.alerts` for alerts.
- Consumer groups: `pdp-raw-loader` and `pdp-risk`.
- Debezium publication `pdp_pub`, replication slot `pdp_slot`.
- Database roles: `pdp` (owner) and `pdp_cdc` (REPLICATION).

**Honesty labels**
- The regulatory report carries "Inspired by OJK indicators, not an official format".
- README results come only from CI e2e output.

**Conventions**
- Code, CLI text and README are in English; spec and plan files keep their language.
- Commit after every task. Messages end with a blank line and then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Git identity is repo-local: `91862289+ExHansen@users.noreply.github.com`.

**Prerequisite before Task 3:** the user creates the empty GitHub repo `ExHansen/paylater-data-platform` and the branch is pushed. From then on every task's `pg` verification step is "push, then read the `ci` workflow result". Ask the user before the first push.

## Review Focus

1. **Debezium value encodings.**
   - Debezium sends numerics as strings, `date` as days since epoch (int), and `timestamptz` as ISO-8601 strings; snapshot rows have `op = "r"`; deletes are followed by a tombstone (`null` value).
   - The parser must convert all of these exactly, never via float.
   - *Test in Task 5:* `test_parse_encodings_exactly` and `test_tombstone_is_ignored`.
2. **A crash between the DB commit and the Kafka offset commit.**
   - The same batch is delivered again.
   - Unique `(kafka_topic, kafka_partition, kafka_offset)` plus `ON CONFLICT DO NOTHING` must keep the row count unchanged.
   - *Test in Task 6:* `test_writer_is_idempotent_on_redelivery` (pg).
3. **Late or out-of-order versions of a row.**
   - A lower `lsn` can arrive at a higher offset.
   - Staging must keep the highest `lsn` version and never resurrect a deleted row.
   - *Test in Task 9:* fixture rows `T2_v1_late` and `R3_deleted`, asserted in `test_staging_picks_latest_lsn` (pg).
4. **WIB vs UTC at month and day edges.**
   - A transaction at 2026-10-31 23:30 WIB (16:30 UTC) belongs to October in the regulatory report and in `mart_portfolio_daily`.
   - DPD uses the WIB date.
   - *Tests in Task 10 and Task 12:* `test_wib_month_boundary` (pg) and `test_report_month_window_uses_wib`.
5. **Schema drift must not crash anything downstream.**
   - An unknown field in `after` (e.g. `promo_code`) is kept by the loader and ignored by stream and staging (explicit column lists).
   - Only the `schema_drift` check reacts.
   - *Tests in Task 5, Task 7 and Task 11:* `test_unknown_fields_are_preserved`, `test_stream_ignores_unknown_fields` and `test_schema_drift_detects_new_field` (pg).

---

## File Map

| Path | Responsibility | Task |
|---|---|---|
| `pyproject.toml`, `.python-version`, `.gitignore`, `.gitattributes`, `.env.example`, `LICENSE`, `.github/workflows/ci.yml` | Scaffold, unit CI | 1 |
| `src/pdp/config.py` | `Settings` (`PDP_*`) | 1 |
| `tests/conftest.py` | `pg` / `e2e` markers, `pg_conn` fixture | 1 |
| `src/pdp/simulator/domain.py`, `generator.py` | Entities, constants, deterministic history | 2 |
| `infra/postgres/init.sql`, `src/pdp/db.py`, `src/pdp/simulator/seed.py` | DDL (app + warehouse schemas), connections, COPY seeding | 3 |
| `src/pdp/simulator/live.py`, `faults.py`, `src/pdp/ops.py` | Live traffic, fault catalogue, ops tables | 4 |
| `src/pdp/loader/envelope.py` | Debezium envelope parsing | 5 |
| `src/pdp/loader/writer.py`, `batcher.py`, `app.py` | Raw writer, batching, chaos hook, consumer loop | 6 |
| `src/pdp/stream/features.py`, `rules.py`, `exposure.py`, `app.py` | Windows, rules, exposure, Quix wiring | 7 |
| `docker-compose.yml`, `docker/app.Dockerfile`, `infra/connect/postgres-cdc.json`, `infra/connect/register.sh`, `infra/contracts/*.yml`, `.devcontainer/devcontainer.json` | Runtime stack | 8 |
| `dbt/` (project, profiles, staging), `tests/dbt/fixture_builder.py`, `tests/dbt/test_staging.py` | Staging models, CDC fixtures | 9 |
| `dbt/models/intermediate`, `dbt/models/marts`, `tests/dbt/test_marts.py` | DPD, SCD2, marts, contracts, value asserts | 10 |
| `src/pdp/quality/*` | Checks, runner, alerts, MTTD | 11 |
| `src/pdp/reports/regulatory.py` | Monthly Excel report | 12 |
| `docker/airflow.Dockerfile`, `airflow/dags/*.py`, `src/pdp/tasks.py` | Airflow image, 4 DAGs, task callables | 13 |
| `src/pdp/cli.py` | Typer CLI `pdp` | 14 |
| `src/pdp/dashboard/app.py` | Streamlit dashboard | 15 |
| `src/pdp/e2e.py`, `tests/e2e/test_e2e.py`, `.github/workflows/e2e.yml`, `README.md`, `docs/data-dictionary.md` | E2E scenario, workflow, docs | 16 |

---

### Task 1: Scaffold, settings, test markers, unit CI

**Files:**
- Create: `pyproject.toml`, `.python-version` (3.11), `.gitignore`, `.env.example`, `LICENSE` (MIT, "Copyright (c) 2026 Hansen Vernandez"), `src/pdp/__init__.py`, `src/pdp/config.py`, `tests/__init__.py`, `tests/conftest.py`, `.github/workflows/ci.yml`
- Test: `tests/unit/test_config.py`, `tests/unit/test_markers.py`

**Interfaces:**
- Produces: `Settings(BaseSettings)` with `env_prefix="PDP_"` and `env_file=".env"`. Fields and defaults:
  - `app_dsn = "postgresql://pdp:pdp@localhost:5432/paylater_app"`
  - `wh_dsn = "postgresql://pdp:pdp@localhost:5432/warehouse"`
  - `kafka_bootstrap = "localhost:9092"`
  - `topic_prefix = "paylater"`
  - `pii_salt = "change-me"`
  - `alert_webhook: str | None = None`
  - `freshness_threshold_min = 10`
  - `volume_window_min = 60`
  - `volume_baseline_windows = 24`
  - `volume_z_threshold = 3.0`
  - `reconciliation_tolerance = 0.001`
  - `seed = 42`
  - `reports_dir = Path("reports")`
  - `compose_file = Path("docker-compose.yml")`
- Produces: pytest markers `pg` and `e2e`. In `tests/conftest.py`, `pg`-marked tests are skipped unless `PDP_TEST_PG_DSN` is set. Fixture `pg_conn` yields a psycopg connection to that DSN inside a transaction that is rolled back.

- [ ] **Step 1: Scaffold**

Write `pyproject.toml`:
- hatchling build, `packages=["src/pdp"]`
- script `pdp = "pdp.cli:app"`
- ruff `line-length = 100`, `select = ["E","F","I","UP","B"]`
- pytest `testpaths = ["tests"]`, markers `pg` and `e2e`

Then run:

`uv add "psycopg[binary]>=3.2" "pydantic-settings>=2.3" "typer>=0.13" "rich>=13.7" "openpyxl>=3.1" "pyyaml>=6"`

`uv add --optional loader "confluent-kafka>=2.5"`

`uv add --optional stream "quixstreams>=3.20,<4"`

`uv add --optional dashboard "streamlit>=1.50" "pandas>=2.2.2"`

`uv add --dev pytest ruff "dbt-core>=1.10" "dbt-postgres>=1.9"`

`.gitignore` covers `.venv/`, `__pycache__/`, `.env`, `reports/`, `dbt/target/`, `dbt/logs/`, `.pytest_cache/` and `.ruff_cache/`.

- [ ] **Step 2: Write failing tests**

```python
# tests/unit/test_config.py
def test_defaults(monkeypatch):
    for k in list(os.environ):
        if k.startswith("PDP_"):
            monkeypatch.delenv(k)
    s = Settings(_env_file=None)
    assert s.topic_prefix == "paylater" and s.freshness_threshold_min == 10 and s.seed == 42


def test_env_override(monkeypatch):
    monkeypatch.setenv("PDP_VOLUME_WINDOW_MIN", "1")
    assert Settings(_env_file=None).volume_window_min == 1


# tests/unit/test_markers.py
@pytest.mark.pg
def test_pg_marker_skips_without_dsn(pg_conn):
    assert pg_conn.execute("select 1").fetchone() == (1,)
```

- [ ] **Step 3: Run to verify fail**

Run: `uv run pytest -q`
Expected: FAIL (`ModuleNotFoundError: pdp.config`).

- [ ] **Step 4: Implement `config.py` and `conftest.py`**, then write `ci.yml`.

`ci.yml` runs on push and pull_request:
- Job `lint`: `uv sync --locked`, `ruff check`, `ruff format --check`.
- Job `unit`: `uv run pytest -m "not pg and not e2e" -q`.

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest -m "not pg and not e2e" -q; uv run ruff check`
Expected: config tests pass, the `pg` test is reported as skipped, and ruff reports no issues.

- [ ] **Step 6: Commit** `chore: scaffold project with settings, test markers and unit CI`.

---

### Task 2: Domain constants and deterministic history generator

**Files:**
- Create: `src/pdp/simulator/__init__.py`, `domain.py`, `generator.py`
- Test: `tests/unit/test_generator.py`

**Interfaces:**
- **`domain.py` constants:**
  - `TENORS = (1, 3, 6, 12)`, `TENOR_WEIGHTS = (0.4, 0.3, 0.2, 0.1)`.
  - `RISK_GRADES`: dict grade → `Grade(monthly_rate, default_prob, late_prob, limit_range)` with:

    | Grade | Monthly rate | Default prob | Late prob | Limit range (Rp) |
    |---|---|---|---|---|
    | A | 0.020 | 0.01 | 0.05 | 10–20 million |
    | B | 0.025 | 0.02 | 0.08 | 7–12 million |
    | C | 0.030 | 0.04 | 0.12 | 4–8 million |
    | D | 0.035 | 0.08 | 0.18 | 2–5 million |
    | E | 0.040 | 0.15 | 0.25 | 1–3 million |

  - `GRADE_WEIGHTS = (0.15, 0.25, 0.30, 0.20, 0.10)`.
  - `PROVINCES`: ≥ 15 (name, weight) pairs, weights roughly proportional to BPS population with Jawa Barat largest, summing to 1.
  - `MERCHANT_CATEGORIES` (≥ 8: electronics, fashion, groceries, travel, …).
  - `PAYMENT_METHODS = ("va", "ewallet", "retail")`, `CHANNELS = ("app", "web", "qr")`.
- **Entity dataclasses** (frozen, fields exactly as the spec §4.1 tables, `installments` including `account_id`): `User`, `CreditAccount`, `Merchant`, `Transaction`, `Installment`, `Repayment`. Ids are `uuid.UUID` built from the RNG (`uuid.UUID(int=rng.getrandbits(128), version=4)`).
- **`generator.py`:**
  - `@dataclass History(users, accounts, merchants, transactions, installments, repayments)`.
  - `generate_history(seed: int, months: int, now: datetime, n_users: int = 5000, n_merchants: int = 300, txns_per_user_month: float = 1.0) -> History`.
  - `build_installments(txn: Transaction, rng) -> list[Installment]`:
    - principal is split equally, rounded down to whole rupiah, with the remainder on the last installment;
    - interest per installment = `round(amount × monthly_rate)`;
    - `due_date` = WIB date of `created_at` + `seq` months (end-of-month clamp).
- **Behavior rules:**
  - each account samples once: `default` (stops paying from a random installment), `late` (pays each installment 1–90 days late) or `on_time`, using its grade probabilities;
  - only installments due ≤ `now` (WIB) can be paid, and a payment exists only if `paid_at ≤ now`;
  - amounts follow a lognormal with median Rp 750,000, rounded to Rp 1,000, capped at the available limit; a transaction is skipped if the available limit is < Rp 100,000;
  - transaction status is 97% approved, 2% declined, 1% refunded; only approved transactions get installments.
- `NIK` is 16 random digits; `phone` is `08` + 10 digits. Both are fake.

- [ ] **Step 1: Write failing tests**

```python
NOW = datetime(2026, 11, 6, 12, 0, tzinfo=ZoneInfo("Asia/Jakarta"))


def test_deterministic():
    assert generate_history(42, 2, NOW, n_users=200) == generate_history(42, 2, NOW, n_users=200)


def test_installments_sum_to_amount():
    h = generate_history(42, 3, NOW, n_users=300)
    by_txn = defaultdict(Decimal)
    for i in h.installments:
        by_txn[i.txn_id] += i.principal
    assert all(by_txn[t.txn_id] == t.amount for t in h.transactions if t.status == "approved")


def test_tenor_mix_and_references():
    h = generate_history(42, 12, NOW, n_users=2000)
    share = Counter(t.tenor_months for t in h.transactions)
    assert abs(share[1] / len(h.transactions) - 0.4) < 0.03
    accounts = {a.account_id for a in h.accounts}
    assert all(t.account_id in accounts for t in h.transactions)


def test_worse_grades_default_more():
    h = generate_history(42, 12, NOW, n_users=3000)
    # share of accounts with an installment >90 days overdue at NOW, grade E vs grade A
    assert overdue_share(h, "E") > 2 * overdue_share(h, "A")


def test_dates_within_window():
    h = generate_history(42, 6, NOW, n_users=300)
    assert all(NOW - relativedelta(months=6) <= t.created_at <= NOW for t in h.transactions)
```

The helper `overdue_share` lives in the test module. The plan allows a dependency on `python-dateutil`; add it to core if used.

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/unit/test_generator.py -q`
Expected: FAIL (module missing).

- [ ] **Step 3: Implement `domain.py` and `generator.py`** per Interfaces.

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit -q`
Expected: PASS, and `generate_history(42, 12, NOW)` with defaults completes in under 20 s locally.

- [ ] **Step 5: Commit** `feat: add deterministic PayLater history generator`.

---

### Task 3: Database DDL, connections and seed writer

**Files:**
- Create: `infra/postgres/init.sql`, `src/pdp/db.py`, `src/pdp/simulator/seed.py`
- Test: `tests/unit/test_init_sql.py`, `tests/pg/test_seed.py`

**Interfaces:**
- **`init.sql`**, run by the Postgres entrypoint:
  - **Roles and databases:** roles `pdp` (owner) and `pdp_cdc` (`REPLICATION LOGIN`); databases `paylater_app`, `warehouse` and `airflow`.
  - **App tables:** in `paylater_app.public`, the six tables from spec §4.1 with PKs/FKs, **no CHECK** on `amount`/`tenor_months`, and `REPLICA IDENTITY FULL` on all of them.
  - **Publication:** `CREATE PUBLICATION pdp_pub FOR TABLE <the six tables>`.
  - **Warehouse schemas:** `raw`, `staging`, `intermediate`, `marts`, `online`, `ops`.
  - **Raw tables:** `raw.cdc_<table>` with columns `op text, pk jsonb, before jsonb, after jsonb, source_ts_ms bigint, lsn bigint, kafka_topic text, kafka_partition int, kafka_offset bigint, ingested_at timestamptz default now()`, and `UNIQUE (kafka_topic, kafka_partition, kafka_offset)`. Plus `raw.cdc_dead_letter`.
  - **Online tables:** `online.risk_features`, `online.risk_alerts`, `online.account_exposure (account_id pk, exposure numeric)` and `online.account_limits (account_id pk, credit_limit numeric, updated_at)`.
  - **Ops tables:** `ops.fault_log`, `ops.dq_results`, `ops.alerts`, `ops.chaos (name text pk, enabled bool)`.
  - The script must be safe to run twice in a test database: `IF NOT EXISTS` wherever Postgres allows it.
- **`db.py`:**
  - `connect(dsn: str, autocommit: bool = False) -> psycopg.Connection`
  - `apply_sql_file(conn, path: Path) -> None`, used by tests to create the schemas inside a test database.
- **`seed.py`:**
  - `write_history(conn, history: History) -> dict[str, int]`: one `COPY ... FROM STDIN` per table, in FK order; returns row counts.
  - `seed(settings: Settings, months: int, seed: int, now: datetime | None = None) -> dict[str, int]`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_init_sql.py  (static checks, no database)
SQL = Path("infra/postgres/init.sql").read_text()


def test_publication_and_replica_identity():
    assert "CREATE PUBLICATION pdp_pub" in SQL
    for t in TABLES:
        assert f"ALTER TABLE {t} REPLICA IDENTITY FULL" in SQL


def test_raw_tables_are_idempotent_on_offset():
    assert SQL.count("UNIQUE (kafka_topic, kafka_partition, kafka_offset)") == len(TABLES)


def test_no_check_constraints_on_amount_or_tenor():
    assert not re.search(r"CHECK\s*\(\s*(amount|tenor_months)", SQL)


# tests/pg/test_seed.py
@pytest.mark.pg
def test_write_history_counts(pg_conn):
    apply_app_schema(
        pg_conn
    )  # helper: runs the paylater_app part into a scratch schema search_path
    counts = write_history(pg_conn, generate_history(42, 1, NOW, n_users=50))
    assert counts["users"] == 50
    assert (
        pg_conn.execute("select count(*) from installments").fetchone()[0] == counts["installments"]
    )
```

Ruling allowed: if one `init.sql` cannot serve both the entrypoint (multiple databases) and tests, split it into `infra/postgres/00_databases.sql`, `10_app.sql` and `20_warehouse.sql`. Tests apply only the last two.

- [ ] **Step 2: Run to verify fail**

Run: `uv run pytest tests/unit/test_init_sql.py -q`
Expected: FAIL.

- [ ] **Step 3: Implement** `init.sql` (or the split files), `db.py` and `seed.py`.

- [ ] **Step 4: Verify**

Locally: `uv run pytest -m "not pg and not e2e" -q` passes.

Before pushing, ask the user to create the GitHub repo. Then add CI job `pg` to `ci.yml`:
- service `postgres:16.15`;
- env `PDP_TEST_PG_DSN`;
- `uv run pytest -m pg -q`.

Push, and read the run. Expected: `pg` job green and `test_write_history_counts` passed.

- [ ] **Step 5: Commit** `feat: add database DDL and COPY-based seeding`.

---

### Task 4: Live traffic, fault catalogue and ops helpers

**Files:**
- Create: `src/pdp/simulator/live.py`, `src/pdp/simulator/faults.py`, `src/pdp/ops.py`
- Test: `tests/unit/test_live.py`, `tests/unit/test_faults.py`, `tests/pg/test_faults_pg.py`

**Interfaces:**
- **`live.py`:**
  - `@dataclass Action(kind: Literal["txn","repay","limit_change"], account_id, payload: dict)`.
  - `plan_tick(rng, rate: float, dt_s: float, accounts: Sequence[AccountView], due: Sequence[InstallmentView], naughty: set[UUID]) -> list[Action]`:
    - the transaction count is Poisson(`rate × dt_s`);
    - 3% of transactions come from `naughty` accounts in bursts of 5–8 within one tick, with a new `device_id` and an amount of 80–100% of the available limit;
    - each due installment is repaid with probability 0.2 per tick;
    - limit changes happen with probability 0.001 per tick.
  - `class LiveSimulator(settings, rate: float)`:
    - `run(duration: timedelta | None)` loads `AccountView`/`InstallmentView` from the app DB;
    - every 1 s it plans one tick and applies it in one transaction (insert txn + installments, mark the installment paid and insert the repayment, update the limit);
    - returns `LiveStats(txns, repayments, limit_changes)`.
- **`faults.py`:**
  - `FAULT_CHECK = {"duplicate_events": "raw_duplicate_lsn", "out_of_order": "lsn_regression", "schema_drift": "schema_drift", "invalid_amount": "invalid_values", "invalid_tenor": "invalid_values", "loader_stall": "freshness", "volume_drop": "volume_anomaly", "reconciliation_gap": "reconciliation"}`
  - `inject(fault: str, settings: Settings, **opts) -> int` returns the `fault_id`. It logs to `ops.fault_log` first, then acts:
    - **`duplicate_events`:** re-produce the last 50 messages of `paylater.public.transactions` with the same key and value (needs extra `loader`).
    - **`out_of_order`:** update one transaction's `channel` twice, then re-produce the first update's message.
    - **`schema_drift`:** `ALTER TABLE transactions ADD COLUMN IF NOT EXISTS promo_code text`, then insert 5 transactions with a promo code.
    - **`invalid_amount`:** insert 3 approved transactions with amount `0` and `-1000`.
    - **`invalid_tenor`:** insert 2 transactions with `tenor_months = 24`.
    - **`loader_stall`:** `docker compose stop loader`, wait `opts["minutes"]`, then `docker compose start loader`. The subprocess call goes through `pdp.cli.compose()` from Task 14; in tests it is stubbed by injection.
    - **`volume_drop`:** `docker compose stop simulator` for `opts["minutes"]`, then start it again.
    - **`reconciliation_gap`:** `UPDATE ops.chaos SET enabled = true WHERE name = 'drop_next_batch'`.
- **`ops.py`:**
  - `log_fault(conn, fault_type, details) -> int`
  - `set_chaos(conn, name, enabled)`
  - `chaos_enabled(conn, name) -> bool`

- [ ] **Step 1: Write failing tests**

```python
def test_tick_rate_matches_poisson_mean():
    rng = random.Random(1)
    accts = [view(i) for i in range(500)]
    counts = [
        sum(a.kind == "txn" for a in plan_tick(rng, 5.0, 1.0, accts, [], set()))
        for _ in range(2000)
    ]
    assert 4.8 < statistics.mean(counts) < 5.2


def test_naughty_bursts_trigger_velocity_shape():
    acts = plan_tick(random.Random(3), 50.0, 1.0, accts, [], naughty={accts[0].account_id})
    burst = [a for a in acts if a.account_id == accts[0].account_id and a.kind == "txn"]
    assert len(burst) == 0 or 5 <= len(burst) <= 8


def test_every_fault_maps_to_a_known_check():
    assert set(FAULT_CHECK) == set(FAULTS) and set(FAULT_CHECK.values()) <= set(CHECK_TYPES)
```

`CHECK_TYPES` is exported by `pdp.quality` in Task 11. Until then, define it as a constant tuple in `faults.py` and re-export it from `pdp.quality` later. Record this in the ledger.

Also add `tests/pg/test_faults_pg.py`:
- `invalid_amount` writes 3 rows plus 1 `ops.fault_log` row;
- `reconciliation_gap` flips `ops.chaos`.

- [ ] **Step 2: Run to verify fail**, then **Step 3: Implement**, then **Step 4: Verify**.

Run unit tests locally; push and read the `pg` job. Expected: both green.

- [ ] **Step 5: Commit** `feat: add live traffic simulator and fault catalogue`.

---

### Task 5: Debezium envelope parsing

**Files:**
- Create: `src/pdp/loader/__init__.py`, `src/pdp/loader/envelope.py`
- Test: `tests/unit/test_envelope.py`, plus fixtures `tests/fixtures/debezium/*.json`. These are real-shaped payloads with `value.converter.schemas.enable=false`: `create.json`, `update.json`, `delete.json`, `snapshot.json`, `drift.json` and `malformed.json`.

**Interfaces:**
- `@dataclass(frozen=True) CdcEvent(table: str, op: Literal["c","u","d","r"], pk: dict, before: dict | None, after: dict | None, source_ts_ms: int, lsn: int)`
- `class DeadLetter(Exception)` carrying `reason`.
- `parse(topic: str, key: bytes | None, value: bytes | None) -> CdcEvent | None`:
  - returns `None` for a tombstone (`value is None`);
  - raises `DeadLetter` for invalid JSON or missing `op`/`source`;
  - `table` = the last topic segment;
  - `lsn` = `source.lsn`.
- `decode_row(table: str, row: dict) -> dict` converts values to Python types by column, from a column-type map per table:
  - numeric strings → `Decimal`;
  - `date` ints → `date(1970,1,1) + timedelta(days=n)`;
  - ISO timestamps → aware `datetime`;
  - unknown keys are passed through unchanged.

  Used by stream and quality. The loader stores the raw JSON unchanged.

- [ ] **Step 1: Write failing tests**

```python
def test_parse_create():
    ev = parse("paylater.public.transactions", KEY, load("create.json"))
    assert ev.op == "c" and ev.table == "transactions" and ev.before is None and ev.lsn > 0


def test_parse_encodings_exactly():  # Review Focus 1
    row = decode_row("installments", parse(T_INST, KEY, load("snapshot.json")).after)
    assert row["principal"] == Decimal("1000000.00") and row["due_date"] == date(2026, 6, 30)
    assert row["paid_at"] is None or row["paid_at"].tzinfo is not None


def test_tombstone_is_ignored():
    assert parse(T_TXN, KEY, None) is None


def test_unknown_fields_are_preserved():  # Review Focus 5
    ev = parse(T_TXN, KEY, load("drift.json"))
    assert ev.after["promo_code"] == "HARBOLNAS" and "promo_code" in decode_row(
        "transactions", ev.after
    )


def test_malformed_goes_to_dead_letter():
    with pytest.raises(DeadLetter):
        parse(T_TXN, KEY, b"{not json")
```

- [ ] **Steps 2–4:** verify fail, implement, verify pass (`uv run pytest tests/unit/test_envelope.py -q`).

- [ ] **Step 5: Commit** `feat: parse Debezium CDC envelopes`.

---

### Task 6: Raw loader — writer, batching, chaos hook, consumer loop

**Files:**
- Create: `src/pdp/loader/writer.py`, `src/pdp/loader/batcher.py`, `src/pdp/loader/app.py`
- Test: `tests/unit/test_batcher.py`, `tests/pg/test_writer.py`

**Interfaces:**
- **`batcher.py`:** `class Batcher(max_size=500, max_wait_s=5.0)` with:
  - `add(msg) -> bool` (True when the batch should flush);
  - `due(now) -> bool`;
  - `drain() -> list`.
- **`writer.py`:**
  - `write_batch(conn, messages: list[RawMessage]) -> WriteResult(inserted: int, dead_letters: int)`.
  - **Normal path:** one transaction; per table, `INSERT ... ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING`. Parse failures go to `raw.cdc_dead_letter`. Tombstones are skipped.
  - **Chaos path:** if `chaos_enabled(conn, "drop_next_batch")`, discard the batch, reset the flag, and return `inserted=0`. This hook exists for the `reconciliation_gap` fault.
  - `RawMessage(topic, partition, offset, key: bytes|None, value: bytes|None)`.
- **`app.py`:** `run(settings)`:
  - confluent-kafka consumer, group `pdp-raw-loader`, subscription `^paylater\.public\..*`, `enable.auto.commit=false`;
  - poll → batcher → `write_batch` → `consumer.commit(asynchronous=False)` only after `write_batch` returns;
  - when the database errors, back off 1, 2, 4… up to 30 s without committing.

- [ ] **Step 1: Write failing tests**

```python
def test_batcher_flushes_on_size_or_time():
    b = Batcher(max_size=3, max_wait_s=5)
    assert [b.add(i) for i in range(3)] == [False, False, True]
    b.drain()
    b.add(1)
    assert b.due(time.monotonic() + 6)


@pytest.mark.pg
def test_writer_is_idempotent_on_redelivery(wh_conn):  # Review Focus 2
    msgs = [raw_msg("transactions", offset=o, value=load("create.json")) for o in range(10)]
    assert write_batch(wh_conn, msgs).inserted == 10
    assert write_batch(wh_conn, msgs).inserted == 0
    assert count(wh_conn, "raw.cdc_transactions") == 10


@pytest.mark.pg
def test_dead_letter_and_chaos_drop(wh_conn):
    assert write_batch(wh_conn, [raw_msg("transactions", 1, b"{bad")]).dead_letters == 1
    set_chaos(wh_conn, "drop_next_batch", True)
    assert write_batch(wh_conn, [raw_msg("transactions", 2, load("create.json"))]).inserted == 0
    assert not chaos_enabled(wh_conn, "drop_next_batch")
```

`wh_conn` is a fixture in `tests/conftest.py` that applies the warehouse DDL to the test DSN.

- [ ] **Steps 2–4:** verify fail, implement, verify (unit locally; `pg` job in CI).

- [ ] **Step 5: Commit** `feat: add idempotent raw CDC loader`.

---

### Task 7: Stream — windows, rules, exposure, Quix wiring

**Files:**
- Create: `src/pdp/stream/__init__.py`, `features.py`, `rules.py`, `exposure.py`, `app.py`
- Test: `tests/unit/test_features.py`, `tests/unit/test_rules.py`, `tests/unit/test_exposure.py`

**Interfaces:**
- **`features.py`:**
  - `@dataclass AccountWindow(events: deque[tuple[int,Decimal]], devices: dict[str,int])` (timestamps in ms).
  - `update(state: AccountWindow, ts_ms: int, amount: Decimal, device_id: str) -> Features`:
    - prunes events older than 1 h and devices older than 30 days;
    - `is_new_device` is True when the device was not in `devices` before this event.
  - `Features(txn_count_10m, txn_amount_10m, txn_count_1h, txn_amount_1h, is_new_device)`.
  - `to_state(w) -> dict` / `from_state(d) -> AccountWindow`, JSON-serialisable for Quix `State`.
- **`rules.py`:**
  - `evaluate(f: Features, amount: Decimal, credit_limit: Decimal | None, exposure: Decimal | None) -> list[str]` returns rule ids `RR1_velocity`, `RR2_near_limit`, `RR3_new_device_high` and `RR4_utilization`, with the thresholds from spec §4.4.
  - `utilization(exposure, limit) -> float | None`.
- **`exposure.py`:**
  - `exposure_delta(event: CdcEvent) -> tuple[UUID, Decimal] | None`:
    - `+amount` for a transaction with `op ∈ {c,r}` and status approved;
    - `−amount` for an approved → refunded update;
    - `−principal` for an installment with `op = r` and status paid, or an update from unpaid to paid;
    - otherwise `None`.
  - `apply_delta(conn, account_id, delta)` upserts into `online.account_exposure`.
- **`app.py`:**
  - Quix `Application(broker_address, consumer_group="pdp-risk", auto_offset_reset="earliest")`.
  - **Transactions topic:**
    - filter to approved, `op c/r`;
    - `group_by("account_id")`;
    - stateful apply: `update` → upsert `online.risk_features`;
    - only for `op = c`: `evaluate` with the limit from `LimitCache`, a 60 s TTL read of `online.account_limits` plus `online.account_exposure`;
    - emit `{alert_id, account_id, txn_id, rules, amount, latency_ms = now_ms − source_ts_ms}` to `risk.alerts` and `online.risk_alerts`.
  - **Installments topic:** `exposure_delta` → `apply_delta`.
  - **Transactions topic, second branch:** the same exposure handling.
  - **credit_accounts topic:** upsert `online.account_limits`.
  - Only explicitly named fields are read, so unknown fields are ignored.

- [ ] **Step 1: Write failing tests**

```python
def test_windows_count_and_prune():
    w = AccountWindow(deque(), {})
    for i in range(5):
        f = update(w, 1_000 + i * 60_000, Decimal("100000"), "d1")
    assert f.txn_count_10m == 5 and f.txn_count_1h == 5
    f = update(w, 1_000 + 11 * 60_000, Decimal("1"), "d1")
    assert f.txn_count_10m == 5 and f.txn_count_1h == 6  # first event (t=0) left the 10-min window


def test_new_device_flag_once():
    w = AccountWindow(deque(), {})
    assert update(w, 0, D1, "x").is_new_device and not update(w, 1000, D1, "x").is_new_device


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        (dict(count10=5), {"RR1_velocity"}),
        (dict(amount="850000", limit="1000000"), {"RR2_near_limit"}),
        (dict(new_device=True, amount="2500000", limit="20000000"), {"RR3_new_device_high"}),
        (dict(exposure="960000", limit="1000000", amount="10000"), {"RR4_utilization"}),
    ],
)
def test_rules(kwargs, expected): ...


def test_rules_without_limit_skip_limit_rules():
    assert evaluate(feat(count10=1), Decimal("5000000"), None, None) == []


def test_exposure_deltas():
    assert exposure_delta(ev_txn("c", "approved", "300000"))[1] == Decimal("300000")
    assert exposure_delta(ev_inst_paid(principal="100000"))[1] == Decimal("-100000")
    assert exposure_delta(ev_txn("c", "declined", "1")) is None


def test_stream_ignores_unknown_fields():  # Review Focus 5
    assert exposure_delta(ev_txn("c", "approved", "1", extra={"promo_code": "X"}))[1] == Decimal(
        "1"
    )
```

- [ ] **Steps 2–4:** verify fail, implement, verify (`uv run pytest tests/unit -q`). `app.py` is not unit-tested; it is covered by e2e in Task 16.

- [ ] **Step 5: Commit** `feat: add real-time risk features, rules and exposure tracking`.

---

### Task 8: Runtime stack — compose, images, Debezium, contracts, devcontainer

**Files:**
- Create: `docker-compose.yml`, `docker/app.Dockerfile`, `infra/connect/postgres-cdc.json`, `infra/connect/register.sh`, `infra/contracts/{users,credit_accounts,merchants,transactions,installments,repayments}.yml`, `.devcontainer/devcontainer.json`, `.env.example` (extend)
- Modify: `.github/workflows/ci.yml` (lint job: `docker compose config -q` for both profiles)
- Test: `tests/unit/test_stack_config.py`

**Interfaces:**
- **Compose services** (profiles and `mem_limit` per spec §5.1):

  | Service | Profile | Notes |
  |---|---|---|
  | `postgres` | core | `postgres:16.15`, command `-c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4`, init scripts mounted |
  | `kafka` | core | `apache/kafka:4.3.2`, KRaft single node, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` |
  | `connect` | core | `quay.io/debezium/connect:3.7.0.Final` |
  | `connect-init` | core | one-shot: waits for connect REST, PUTs `postgres-cdc.json`, retries 30× every 5 s |
  | `simulator`, `loader`, `stream` | core | `build: docker/app.Dockerfile`, commands `pdp live`, `python -m pdp.loader.app`, `python -m pdp.stream.app` |
  | `airflow` | full | built in Task 13; placeholder service added there |
  | `dashboard` | full | `streamlit run src/pdp/dashboard/app.py` |

  Every long-running service has a healthcheck.
- **`postgres-cdc.json`:** connector class `io.debezium.connector.postgresql.PostgresConnector`, with:
  - `plugin.name=pgoutput`, `topic.prefix=paylater`, `publication.name=pdp_pub`, `publication.autocreate.mode=disabled`, `slot.name=pdp_slot`;
  - `table.include.list` = the six tables;
  - JSON key/value converters with `schemas.enable=false`;
  - `decimal.handling.mode=string`, `snapshot.mode=initial`, `tombstones.on.delete=true`.
- **Contracts YAML:** `table`, `columns: [names]`, generated once from the DDL and then maintained by hand.
- **Devcontainer:**
  - image `mcr.microsoft.com/devcontainers/python:3.11`;
  - features docker-in-docker and uv;
  - `hostRequirements: {cpus: 4, memory: "16gb"}`;
  - `postCreateCommand: uv sync --all-extras`;
  - `forwardPorts: [8080, 8501]`.

- [ ] **Step 1: Write failing tests**

```python
COMPOSE = yaml.safe_load(Path("docker-compose.yml").read_text())


def test_profiles_and_memory_limits():
    core = {n for n, s in COMPOSE["services"].items() if "core" in s.get("profiles", [])}
    assert {"postgres", "kafka", "connect", "connect-init", "simulator", "loader", "stream"} <= core
    assert all("mem_limit" in s for s in COMPOSE["services"].values())


def test_images_are_pinned():
    assert all(
        ":" in s["image"] and not s["image"].endswith(":latest")
        for s in COMPOSE["services"].values()
        if "image" in s
    )


def test_contracts_match_ddl():
    for t in TABLES:
        assert set(
            yaml.safe_load(Path(f"infra/contracts/{t}.yml").read_text())["columns"]
        ) == ddl_columns(t)


def test_connector_config():
    c = json.loads(Path("infra/connect/postgres-cdc.json").read_text())["config"]
    assert c["decimal.handling.mode"] == "string" and c["publication.name"] == "pdp_pub"
```

`ddl_columns(t)` parses the `CREATE TABLE` block in the app DDL with a regex.

- [ ] **Steps 2–4:** verify fail, implement, verify unit tests locally. Push and read CI: lint job runs `docker compose --profile core config -q` and `--profile full config -q`; expected green.

- [ ] **Step 5: Commit** `feat: add docker compose stack, Debezium connector and devcontainer`.

---

### Task 9: dbt project, staging models and CDC fixtures

**Files:**
- Create: `dbt/dbt_project.yml` (profile `pdp`), `dbt/profiles.yml` (reads `PDP_DBT_HOST/PORT/USER/PASSWORD/DB` via `env_var`, schema `staging` by default, custom schema macro so models land in `staging`/`intermediate`/`marts` exactly), `dbt/models/sources.yml`, `dbt/models/staging/stg_<table>.sql` ×6 + `staging.yml`, `dbt/macros/latest_cdc_version.sql`, `dbt/macros/hash_pii.sql`, `tests/dbt/fixture_builder.py`, `tests/dbt/test_staging.py`
- Modify: `ci.yml`, whose `pg` job loads fixtures, then `dbt build --select staging`, then `pytest tests/dbt -m pg`

**Interfaces:**
- **`latest_cdc_version(table, pk)` macro:** keeps the row with the highest `lsn` per PK (tie-break on `source_ts_ms`, then `kafka_offset`) and drops it when its latest `op = 'd'`. It reads `coalesce(after, before)` and casts columns explicitly; unknown keys are never selected.
- **`hash_pii(col)` macro:** `encode(sha256(convert_to('{{ var("pii_salt") }}' || col, 'UTF8')), 'hex')`.
- **`stg_users`:** columns `user_id, nik_hash, phone_hash, province, city, kyc_status, created_at, updated_at`. `full_name` is dropped, and the PII columns carry `meta: {pii: true}`.
- **`fixture_builder.py`:**
  - `build_fixture() -> dict[str, list[RawRow]]` is a hand-written scenario.
  - **Accounts and users:** 3 users, 3 accounts. Account A1's limit goes 5,000,000 → 7,500,000 at 2026-08-01 WIB.
  - **Transactions:**
    - **T1:** 3,000,000, tenor 3, none paid.
    - **T2:** 1,200,000, tenor 3, first installment paid. Includes a late duplicate `T2_v1_late` with lower `lsn` and higher offset.
    - **T3:** 6,000,000, tenor 6, first two installments paid.
  - **Deletes:** repayment `R3` is created and then deleted (`op=d`).
  - **WIB edge:** one transaction `T4` at 2026-10-31 23:30 WIB, amount 500,000, tenor 1.
  - `load_fixture(conn)` inserts the rows into `raw.cdc_*`.

- [ ] **Step 1: Write failing tests**

```python
@pytest.mark.pg
def test_staging_picks_latest_lsn(dbt_built):  # Review Focus 3
    assert (
        q(dbt_built, "select channel from staging.stg_transactions where txn_id = :t2") == "web"
    )  # v2, not late v1


@pytest.mark.pg
def test_deleted_rows_are_gone(dbt_built):
    assert q(dbt_built, "select count(*) from staging.stg_repayments where repayment_id = :r3") == 0


@pytest.mark.pg
def test_pii_is_hashed(dbt_built):
    nik_hash = q(dbt_built, "select nik_hash from staging.stg_users limit 1")
    assert len(nik_hash) == 64 and not nik_hash.isdigit()
```

`dbt_built` is a fixture that runs `load_fixture` plus `dbt build --select staging` once per session (subprocess, `--target ci`).

- [ ] **Step 2:** locally `uv run dbt parse --project-dir dbt --profiles-dir dbt` fails (no models yet).

- [ ] **Step 3:** implement the models and macros.

- [ ] **Step 4:** locally `dbt parse` succeeds. Push and read CI: the `pg` job runs `dbt build --select staging` (all tests pass), then `pytest tests/dbt -m pg`; expected green.

- [ ] **Step 5: Commit** `feat: add dbt staging models with CDC dedup and PII hashing`.

---

### Task 10: dbt intermediate models, marts, contracts and value assertions

**Files:**
- Create: `dbt/models/intermediate/int_installment_dpd.sql`, `int_credit_limit_history.sql`, `dbt/models/marts/{dim_user,dim_merchant,dim_date,fct_transactions,fct_installments,fct_repayments,mart_portfolio_daily,mart_delinquency,mart_collections,mart_merchant_performance}.sql` + `marts.yml` (contracts enforced, tests), `dbt/tests/{installments_sum_to_amount,repayment_not_above_due}.sql`, `tests/dbt/test_marts.py`

**Interfaces:**
- **Variable `as_of_date`:** defaults to the current WIB date; CI passes `--vars '{as_of_date: 2026-10-31}'`.
- **`int_installment_dpd`:** one row per installment and as-of date, for each date in `dim_date` between the first due date and `as_of_date`.
  - `dpd = greatest(0, as_of − due_date)` while unpaid at that date, otherwise 0.
  - Materialised incrementally by date.
- **`int_credit_limit_history`:** `account_id, credit_limit, valid_from, valid_to` (null = current), derived from CDC before/after pairs.
- **`mart_portfolio_daily`:** per WIB date: `outstanding_principal`, `disbursed_amount`, `active_borrowers`, `txn_count`.
- **`mart_delinquency`:** per WIB date and province: bucket amounts `current`, `dpd_1_30`, `dpd_31_60`, `dpd_61_90`, `dpd_90_plus`, plus `twp90 = dpd_90_plus / nullif(total_outstanding, 0)`. A transaction is bucketed by the maximum DPD over its unpaid installments.
- **Contracts:** every mart has `contract: {enforced: true}` with explicit `data_type`s.
- **Generic tests:**
  - `accepted_values` for tenor {1,3,6,12} and statuses;
  - `relationships` from facts to dimensions;
  - singular tests from Files.

- [ ] **Step 1: Write failing tests** (expected values come from the Task 9 fixture, as of 2026-10-31)

```python
@pytest.mark.pg
def test_twp90_matches_hand_calculation(marts):
    # T1 unpaid since 2026-06-30 (DPD 123): 3,000,000; T3 installment 3 due 2026-07-31 (DPD 92): 4,000,000 outstanding;
    # T2 current: 800,000; T4 current: 500,000  -> 7,000,000 / 8,300,000
    assert q(
        marts,
        "select round(sum(dpd_90_plus)/sum(total_outstanding), 4) from marts.mart_delinquency "
        "where as_of_date = '2026-10-31'",
    ) == Decimal("0.8434")


@pytest.mark.pg
def test_scd2_limit_history(marts):
    assert rows(
        marts,
        "select credit_limit, valid_to is null from intermediate.int_credit_limit_history "
        "where account_id = :a1 order by valid_from",
    ) == [(Decimal("5000000"), False), (Decimal("7500000"), True)]


@pytest.mark.pg
def test_wib_month_boundary(marts):  # Review Focus 4
    assert q(
        marts,
        "select disbursed_amount from marts.mart_portfolio_daily where as_of_date = '2026-10-31'",
    ) >= Decimal("500000")
```

Recompute `0.8434` once the fixture is final: (3,000,000 + 4,000,000) / (3,000,000 + 800,000 + 4,000,000 + 500,000) = 0.84337 → `0.8434`.

- [ ] **Steps 2–4:**
  - Locally: `dbt parse` passes.
  - In CI, the `pg` job runs `dbt build --vars '{as_of_date: 2026-10-31}'` (all models and tests) and then `pytest tests/dbt -m pg`; expected green.

- [ ] **Step 5: Commit** `feat: add delinquency, TWP90 and portfolio marts with contracts`.

---

### Task 11: Data-quality checks, runner, alerts, MTTD

**Files:**
- Create: `src/pdp/quality/__init__.py` (exports `CHECK_TYPES`, `run_checks`), `checks.py`, `drift.py`, `reconcile.py`, `stats.py`, `alerts.py`, `mttd.py`
- Test: `tests/unit/test_quality_logic.py`, `tests/pg/test_checks_pg.py`

**Interfaces:**
- `CHECK_TYPES = ("freshness","raw_duplicate_lsn","lsn_regression","schema_drift","invalid_values","volume_anomaly","reconciliation")`.
- `@dataclass CheckResult(check_type, target, status: Literal["pass","warn","fail","error"], observed: str, expected: str)`.
- **`stats.py`:**
  - `zscore(value: float, baseline: Sequence[float]) -> float | None` returns `None` when there are fewer than 3 points or the standard deviation is 0;
  - `pct_diff(a, b) -> float`.
- **`drift.py`:** `field_diff(observed: set[str], contract: set[str]) -> tuple[set, set]` returns (added, removed).
- **`checks.py`:** each check is a function `(settings, wh_conn, app_conn) -> list[CheckResult]` implementing spec §4.7 with the thresholds from `Settings`:
  - `volume_anomaly` buckets `raw.cdc_transactions` (`op = 'c'`) into `settings.volume_window_min` windows and compares the last complete window with the previous `volume_baseline_windows`;
  - `reconciliation` compares app counts and Σ amount per table with staging at the same `as_of` (the latest `source_ts_ms` seen in raw);
  - `schema_drift` compares the union of `after` keys over the last 15 minutes of each raw table with `infra/contracts/<table>.yml`.
- **`run_checks(settings, checks: Sequence[str] | None = None) -> list[CheckResult]`:**
  - runs each check independently; an exception becomes status `error` with the message;
  - writes every result to `ops.dq_results`;
  - calls `alerts.raise_alert` for each `fail`.
- **`alerts.py`:** `raise_alert(conn, result, webhook: str | None)` inserts into `ops.alerts`. When `webhook` is set, it POSTs `{"text": ...}` with `urllib` and a 5 s timeout; webhook errors are swallowed and logged.
- **`mttd.py`:** `compute_mttd(conn) -> list[MttdRow(fault_id, fault_type, check_type, injected_at, detected_at | None, mttd_seconds | None)]`. It joins `ops.fault_log` and `ops.dq_results` via `FAULT_CHECK`, taking the first `fail`/`warn` at or after `injected_at`.

- [ ] **Step 1: Write failing tests**

```python
def test_zscore():
    assert zscore(10, [100, 102, 98, 101]) < -3 and zscore(5, [5, 5, 5]) is None
def test_field_diff():
    assert field_diff({"a", "b", "promo_code"}, {"a", "b", "c"}) == ({"promo_code"}, {"c"})
def test_runner_isolates_exceptions(monkeypatch):
    monkeypatch.setitem(CHECKS, "freshness", lambda *a: 1 / 0)
    results = run_checks_offline(["freshness", "invalid_values"], fake_conns())
    assert {r.check_type: r.status for r in results}["freshness"] == "error"

@pytest.mark.pg
def test_schema_drift_detects_new_field(wh):                   # Review Focus 5
    insert_raw(wh, "transactions", after={**TXN, "promo_code": "X"})
    assert [r.status for r in check_schema_drift(S, wh, None) if r.target == "transactions"] == ["fail"]
@pytest.mark.pg
def test_invalid_values_and_duplicates(wh): ...
@pytest.mark.pg
def test_freshness_fails_when_stale(wh): ...                    # ingested_at = now() - 15 min
@pytest.mark.pg
def test_mttd_pairs_fault_with_first_detection(wh):
    fid = log_fault(wh, "invalid_amount", {}); write_result(wh, "invalid_values", "fail", at=+70s)
    assert compute_mttd(wh)[0].mttd_seconds == pytest.approx(70, abs=1)
```

- [ ] **Steps 2–4:** verify fail, implement, verify (unit locally; `pg` CI green).

- [ ] **Step 5: Commit** `feat: add data-quality checks, alerts and MTTD`.

---

### Task 12: Monthly regulatory report

**Files:**
- Create: `src/pdp/reports/__init__.py`, `src/pdp/reports/regulatory.py`
- Test: `tests/unit/test_regulatory.py`, `tests/pg/test_regulatory_pg.py`

**Interfaces:**
- `month_window(month: str) -> tuple[date, date]` returns the first and last WIB date (`"2026-10"` → 2026-10-01 … 2026-10-31).
- `@dataclass ReportData(month, summary: dict, by_province: list[dict], dpd_buckets: dict)`.
- `load_report_data(wh_conn, month) -> ReportData`, read from the marts at the last day of the month.
- `validate(data) -> list[str]` lists mismatches: the province sum must equal the summary outstanding, and the bucket sum must equal the summary outstanding.
- `write_excel(data, path) -> Path`:
  - sheets `Summary`, `By province`, `DPD buckets`;
  - a header note "Inspired by OJK indicators, not an official format";
  - money cells formatted `#,##0`, TWP90 as `0.00%`.
- `build_report(settings, month) -> Path` raises `ValueError` listing the mismatches when `validate` fails.

- [ ] **Step 1: Write failing tests**

```python
def test_report_month_window_uses_wib():  # Review Focus 4
    assert month_window("2026-10") == (date(2026, 10, 1), date(2026, 10, 31))


def test_validate_catches_province_mismatch():
    assert validate(sample(by_province_total=99)) == [
        "by_province total 99 != summary outstanding 100"
    ]


def test_excel_layout(tmp_path):
    p = write_excel(sample(), tmp_path / "r.xlsx")
    wb = openpyxl.load_workbook(p)
    assert wb.sheetnames == ["Summary", "By province", "DPD buckets"]
    assert "not an official format" in wb["Summary"]["A1"].value
```

- [ ] **Steps 2–4:** verify fail, implement, verify. The `pg` test runs `build_report` for 2026-10 on the Task 10 fixture marts, and `validate` must return `[]`.

- [ ] **Step 5: Commit** `feat: add monthly regulatory report`.

---

### Task 13: Airflow image, task callables and four DAGs

**Files:**
- Create: `docker/airflow.Dockerfile`, `src/pdp/tasks.py`, `airflow/dags/{pdp_quality_5min,pdp_hourly,pdp_daily_dq,pdp_monthly_regulatory}.py`
- Modify: `docker-compose.yml` (service `airflow`, profile `full`)
- Test: `tests/unit/test_tasks.py`, `tests/unit/test_dag_files.py`

**Interfaces:**
- **`airflow.Dockerfile`:**
  - `FROM apache/airflow:3.3.2-python3.11`;
  - `pip install` the `pdp` core package with Airflow's constraints file `constraints-3.3.2/constraints-3.11.txt`;
  - dbt in a separate venv: `python -m venv /opt/dbt && /opt/dbt/bin/pip install dbt-postgres==1.11.*`;
  - copies `dbt/` to `/opt/pdp/dbt` and `airflow/dags` to `/opt/airflow/dags`.
- **Compose `airflow` service:**
  - command `airflow standalone`;
  - env `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://pdp:pdp@postgres/airflow`, `AIRFLOW__CORE__LOAD_EXAMPLES=false`, `AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Jakarta`;
  - port 8080, healthcheck `airflow jobs check`, `mem_limit 2g`.
- **`tasks.py`**, plain functions called by the DAGs, each building `Settings()` itself:
  - `quality_frequent()` runs `run_checks(["freshness","raw_duplicate_lsn","lsn_regression","schema_drift","invalid_values"])`;
  - `quality_daily()` runs `run_checks(["reconciliation","volume_anomaly"])` and writes a summary row;
  - `dbt_results_to_ops(run_results_path)` maps each dbt test result to an `ops.dq_results` row with check type `dbt_test`;
  - `regulatory_previous_month(logical_date)` calls `build_report` for the month before `logical_date` in WIB.
- **DAGs** (Airflow 3 `airflow.sdk` `@dag`/`@task`; bash via `airflow.providers.standard.operators.bash.BashOperator`; `catchup=False`; retries 2 with a 1-minute delay; `on_failure_callback` writes to `ops.alerts`):

  | DAG | Schedule | Tasks |
  |---|---|---|
  | `pdp_quality_5min` | `*/5 * * * *` | `quality_frequent` |
  | `pdp_hourly` | `0 * * * *` | `quality_frequent` → `BashOperator("/opt/dbt/bin/dbt build --project-dir /opt/pdp/dbt --profiles-dir /opt/pdp/dbt")` → `dbt_results_to_ops` |
  | `pdp_daily_dq` | `0 1 * * *` | `quality_daily` |
  | `pdp_monthly_regulatory` | `0 2 1 * *` | `regulatory_previous_month` |

- [ ] **Step 1: Write failing tests**

```python
def test_regulatory_previous_month_uses_wib(monkeypatch):
    called = {}
    monkeypatch.setattr(tasks, "build_report", lambda s, m: called.setdefault("m", m))
    tasks.regulatory_previous_month(
        datetime(2026, 11, 1, 19, 0, tzinfo=UTC)
    )  # 2026-11-02 02:00 WIB
    assert called["m"] == "2026-10"


def test_dbt_results_mapping(tmp_path): ...  # sample run_results.json -> rows
def test_dag_files_only_call_pdp_tasks():
    for f in Path("airflow/dags").glob("*.py"):
        tree = ast.parse(f.read_text())
        assert "pdp.tasks" in f.read_text() and compile(tree, f, "exec")
```

DAG import with real Airflow is verified in e2e (Task 16) with `airflow dags list-import-errors`; it must print no errors.

- [ ] **Steps 2–4:** verify fail, implement, verify unit tests locally. CI lint still runs `docker compose --profile full config -q`.

- [ ] **Step 5: Commit** `feat: add Airflow image, task callables and four DAGs`.

---

### Task 14: CLI `pdp`

**Files:**
- Create: `src/pdp/cli.py`
- Test: `tests/unit/test_cli.py`

**Interfaces:**
- **Shared helper:** `compose(*args: str, profile: str | None = None, check: bool = True) -> subprocess.CompletedProcess` builds `["docker","compose","-f",settings.compose_file,("--profile",profile)?, *args]`. Tests monkeypatch the runner.
- **Commands:**

  | Command | Calls |
  |---|---|
  | `up --profile core\|full` | `compose("up","-d","--build","--wait", profile=...)` |
  | `down [--volumes]` | `compose("down")` |
  | `seed --months 12 --seed 42` | `simulator.seed.seed` |
  | `live --rate 5 --duration 10m` | `LiveSimulator.run` |
  | `fault <type> [--minutes N]` | `faults.inject` (the type is a Typer enum built from `FAULT_CHECK` keys) |
  | `quality run [--checks a,b]` | `run_checks` |
  | `dbt-build [--as-of DATE]` | `dbt build` subprocess with the project/profile dirs |
  | `report --month YYYY-MM` | `build_report` |
  | `status` | Rich table: compose service health, consumer lag (`kafka-consumer-groups.sh` via `compose exec kafka`), freshness per raw table, open alerts |
  | `e2e` | `pdp.e2e.run` (Task 16) |

- **Error handling:** the CLI exits 1 with a short message on `subprocess.CalledProcessError`, suggesting `docker compose logs <service>`.

- [ ] **Step 1: Write failing tests**

```python
def test_up_builds_compose_command(monkeypatch):
    seen = []
    monkeypatch.setattr(cli, "_run", lambda cmd, **k: seen.append(cmd) or ok())
    assert runner.invoke(app, ["up", "--profile", "core"]).exit_code == 0
    assert seen[0][-6:] == ["--profile", "core", "up", "-d", "--build", "--wait"]


def test_unknown_fault_is_usage_error():
    assert runner.invoke(app, ["fault", "nope"]).exit_code == 2


def test_failed_compose_exits_1(monkeypatch): ...
```

- [ ] **Steps 2–4:** verify fail, implement, verify.

- [ ] **Step 5: Commit** `feat: add pdp CLI`.

---

### Task 15: Streamlit dashboard

**Files:**
- Create: `src/pdp/dashboard/__init__.py`, `src/pdp/dashboard/queries.py`, `src/pdp/dashboard/app.py`
- Test: `tests/unit/test_dashboard_queries.py`, `tests/pg/test_dashboard_pg.py`

**Interfaces:**
- **`queries.py`** holds the SQL strings and small functions `(conn) -> list[dict]`: `pipeline_health`, `recent_alerts(limit=50)`, `latency_percentiles` (p50/p95 of `online.risk_alerts.latency_ms` over the last hour), `mttd_table`, `portfolio_kpis(as_of)`, `twp90_trend(days=90)`, `by_province(as_of)`.
- **`app.py`:**
  - three pages through `st.navigation`: "Pipeline health", "Risk alerts", "Portfolio";
  - a short-lived connection per render;
  - when the warehouse is unreachable, it shows "Warehouse not reachable — is `pdp up` running?";
  - it never selects `nik`, `phone` or `full_name`.

- [ ] **Step 1: Write failing tests**

```python
def test_queries_never_select_pii():
    for sql in ALL_SQL:
        assert not re.search(r"\b(nik|phone|full_name)\b", sql)


@pytest.mark.pg
def test_dashboard_renders(marts_db, monkeypatch):
    monkeypatch.setenv("PDP_WH_DSN", marts_db)
    at = AppTest.from_file("src/pdp/dashboard/app.py", default_timeout=60).run()
    assert not at.exception
```

- [ ] **Steps 2–4:** verify fail, implement, verify (unit locally; `pg` CI green).

- [ ] **Step 5: Commit** `feat: add Streamlit dashboard`.

---

### Task 16: End-to-end scenario, e2e workflow, README

**Files:**
- Create: `src/pdp/e2e.py`, `tests/e2e/test_e2e.py`, `.github/workflows/e2e.yml`, `README.md`, `docs/data-dictionary.md`

**Interfaces:**
- **`e2e.run(settings, minutes_live=12) -> E2EResult`**, with env overrides `PDP_FRESHNESS_THRESHOLD_MIN=2`, `PDP_VOLUME_WINDOW_MIN=1`, `PDP_VOLUME_BASELINE_WINDOWS=10`. Sequence:
  1. `pdp up --profile full`; then `pdp seed --months 3`; then wait until `raw.cdc_transactions` count ≥ seeded count (timeout 10 min).
  2. Start `live` at rate 5 in the `simulator` container. Run `quality run` every 60 s in a background thread for the whole scenario.
  3. At minute 3, inject `invalid_amount`, `invalid_tenor` and `schema_drift`. At minute 4, inject `duplicate_events` and `out_of_order`.
  4. At minute 5, inject `loader_stall` for 3 minutes. At minute 9, inject `volume_drop` for 2 minutes. At minute 11, inject `reconciliation_gap`.
  5. At the end:
     - `pdp dbt-build`;
     - `docker compose exec airflow airflow dags list-import-errors` (must report none);
     - `airflow dags test pdp_hourly`;
     - `pdp report --month <previous month>`.
  6. Collect into `E2EResult`:
     - alert counts per rule;
     - latency p50/p95;
     - MTTD per fault;
     - mart row counts and TWP90 at today;
     - report path.

  `write_summary(result, path)` writes `e2e-summary.md`.
- **`tests/e2e/test_e2e.py`** (`@pytest.mark.e2e`) asserts against the result:
  - all four `RR*` rules fired at least once;
  - `latency_p95_ms < 5000`;
  - every injected fault has `detected_at` not None;
  - `mart_delinquency` has rows for today;
  - the report file exists;
  - no Airflow import errors.
- **`e2e.yml`** (on push to `main` and `workflow_dispatch`; `ubuntu-latest`; timeout 45 min):
  - checkout → setup-uv → `uv sync --all-extras --locked`;
  - `uv run pytest -m e2e -q`;
  - `cat e2e-summary.md >> $GITHUB_STEP_SUMMARY`;
  - upload artifacts `e2e-summary.md`, `reports/*.xlsx` and `docker compose logs` (always);
  - `pdp down --volumes` (always).
- **`README.md`** (English), sections in order:
  1. Title + CI badges.
  2. Problem (job-market findings in three lines, with links).
  3. Architecture (Mermaid).
  4. What it demonstrates (skills ↔ components table).
  5. Quickstart (Codespaces button; `pdp up/seed/live/status`; dashboard and Airflow ports).
  6. Results: numbers copied from the latest green e2e summary, under "Measured in CI on <date>".
  7. Data quality and fault catalogue (fault → check → MTTD).
  8. Data model and TWP90 definition.
  9. Design decisions.
  10. Limitations and roadmap (spec §9).
  11. License.
- **`docs/data-dictionary.md`:** every mart column with its meaning, plus PII handling.

- [ ] **Step 1:** write `tests/e2e/test_e2e.py` and `e2e.yml`, push, and trigger the workflow manually. Expected: FAIL, because `pdp.e2e` is missing.

- [ ] **Step 2:** implement `e2e.py`. Push, trigger again, and read the run. Expected: green, with a job summary showing latency and MTTD.

  If a timing assertion fails on the runner, rule in the ledger: tune wait times or thresholds, never weaken an assertion silently.

- [ ] **Step 3:** write README (numbers from the green run) and the data dictionary. Run ruff and unit tests locally.

- [ ] **Step 4: Commit** `docs: add README, data dictionary and e2e workflow`.

---

## Self-Review Notes

**Spec coverage**

| Spec section | Task(s) |
|---|---|
| §2 success criteria | 16 (1, 3, 4, 5), 10 (2), 8 (6), 16 (7) |
| §4.1 | 3 |
| §4.2 | 2, 3, 4 |
| §4.3 | 4 (+ check mapping 11) |
| §4.4 | 7 |
| §4.5 | 5, 6 |
| §4.6 | 9, 10 |
| §4.7 | 11 |
| §4.8 | 13 |
| §4.9 | 12 |
| §4.10 | 15 |
| §5 | 8, 13, 14 |
| §6 | 6, 7, 11, 13, 14 |
| §7 | every task + 16 |
| §8 | File Map |

**Spec amendments applied before this plan**
- Airflow 3.3 instead of 2.x.
- `installments.account_id`.
- `volume_anomaly` uses a rolling baseline.
- Exposure and limits live in `online.*` tables.

**Proportion**

Code blocks are tests and fixtures only; bodies are left to the implementer except the window, exposure and TWP90 definitions, which the tests pin.

**Dependency on the user**

Task 3 onward needs the GitHub repo for `pg`/dbt/e2e verification. Ask before the first push.
