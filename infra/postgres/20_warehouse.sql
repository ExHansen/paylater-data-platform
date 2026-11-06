-- Warehouse database: raw CDC (bronze), dbt layers, online feature store, operations.
\connect warehouse

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS online;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS raw.cdc_users (
    id               bigserial PRIMARY KEY,
    op               text NOT NULL,
    pk               jsonb,
    before           jsonb,
    after            jsonb,
    source_ts_ms     bigint NOT NULL,
    lsn              bigint NOT NULL,
    kafka_topic      text NOT NULL,
    kafka_partition  integer NOT NULL,
    kafka_offset     bigint NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS cdc_users_ingested ON raw.cdc_users (ingested_at);
CREATE INDEX IF NOT EXISTS cdc_users_lsn ON raw.cdc_users (lsn);

CREATE TABLE IF NOT EXISTS raw.cdc_credit_accounts (
    id               bigserial PRIMARY KEY,
    op               text NOT NULL,
    pk               jsonb,
    before           jsonb,
    after            jsonb,
    source_ts_ms     bigint NOT NULL,
    lsn              bigint NOT NULL,
    kafka_topic      text NOT NULL,
    kafka_partition  integer NOT NULL,
    kafka_offset     bigint NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS cdc_credit_accounts_ingested ON raw.cdc_credit_accounts (ingested_at);
CREATE INDEX IF NOT EXISTS cdc_credit_accounts_lsn ON raw.cdc_credit_accounts (lsn);

CREATE TABLE IF NOT EXISTS raw.cdc_merchants (
    id               bigserial PRIMARY KEY,
    op               text NOT NULL,
    pk               jsonb,
    before           jsonb,
    after            jsonb,
    source_ts_ms     bigint NOT NULL,
    lsn              bigint NOT NULL,
    kafka_topic      text NOT NULL,
    kafka_partition  integer NOT NULL,
    kafka_offset     bigint NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS cdc_merchants_ingested ON raw.cdc_merchants (ingested_at);
CREATE INDEX IF NOT EXISTS cdc_merchants_lsn ON raw.cdc_merchants (lsn);

CREATE TABLE IF NOT EXISTS raw.cdc_transactions (
    id               bigserial PRIMARY KEY,
    op               text NOT NULL,
    pk               jsonb,
    before           jsonb,
    after            jsonb,
    source_ts_ms     bigint NOT NULL,
    lsn              bigint NOT NULL,
    kafka_topic      text NOT NULL,
    kafka_partition  integer NOT NULL,
    kafka_offset     bigint NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS cdc_transactions_ingested ON raw.cdc_transactions (ingested_at);
CREATE INDEX IF NOT EXISTS cdc_transactions_lsn ON raw.cdc_transactions (lsn);

CREATE TABLE IF NOT EXISTS raw.cdc_installments (
    id               bigserial PRIMARY KEY,
    op               text NOT NULL,
    pk               jsonb,
    before           jsonb,
    after            jsonb,
    source_ts_ms     bigint NOT NULL,
    lsn              bigint NOT NULL,
    kafka_topic      text NOT NULL,
    kafka_partition  integer NOT NULL,
    kafka_offset     bigint NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS cdc_installments_ingested ON raw.cdc_installments (ingested_at);
CREATE INDEX IF NOT EXISTS cdc_installments_lsn ON raw.cdc_installments (lsn);

CREATE TABLE IF NOT EXISTS raw.cdc_repayments (
    id               bigserial PRIMARY KEY,
    op               text NOT NULL,
    pk               jsonb,
    before           jsonb,
    after            jsonb,
    source_ts_ms     bigint NOT NULL,
    lsn              bigint NOT NULL,
    kafka_topic      text NOT NULL,
    kafka_partition  integer NOT NULL,
    kafka_offset     bigint NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
CREATE INDEX IF NOT EXISTS cdc_repayments_ingested ON raw.cdc_repayments (ingested_at);
CREATE INDEX IF NOT EXISTS cdc_repayments_lsn ON raw.cdc_repayments (lsn);

CREATE TABLE IF NOT EXISTS raw.cdc_dead_letter (
    id               bigserial PRIMARY KEY,
    kafka_topic      text NOT NULL,
    kafka_partition  integer NOT NULL,
    kafka_offset     bigint NOT NULL,
    payload          text,
    error            text NOT NULL,
    ingested_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS online.risk_features (
    account_id      uuid PRIMARY KEY,
    txn_count_10m   integer NOT NULL,
    txn_amount_10m  numeric(18, 2) NOT NULL,
    txn_count_1h    integer NOT NULL,
    txn_amount_1h   numeric(18, 2) NOT NULL,
    utilization     numeric(8, 4),
    is_new_device   boolean NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS online.risk_alerts (
    alert_id      uuid PRIMARY KEY,
    account_id    uuid NOT NULL,
    txn_id        uuid NOT NULL,
    rules         text[] NOT NULL,
    amount        numeric(18, 2) NOT NULL,
    source_ts_ms  bigint NOT NULL,
    latency_ms    integer NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS risk_alerts_created ON online.risk_alerts (created_at);

CREATE TABLE IF NOT EXISTS online.account_exposure (
    account_id  uuid PRIMARY KEY,
    exposure    numeric(18, 2) NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Changes already applied to account_exposure (row + LSN); redelivered events are skipped.
CREATE TABLE IF NOT EXISTS online.exposure_changes (
    source_table  text NOT NULL,
    row_id        text NOT NULL,
    lsn           bigint NOT NULL,
    applied_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source_table, row_id, lsn)
);

CREATE TABLE IF NOT EXISTS online.account_limits (
    account_id    uuid PRIMARY KEY,
    credit_limit  numeric(18, 2) NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.fault_log (
    fault_id     bigserial PRIMARY KEY,
    fault_type   text NOT NULL,
    injected_at  timestamptz NOT NULL DEFAULT now(),
    details      jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    id          bigserial PRIMARY KEY,
    check_type  text NOT NULL,
    target      text NOT NULL,
    status      text NOT NULL,
    observed    text,
    expected    text,
    run_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dq_results_run_at ON ops.dq_results (run_at);

CREATE TABLE IF NOT EXISTS ops.alerts (
    id           bigserial PRIMARY KEY,
    check_type   text NOT NULL,
    target       text NOT NULL,
    message      text NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    resolved_at  timestamptz
);

CREATE TABLE IF NOT EXISTS ops.chaos (
    name     text PRIMARY KEY,
    enabled  boolean NOT NULL DEFAULT false
);
INSERT INTO ops.chaos (name, enabled) VALUES ('drop_next_batch', false)
    ON CONFLICT (name) DO NOTHING;
