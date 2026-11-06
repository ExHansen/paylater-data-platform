-- PayLater application database: the CDC source.
\connect paylater_app

CREATE TABLE IF NOT EXISTS users (
    user_id     uuid PRIMARY KEY,
    full_name   text NOT NULL,
    nik         text NOT NULL,
    phone       text NOT NULL,
    province    text NOT NULL,
    city        text NOT NULL,
    kyc_status  text NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS credit_accounts (
    account_id    uuid PRIMARY KEY,
    user_id       uuid NOT NULL REFERENCES users (user_id),
    credit_limit  numeric(18, 2) NOT NULL,
    status        text NOT NULL,
    risk_grade    text NOT NULL,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id  uuid PRIMARY KEY,
    name         text NOT NULL,
    category     text NOT NULL,
    province     text NOT NULL,
    created_at   timestamptz NOT NULL
);

-- Deliberately no CHECK on amount or tenor_months: application bugs must reach the data
-- so that the data-quality checks can catch them.
CREATE TABLE IF NOT EXISTS transactions (
    txn_id        uuid PRIMARY KEY,
    account_id    uuid NOT NULL REFERENCES credit_accounts (account_id),
    merchant_id   uuid NOT NULL REFERENCES merchants (merchant_id),
    amount        numeric(18, 2) NOT NULL,
    tenor_months  integer NOT NULL,
    monthly_rate  numeric(6, 4) NOT NULL,
    status        text NOT NULL,
    channel       text NOT NULL,
    device_id     text NOT NULL,
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS transactions_account_created ON transactions (account_id, created_at);

CREATE TABLE IF NOT EXISTS installments (
    installment_id  uuid PRIMARY KEY,
    txn_id          uuid NOT NULL REFERENCES transactions (txn_id),
    account_id      uuid NOT NULL REFERENCES credit_accounts (account_id),
    seq             integer NOT NULL,
    due_date        date NOT NULL,
    principal       numeric(18, 2) NOT NULL,
    interest        numeric(18, 2) NOT NULL,
    status          text NOT NULL,
    paid_at         timestamptz,
    updated_at      timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS installments_status_due ON installments (status, due_date);

CREATE TABLE IF NOT EXISTS repayments (
    repayment_id    uuid PRIMARY KEY,
    installment_id  uuid NOT NULL REFERENCES installments (installment_id),
    account_id      uuid NOT NULL REFERENCES credit_accounts (account_id),
    amount          numeric(18, 2) NOT NULL,
    method          text NOT NULL,
    paid_at         timestamptz NOT NULL
);

-- Updates and deletes carry the full before image in CDC events.
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE credit_accounts REPLICA IDENTITY FULL;
ALTER TABLE merchants REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
ALTER TABLE installments REPLICA IDENTITY FULL;
ALTER TABLE repayments REPLICA IDENTITY FULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'pdp_pub') THEN
        CREATE PUBLICATION pdp_pub FOR TABLE
            users, credit_accounts, merchants, transactions, installments, repayments;
    END IF;
END
$$;
