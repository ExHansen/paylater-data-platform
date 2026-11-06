# Data dictionary

Every table the platform writes, layer by layer. Times are stored in UTC (`timestamptz`).
Business dates (`*_date_wib`, `as_of_date`, `due_date`) are calendar days in WIB (Asia/Jakarta).
Money is Indonesian rupiah, `numeric(18,2)`.

```
paylater_app (Postgres, CDC source)
  └─ Debezium → Kafka topics paylater.public.<table>
       ├─ loader  → warehouse.raw.cdc_<table>  → dbt: staging → intermediate → marts
       └─ stream  → warehouse.online.*          (features, alerts, exposure, limits)
warehouse.ops.*  data-quality results, alerts, fault log, chaos switches
```

## Source: `paylater_app`

The application database the simulator writes to. It deliberately has no `CHECK` constraints on
`amount` or `tenor_months`, so application bugs reach the data and the quality checks must
catch them. All tables use `REPLICA IDENTITY FULL` so updates and deletes carry the old row.

| Table | Grain | Columns |
|---|---|---|
| `users` | one borrower | `user_id`, `full_name`, `nik` (national ID), `phone`, `province`, `city`, `kyc_status`, `created_at`, `updated_at` |
| `credit_accounts` | one PayLater account per user | `account_id`, `user_id`, `credit_limit`, `status`, `risk_grade` (A–E), `created_at`, `updated_at` |
| `merchants` | one merchant | `merchant_id`, `name`, `category`, `province`, `created_at` |
| `transactions` | one purchase | `txn_id`, `account_id`, `merchant_id`, `amount`, `tenor_months` (1/3/6/12), `monthly_rate`, `status` (approved/declined/refunded), `channel`, `device_id`, `created_at`, `updated_at` |
| `installments` | one monthly instalment of a purchase | `installment_id`, `txn_id`, `account_id`, `seq`, `due_date`, `principal`, `interest`, `status` (unpaid/paid), `paid_at`, `updated_at` |
| `repayments` | one payment of an instalment | `repayment_id`, `installment_id`, `account_id`, `amount`, `method`, `paid_at` |

`full_name`, `nik` and `phone` are personal data. They never leave the source unhashed: staging
replaces `nik` and `phone` with salted SHA-256 hashes and drops `full_name`.

## Raw: `warehouse.raw`

One append-only table per source table, written by the loader exactly as Debezium sent it.

| Column | Meaning |
|---|---|
| `id` | load order (bigserial) |
| `op` | `c` create, `u` update, `d` delete, `r` snapshot read |
| `pk` | primary key of the source row (jsonb) |
| `before`, `after` | old and new row images (jsonb; `numeric` as strings, dates as days since 1970-01-01, `timestamptz` as ISO-8601) |
| `source_ts_ms` | commit time in the source database (epoch ms) |
| `lsn` | Postgres log sequence number of the change; orders changes of one row |
| `kafka_topic`, `kafka_partition`, `kafka_offset` | where the event came from; unique together, which makes reloading idempotent |
| `ingested_at` | when the loader stored it |

`raw.cdc_dead_letter` keeps events the loader could not parse (`kafka_*`, `payload`, `error`).
They are stored instead of blocking the topic.

## Staging: `warehouse.staging` (dbt views)

`stg_<table>` holds the current state of each source row. It is the version with the highest
`lsn`, so late or re-delivered events never win over newer ones, and rows whose latest change is a
delete are dropped. Columns are listed explicitly, so a field added at the source (schema drift)
does not leak into the warehouse.

| Model | Notes |
|---|---|
| `stg_users` | `nik_hash`, `phone_hash` (salted SHA-256, salt from the `pii_salt` var) instead of the raw values; no name |
| `stg_credit_accounts` | current limit, status and risk grade |
| `stg_merchants` | `merchant_name` |
| `stg_transactions` | adds `created_date_wib`, `source_lsn`; keeps invalid rows (amount ≤ 0, unknown tenor) so they can be counted. Tests on these are `warn` |
| `stg_installments` | `due_date` decoded from epoch days; adds `paid_date_wib` |
| `stg_repayments` | adds `paid_date_wib` |

## Intermediate: `warehouse.intermediate` (dbt tables)

| Model | Grain | Meaning |
|---|---|---|
| `int_valid_transactions` | purchase | approved purchases with a positive amount and a known tenor. Invalid ones are quarantined here and never reach the marts |
| `int_snapshot_dates` | date | the dates DPD is measured on: the last 30 days plus the last 12 month-ends (as of `as_of_date`) |
| `int_installment_dpd` | snapshot date × open instalment | days past due on that date, `greatest(0, snapshot − due_date)`, for instalments issued and not yet paid |
| `int_credit_limit_history` | account × limit version | SCD type 2 rebuilt from the CDC log: `credit_limit`, `valid_from` (commit time), `valid_to` |

## Marts: `warehouse.marts` (dbt tables, enforced contracts)

| Model | Grain | Columns |
|---|---|---|
| `dim_user` | user | `user_id`, `account_id`, `province`, `city`, `kyc_status`, `risk_grade`, `credit_limit`, `account_status`, `created_at` (no PII) |
| `dim_merchant` | merchant | `merchant_id`, `merchant_name`, `category`, `province`, `created_at` |
| `dim_date` | day | `date_day`, `year`, `month`, `day`, `iso_day_of_week`, `is_month_end` |
| `fct_transactions` | valid approved purchase | `txn_id`, `account_id`, `merchant_id`, `amount`, `tenor_months`, `monthly_rate`, `channel`, `created_at`, `created_date_wib` |
| `fct_installments` | instalment | `installment_id`, `txn_id`, `account_id`, `seq`, `due_date`, `principal`, `interest`, `status`, `paid_at`, `paid_date_wib` |
| `fct_repayments` | repayment | `repayment_id`, `installment_id`, `account_id`, `amount`, `method`, `paid_at`, `paid_date_wib`, `due_date`, `paid_on_time` |
| `mart_portfolio_daily` | snapshot date | `as_of_date`, `outstanding_principal`, `active_borrowers`, `disbursed_amount`, `txn_count` |
| `mart_delinquency` | snapshot date × province | `total_outstanding`, `current_amount`, `dpd_1_30`, `dpd_31_60`, `dpd_61_90`, `dpd_90_plus`, `twp90` |
| `mart_collections` | month × payment method | `n_repayments`, `amount_collected`, `on_time_share` |
| `mart_merchant_performance` | merchant | `txn_count`, `gmv`, `buyers`, `outstanding_principal`, `dpd_90_plus` |

**TWP90** (*tingkat wanprestasi 90 hari*, the OJK indicator) = outstanding principal of purchases
more than 90 days past due ÷ total outstanding principal. A purchase is bucketed by the worst DPD
of its open instalments.

Singular tests check that instalments sum to the purchase amount, that a repayment never exceeds
what was due, and that the DPD buckets add up to the total.

## Online: `warehouse.online` (written by the stream)

| Table | Grain | Columns |
|---|---|---|
| `risk_features` | account | `txn_count_10m`, `txn_amount_10m`, `txn_count_1h`, `txn_amount_1h`, `utilization`, `is_new_device`, `updated_at` |
| `risk_alerts` | purchase that broke a rule | `alert_id`, `account_id`, `txn_id`, `rules` (text[]), `amount`, `source_ts_ms`, `latency_ms` (alert time − source commit time), `created_at` |
| `account_exposure` | account | `exposure`: open principal. Approved purchases add their amount; refunds and paid instalments subtract |
| `account_limits` | account | latest `credit_limit` seen on the CDC stream |

Risk rules: **RR1_velocity** 5+ purchases in 10 minutes · **RR2_near_limit** amount > 80% of
the limit · **RR3_new_device_high** a device not seen on the account in 30 days and amount >
Rp 2,000,000 · **RR4_utilization** account exposure / limit > 95%.

## Operations: `warehouse.ops`

| Table | Meaning |
|---|---|
| `dq_results` | one row per check, target and run: `check_type`, `target`, `status` (pass/warn/fail/error), `observed`, `expected`, `run_at` |
| `alerts` | one row per failing check result: `check_type`, `target`, `message`, `created_at`, `resolved_at` |
| `fault_log` | every injected fault: `fault_type`, `injected_at`, `details` (jsonb). Time to detect is measured from `injected_at` |
| `chaos` | switches the services read, e.g. `drop_next_batch` (the loader loses its next batch) |

Data-quality checks (`check_type`): `freshness`, `raw_duplicate_lsn`, `lsn_regression`,
`schema_drift` (against `infra/contracts/*.yml`), `invalid_values`, `volume_anomaly`,
`reconciliation`, plus `dbt_test` for dbt test results recorded by Airflow.
