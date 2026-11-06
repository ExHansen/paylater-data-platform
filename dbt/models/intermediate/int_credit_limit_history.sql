-- Credit-limit history (SCD type 2) rebuilt from the CDC log: one row per limit value an
-- account has had, valid from the commit time of the change until the next change.
-- Debezium stamps every snapshot record with the same LSN, so changes are keyed by account + LSN.
with events as (
    select
        (coalesce(after, before) ->> 'account_id')::uuid as account_id,
        (after ->> 'credit_limit')::numeric(18, 2) as credit_limit,
        to_timestamp(source_ts_ms / 1000.0) as changed_at,
        lsn,
        kafka_offset
    from {{ source('raw', 'cdc_credit_accounts') }}
    where op <> 'd'
),

changes as (
    select distinct on (account_id, lsn) account_id, credit_limit, changed_at, lsn
    from events
    order by account_id, lsn, kafka_offset
),

with_previous as (
    select
        *,
        lag(credit_limit) over (partition by account_id order by lsn) as previous_limit
    from changes
),

versions as (
    select account_id, credit_limit, changed_at as valid_from
    from with_previous
    where previous_limit is null or previous_limit <> credit_limit
)

select
    account_id,
    credit_limit,
    valid_from,
    lead(valid_from) over (partition by account_id order by valid_from) as valid_to
from versions
