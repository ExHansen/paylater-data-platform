-- Explicit column list: fields added at the source (schema drift) never leak into the warehouse.
with latest as (
    {{ latest_cdc_version('cdc_transactions', 'txn_id') }}
)

select
    (row_json ->> 'txn_id')::uuid as txn_id,
    (row_json ->> 'account_id')::uuid as account_id,
    (row_json ->> 'merchant_id')::uuid as merchant_id,
    (row_json ->> 'amount')::numeric(18, 2) as amount,
    (row_json ->> 'tenor_months')::int as tenor_months,
    (row_json ->> 'monthly_rate')::numeric(6, 4) as monthly_rate,
    row_json ->> 'status' as status,
    row_json ->> 'channel' as channel,
    row_json ->> 'device_id' as device_id,
    (row_json ->> 'created_at')::timestamptz as created_at,
    ((row_json ->> 'created_at')::timestamptz at time zone 'Asia/Jakarta')::date as created_date_wib,
    (row_json ->> 'updated_at')::timestamptz as updated_at,
    lsn as source_lsn
from latest
