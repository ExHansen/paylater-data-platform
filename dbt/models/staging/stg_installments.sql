with latest as (
    {{ latest_cdc_version('cdc_installments', 'installment_id') }}
)

select
    (row_json ->> 'installment_id')::uuid as installment_id,
    (row_json ->> 'txn_id')::uuid as txn_id,
    (row_json ->> 'account_id')::uuid as account_id,
    (row_json ->> 'seq')::int as seq,
    {{ debezium_date("row_json ->> 'due_date'") }} as due_date,
    (row_json ->> 'principal')::numeric(18, 2) as principal,
    (row_json ->> 'interest')::numeric(18, 2) as interest,
    row_json ->> 'status' as status,
    (row_json ->> 'paid_at')::timestamptz as paid_at,
    ((row_json ->> 'paid_at')::timestamptz at time zone 'Asia/Jakarta')::date as paid_date_wib,
    (row_json ->> 'updated_at')::timestamptz as updated_at
from latest
