with latest as (
    {{ latest_cdc_version('cdc_repayments', 'repayment_id') }}
)

select
    (row_json ->> 'repayment_id')::uuid as repayment_id,
    (row_json ->> 'installment_id')::uuid as installment_id,
    (row_json ->> 'account_id')::uuid as account_id,
    (row_json ->> 'amount')::numeric(18, 2) as amount,
    row_json ->> 'method' as method,
    (row_json ->> 'paid_at')::timestamptz as paid_at,
    ((row_json ->> 'paid_at')::timestamptz at time zone 'Asia/Jakarta')::date as paid_date_wib
from latest
