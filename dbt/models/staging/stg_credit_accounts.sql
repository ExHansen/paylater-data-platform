with latest as (
    {{ latest_cdc_version('cdc_credit_accounts', 'account_id') }}
)

select
    (row_json ->> 'account_id')::uuid as account_id,
    (row_json ->> 'user_id')::uuid as user_id,
    (row_json ->> 'credit_limit')::numeric(18, 2) as credit_limit,
    row_json ->> 'status' as status,
    row_json ->> 'risk_grade' as risk_grade,
    (row_json ->> 'created_at')::timestamptz as created_at,
    (row_json ->> 'updated_at')::timestamptz as updated_at
from latest
