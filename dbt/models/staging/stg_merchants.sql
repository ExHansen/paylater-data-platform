with latest as (
    {{ latest_cdc_version('cdc_merchants', 'merchant_id') }}
)

select
    (row_json ->> 'merchant_id')::uuid as merchant_id,
    row_json ->> 'name' as merchant_name,
    row_json ->> 'category' as category,
    row_json ->> 'province' as province,
    (row_json ->> 'created_at')::timestamptz as created_at
from latest
