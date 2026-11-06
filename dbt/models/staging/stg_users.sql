with latest as (
    {{ latest_cdc_version('cdc_users', 'user_id') }}
)

select
    (row_json ->> 'user_id')::uuid as user_id,
    {{ hash_pii("(row_json ->> 'nik')") }} as nik_hash,
    {{ hash_pii("(row_json ->> 'phone')") }} as phone_hash,
    row_json ->> 'province' as province,
    row_json ->> 'city' as city,
    row_json ->> 'kyc_status' as kyc_status,
    (row_json ->> 'created_at')::timestamptz as created_at,
    (row_json ->> 'updated_at')::timestamptz as updated_at
from latest
