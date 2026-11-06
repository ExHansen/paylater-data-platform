select
    merchant_id,
    merchant_name::text as merchant_name,
    category::text as category,
    province::text as province,
    created_at
from {{ ref('stg_merchants') }}
