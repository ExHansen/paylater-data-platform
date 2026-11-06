select
    txn_id,
    account_id,
    merchant_id,
    amount::numeric(18, 2) as amount,
    tenor_months::int as tenor_months,
    monthly_rate::numeric(6, 4) as monthly_rate,
    channel::text as channel,
    created_at,
    created_date_wib
from {{ ref('int_valid_transactions') }}
