-- One row per user with their (single) PayLater account. No personal data beyond hashes.
select
    u.user_id,
    a.account_id,
    u.province::text as province,
    u.city::text as city,
    u.kyc_status::text as kyc_status,
    a.risk_grade::text as risk_grade,
    a.credit_limit::numeric(18, 2) as credit_limit,
    a.status::text as account_status,
    u.created_at
from {{ ref('stg_users') }} as u
left join {{ ref('stg_credit_accounts') }} as a using (user_id)
