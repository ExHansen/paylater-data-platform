-- Lending volume per merchant and how much of it is 90+ days past due at the as-of date.
with sales as (
    select
        merchant_id,
        count(*) as txn_count,
        sum(amount) as gmv,
        count(distinct account_id) as buyers
    from {{ ref('fct_transactions') }}
    group by merchant_id
),

open_at_as_of as (
    select d.txn_id, sum(d.principal) as outstanding, max(d.dpd) as max_dpd
    from {{ ref('int_installment_dpd') }} as d
    where d.as_of_date = {{ as_of_date() }}
    group by d.txn_id
),

risk as (
    select
        t.merchant_id,
        sum(o.outstanding) as outstanding,
        sum(o.outstanding) filter (where o.max_dpd > 90) as dpd_90_plus
    from open_at_as_of as o
    inner join {{ ref('fct_transactions') }} as t using (txn_id)
    group by t.merchant_id
)

select
    m.merchant_id,
    m.merchant_name,
    m.category,
    m.province,
    coalesce(s.txn_count, 0)::int as txn_count,
    coalesce(s.gmv, 0)::numeric(18, 2) as gmv,
    coalesce(s.buyers, 0)::int as buyers,
    coalesce(r.outstanding, 0)::numeric(18, 2) as outstanding_principal,
    coalesce(r.dpd_90_plus, 0)::numeric(18, 2) as dpd_90_plus
from {{ ref('dim_merchant') }} as m
left join sales as s using (merchant_id)
left join risk as r using (merchant_id)
