-- Outstanding principal by days-past-due bucket, per snapshot date and province.
-- A purchase is bucketed by the worst (maximum) DPD of its open installments.
-- TWP90 (OJK term: "tingkat wanprestasi 90 hari") = 90+ DPD outstanding / total outstanding.
with per_purchase as (
    select as_of_date, txn_id, account_id, sum(principal) as outstanding, max(dpd) as max_dpd
    from {{ ref('int_installment_dpd') }}
    group by as_of_date, txn_id, account_id
),

with_province as (
    select p.*, u.province
    from per_purchase as p
    inner join {{ ref('dim_user') }} as u using (account_id)
)

select
    as_of_date,
    province,
    sum(outstanding)::numeric(18, 2) as total_outstanding,
    coalesce(sum(outstanding) filter (where max_dpd = 0), 0)::numeric(18, 2) as current_amount,
    coalesce(sum(outstanding) filter (where max_dpd between 1 and 30), 0)::numeric(18, 2) as dpd_1_30,
    coalesce(sum(outstanding) filter (where max_dpd between 31 and 60), 0)::numeric(18, 2) as dpd_31_60,
    coalesce(sum(outstanding) filter (where max_dpd between 61 and 90), 0)::numeric(18, 2) as dpd_61_90,
    coalesce(sum(outstanding) filter (where max_dpd > 90), 0)::numeric(18, 2) as dpd_90_plus,
    round(
        coalesce(sum(outstanding) filter (where max_dpd > 90), 0) / nullif(sum(outstanding), 0), 6
    )::numeric(8, 6) as twp90
from with_province
group by as_of_date, province
