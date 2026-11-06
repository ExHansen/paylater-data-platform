-- Repayments per month (WIB) and payment method, with the share paid on or before the due date.
select
    date_trunc('month', paid_date_wib)::date as month,
    method,
    count(*)::int as n_repayments,
    sum(amount)::numeric(18, 2) as amount_collected,
    round(avg(case when paid_on_time then 1 else 0 end), 4)::numeric(6, 4) as on_time_share
from {{ ref('fct_repayments') }}
group by 1, 2
