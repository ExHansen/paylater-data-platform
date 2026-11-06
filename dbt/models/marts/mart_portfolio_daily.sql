-- Portfolio size per snapshot date (WIB): open principal, borrowers, and that day's lending.
with open_book as (
    select
        as_of_date,
        sum(principal) as outstanding,
        count(distinct account_id) as borrowers
    from {{ ref('int_installment_dpd') }}
    group by as_of_date
),

lending as (
    select created_date_wib, sum(amount) as disbursed, count(*) as txns
    from {{ ref('fct_transactions') }}
    group by created_date_wib
)

select
    d.snapshot_date as as_of_date,
    coalesce(o.outstanding, 0)::numeric(18, 2) as outstanding_principal,
    coalesce(o.borrowers, 0)::int as active_borrowers,
    coalesce(l.disbursed, 0)::numeric(18, 2) as disbursed_amount,
    coalesce(l.txns, 0)::int as txn_count
from {{ ref('int_snapshot_dates') }} as d
left join open_book as o on o.as_of_date = d.snapshot_date
left join lending as l on l.created_date_wib = d.snapshot_date
