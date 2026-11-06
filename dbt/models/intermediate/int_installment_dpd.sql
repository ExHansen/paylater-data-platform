-- Every installment that is open (issued and not yet paid) on each snapshot date, with its
-- days past due on that date: DPD = snapshot date - due date when overdue, otherwise 0.
with installments as (
    select
        i.installment_id,
        i.txn_id,
        i.account_id,
        i.principal,
        i.due_date,
        i.paid_date_wib,
        t.created_date_wib
    from {{ ref('stg_installments') }} as i
    inner join {{ ref('int_valid_transactions') }} as t using (txn_id)
)

select
    s.snapshot_date as as_of_date,
    i.installment_id,
    i.txn_id,
    i.account_id,
    i.principal,
    i.due_date,
    greatest(0, s.snapshot_date - i.due_date) as dpd
from {{ ref('int_snapshot_dates') }} as s
inner join installments as i
    on i.created_date_wib <= s.snapshot_date
    and (i.paid_date_wib is null or i.paid_date_wib > s.snapshot_date)
