select
    i.installment_id,
    i.txn_id,
    i.account_id,
    i.seq::int as seq,
    i.due_date,
    i.principal::numeric(18, 2) as principal,
    i.interest::numeric(18, 2) as interest,
    i.status::text as status,
    i.paid_at,
    i.paid_date_wib
from {{ ref('stg_installments') }} as i
inner join {{ ref('int_valid_transactions') }} as t using (txn_id)
