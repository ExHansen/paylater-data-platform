select
    r.repayment_id,
    r.installment_id,
    r.account_id,
    r.amount::numeric(18, 2) as amount,
    r.method::text as method,
    r.paid_at,
    r.paid_date_wib,
    i.due_date,
    (r.paid_date_wib <= i.due_date) as paid_on_time
from {{ ref('stg_repayments') }} as r
inner join {{ ref('fct_installments') }} as i using (installment_id)
