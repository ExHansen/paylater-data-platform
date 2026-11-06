-- A repayment never exceeds the installment it pays (principal + interest, Rp 1 tolerance).
select r.repayment_id, r.amount, i.principal + i.interest as due_amount
from {{ ref('fct_repayments') }} as r
inner join {{ ref('fct_installments') }} as i using (installment_id)
where r.amount > i.principal + i.interest + 1
