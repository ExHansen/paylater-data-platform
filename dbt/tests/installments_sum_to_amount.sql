-- The principal of a purchase's installments adds up to the purchase amount (Rp 1 tolerance).
select t.txn_id, t.amount, sum(i.principal) as scheduled_principal
from {{ ref('fct_transactions') }} as t
inner join {{ ref('fct_installments') }} as i using (txn_id)
group by t.txn_id, t.amount
having abs(sum(i.principal) - t.amount) > 1
