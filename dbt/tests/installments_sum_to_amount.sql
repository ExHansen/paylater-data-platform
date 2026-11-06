-- The principal of a purchase's installments adds up to the purchase amount (Rp 1 tolerance).
-- Purchases from the last 10 minutes are skipped: their installments may still be in flight on
-- another topic (lost rows are the reconciliation check's job).
select t.txn_id, t.amount, sum(i.principal) as scheduled_principal
from {{ ref('fct_transactions') }} as t
inner join {{ ref('fct_installments') }} as i using (txn_id)
where t.created_at < current_timestamp - interval '10 minutes'
group by t.txn_id, t.amount
having abs(sum(i.principal) - t.amount) > 1
