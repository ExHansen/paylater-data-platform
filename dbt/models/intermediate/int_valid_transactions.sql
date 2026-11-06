-- Approved purchases that pass basic validity rules. Rows failing them stay visible in
-- staging (warning-level tests) but are quarantined from facts and marts.
select *
from {{ ref('stg_transactions') }}
where status = 'approved'
  and amount > 0
  and tenor_months in (1, 3, 6, 12)
