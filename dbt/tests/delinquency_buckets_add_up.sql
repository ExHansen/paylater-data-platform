-- The DPD buckets of every row add up to its total outstanding.
select *
from {{ ref('mart_delinquency') }}
where current_amount + dpd_1_30 + dpd_31_60 + dpd_61_90 + dpd_90_plus <> total_outstanding
