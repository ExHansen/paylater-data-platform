-- Dates the portfolio is measured on: the last 30 days plus the last 12 month-ends (WIB).
with as_of as (
    select {{ as_of_date() }} as d
)

select (select d from as_of) - offs as snapshot_date
from generate_series(0, 29) as offs

union

select (date_trunc('month', (select d from as_of)) - make_interval(months => m) - interval '1 day')::date
from generate_series(0, 11) as m
