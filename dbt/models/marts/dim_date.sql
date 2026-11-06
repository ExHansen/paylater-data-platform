with days as (
    select generate_series(date '2025-01-01', {{ as_of_date() }} + 400, interval '1 day')::date as date_day
)

select
    date_day,
    extract(year from date_day)::int as year,
    extract(month from date_day)::int as month,
    extract(day from date_day)::int as day,
    extract(isodow from date_day)::int as iso_day_of_week,
    (date_day = (date_trunc('month', date_day) + interval '1 month - 1 day')::date) as is_month_end
from days
