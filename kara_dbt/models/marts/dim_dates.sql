{{ config(materialized='table') }}

with date_series as (
    -- Generates every day from 2024 to the end of 2026
    select generate_series(
        '2024-01-01'::date,
        '2026-12-31'::date,
        '1 day'::interval
    )::date as date_day
)

select
    date_day as date_key,  -- This is our join key (e.g., 2026-01-17)
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    to_char(date_day, 'Month') as month_name,
    extract(day from date_day) as day,
    extract(dow from date_day) as day_of_week,
    to_char(date_day, 'Day') as day_name,
    case 
        when extract(dow from date_day) in (0, 6) then true 
        else false 
    end as is_weekend
from date_series