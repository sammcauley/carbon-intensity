with daily_avg as (
    select
        date_trunc('day', from_ts),
        round(avg(actual_intensity)) as avg_actual_intensity,
    from {{ ref('stg_carbon_intensity') }}
    group by 1
)

select
    date,
    avg_actual_intensity,
    lag(avg_actual_intensity) over (order by date) as yesterday_avg_intensity,
    avg_actual_intensity - lag(avg_actual_intensity) over (order by date) as daily_change
from daily_avg
order by 1
