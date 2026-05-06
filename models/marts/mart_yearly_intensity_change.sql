with monthly_avg as (
    select
        date_trunc('month', from_ts) as month,
        round(avg(actual_intensity)) as avg_actual_intensity
    from {{ ref('stg_carbon_intensity') }}
    group by 1
    order by 1
)

select
    month,
    avg_actual_intensity,
    lag(avg_actual_intensity, 12) over (order by month) as prev_year_avg_intensity,
    avg_actual_intensity - lag(avg_actual_intensity, 12) over (order by month) as yearly_change
from monthly_avg
order by 1