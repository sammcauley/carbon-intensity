select
    date_trunc('day', from_ts) as date,
    round(avg(actual_intensity)) as avg_actual_intensity,
    round(avg(forecast_intensity)) as avg_forecast_intensity,
    max(actual_intensity) as max_actual_intensity,
    min(actual_intensity) as min_actual_intensity,
    count(*) as settlement_periods
from {{ ref('stg_carbon_intensity') }}
group by 1
order by 1