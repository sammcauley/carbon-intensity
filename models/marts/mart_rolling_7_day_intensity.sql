select
    date_trunc('day', from_ts) as date,
    round(avg(actual_intensity)) as avg_actual_intensity,
    round(avg(avg(actual_intensity)) over (
        order by date_trunc('day', from_ts)
        rows between 6 preceding and current row
    )) as rolling_7_day_avg
from {{ ref('stg_carbon_intensity') }}
group by 1
order by 1