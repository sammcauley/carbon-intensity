select
    extract(dow from from_ts) as day_of_week,
    extract(hour from from_ts) as hour_of_day,
    round(avg(actual_intensity)) as avg_actual_intensity,
    count(*) as records
from {{ ref('stg_carbon_intensity') }}
group by 1, 2
order by 1, 2