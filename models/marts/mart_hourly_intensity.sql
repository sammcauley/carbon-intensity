select 
    extract(hour from from_ts) as hour_of_day,
    round(avg(actual_intensity)) as avg_actual_intensity,
    count(*) as records
from {{ ref('stg_carbon_intensity') }}
group by 1
order by 1