select
    hour_of_day,
    avg_actual_intensity,
    rank() over (order by avg_actual_intensity asc) as cleanest_rank,
    rank() over (order by avg_actual_intensity desc) as dirtiest_rank
from {{ ref('mart_hourly_intensity') }}