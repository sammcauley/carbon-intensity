select
    from_ts,
    to_ts,
    forecast_intensity,
    actual_intensity,
    intensity_index,
    ingestion_ts
from {{ source("raw", "raw_carbon_intensity") }}
where actual_intensity is not null