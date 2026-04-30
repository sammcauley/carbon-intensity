# NESO Carbon Intensity Pipeline

## Overview
This project injests data from the NESO Carbon Intensity API, cleans and transforms and models the data with dbt, before loading in Postgres.

Carbon intensity (gCO2eq/kWh) is a measure of how many grams of carbon dioxide are released to produce a kilowatt hour of electricity. It essentially means how clean our electricity is. Electricity generated using fossil fuels emits more CO2, whereas renewable energy sources such as wind, hydro and solar power produce next to no CO2 emissions. Carbon intensity is not constant, the grid is a mix of generation sources which changes constantly based on demand and weather. Knowing when low-carbon elecricity is more available allows us to make more informed choices about when to use more electricity in our buildings. This therefore reduces our carbon footprint, and can reduce energy bills as renewable energy sources are cheaper.

Demand for electricity generally peaks in the morning and evening, and "dirtier" peaking plants are brought online. This pushes the intensity up. Time of year also matters, as in winter solar generation is lower but demand is higher.


## Architecture
NESO Carbon Intensity API -> Ingestion: Python -> Transformation: dbt -> Storage: PostgreSQL

## Data source
https://api.carbonintensity.org.uk/
The API used is the NESO Carbon Intensity API, which is free to use and unrestricted. The following endpoints are used:
- /intensity/{from}/{to}
- /region

The intensity endpoint returns an index with the values "very low", "low", "moderate", "high", and "very high".
This endpoint also returns a forecast intensity, a prediction made at the start of the settlement period, and an actual intensity, which is measured after the settlement period. The actual intensity will sometimes be returned as null because the measurement hasn't come in yet.

## Tech stack
- Python
- Polars
- requests
- dbt
- Airflow
- PostgreSQL

## Project structure
/ requirements.txt \
/ tests \
/ main.py 

## Findings
This query was run on the two year period of 29/04/24 - 29/04/26:
SELECT 
    EXTRACT(HOUR FROM from_ts) AS hour_of_day,
    ROUND(AVG(actual_intensity)) AS avg_intensity,
    COUNT(*) AS records
FROM raw_carbon_intensity
GROUP BY hour_of_day
ORDER BY hour_of_day;

and produced this output:
 hour_of_day | avg_intensity | records 
-------------+---------------+---------
           0 |           110 |    1458
           1 |           108 |    1458
           2 |           107 |    1458
           3 |           109 |    1458
           4 |           116 |    1458
           5 |           128 |    1458
           6 |           136 |    1458
           7 |           134 |    1458
           8 |           128 |    1458
           9 |           120 |    1458
          10 |           114 |    1458
          11 |           111 |    1458
          12 |           110 |    1458
          13 |           113 |    1458
          14 |           121 |    1459
          15 |           135 |    1460
          16 |           148 |    1460
          17 |           156 |    1460
          18 |           158 |    1460
          19 |           155 |    1460
          20 |           147 |    1460
          21 |           131 |    1460
          22 |           116 |    1460
          23 |           110 |    1459

The morning peaks in intensity at around 6am - kettles, showers and lighting are being used at this time. The peak is relatively soft and drops back down at around 9am as demand stabilises. The afternoon/evening peak is more dominant, beginning at 2-3pm until 6pm, and shows people returning home from work, cooking, turning heating on etc. This is when gas peakers are most likely to be running. Overnight shows the cleanest energy as there is less demand and wind and nuclear are doing most of the work.