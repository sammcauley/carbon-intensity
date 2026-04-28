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

