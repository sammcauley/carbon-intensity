# NESO Carbon Intensity Pipeline

## Overview
This project injests data from the NESO Carbon Intensity API, cleans and transforms and models the data with dbt, before loading in Postgres.

## Architecture
NESO Carbon Intensity API -> Ingestion: Python -> Transformation: dbt -> Storage: PostgreSQL

## Data source
https://api.carbonintensity.org.uk/
The API used is the NESO Carbon Intensity API, which is free to use and unrestricted. The following endpoints are used:
- /intensity/{from}/{to}
- /region

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

