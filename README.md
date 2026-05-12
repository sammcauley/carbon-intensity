# NESO Carbon Intensity Pipeline

## Overview
This project ingests data from the NESO Carbon Intensity API, cleans and transforms the data with dbt, and loads it into PostgreSQL for analysis.

Carbon intensity (gCO2eq/kWh) is a measure of how many grams of carbon dioxide are released to produce a kilowatt hour of electricity — essentially how clean our electricity is. Electricity generated using fossil fuels emits more CO2, whereas renewable energy sources such as wind, hydro and solar produce next to no CO2 emissions.

Carbon intensity is not constant — the grid is a mix of generation sources which changes constantly based on demand and weather. Knowing when low-carbon electricity is more available allows us to make more informed choices about when to use electricity, reducing our carbon footprint and potentially our energy bills.

Demand for electricity generally peaks in the morning and evening when dirtier peaking plants are brought online, pushing intensity up. Time of year also matters — in winter solar generation is lower but demand is higher, generally meaning higher intensity than summer.

## Architecture
NESO Carbon Intensity API → Python ingestion → PostgreSQL (raw) → dbt → PostgreSQL (staging + marts)

## Data source
- API: https://api.carbonintensity.org.uk/
- Free to use, no authentication required
- Endpoint used: `/intensity/{from}/{to}`

Each record represents one half-hourly settlement period — the unit the UK electricity market settles in. Each period returns:
- `forecast_intensity` — a prediction made at the start of the settlement period
- `actual_intensity` — measured after the period ends, occasionally null for recent periods where the reading has not yet come in
- `intensity_index` — a qualitative label: very low, low, moderate, high, or very high

### Data quality
During backfill, 43 records (~0.1% of the dataset) were found to have a null `forecast_intensity`. These cluster on specific dates suggesting a forecasting system outage rather than random noise. These records are retained in the raw table and handled in the staging layer. Records with null `actual_intensity` are filtered out in staging as they represent incomplete settlement periods.

## Tech stack
- Python — data ingestion and backfill
- requests — API calls
- psycopg2 — database connection
- dbt — data transformation and modelling
- PostgreSQL — data storage
- Docker — containerised Postgres instance
- pytest — unit and integration testing

## Project structure
````bash
carbon-intensity/
├── src/
│   ├── pipeline.py        # ingestion and loading logic
│   ├── main.py            # entrypoint for backfill
│   └── utils/
│       └── datetime.py    # timestamp parsing and formatting
├── models/
│   ├── sources.yml        # raw source definition
│   ├── staging/
│   │   ├── stg_carbon_intensity.sql
│   │   └── schema.yml
│   └── marts/
│       ├── mart_daily_intensity.sql
│       ├── mart_daily_intensity_change.sql
│       ├── mart_hourly_intensity_by_day_of_week.sql
│       ├── mart_hourly_intensity.sql
│       ├── mart_best_and_worst_hours.sql
│       ├── mart_rolling_7_day_intensity.sql
│       ├── mart_yearly_intensity_change.sql
│       └── schema.yml
├── tests/
│   ├── unit/
│   └── integration/
├── dbt_project.yml
├── docker-compose.yml
└── requirements.txt
````

## dbt models

### Staging
`stg_carbon_intensity` — cleans and filters the raw data. Filters out records where actual intensity is null, leaving a clean dataset of complete settlement periods for downstream models to build on.

### Marts
| Model | Description |
|---|---|
| `mart_daily_intensity` | Daily average, min, max and forecast intensity aggregated from half-hourly settlement periods |
| `mart_daily_intensity_change` | Daily average intensity with day-on-day change using LAG() window function |
| `mart_hourly_intensity_by_day_of_week` | Average intensity by day of week and hour across the full dataset |
| `mart_hourly_intensity` | Average intensity by hour of day across the full dataset |
| `mart_best_and_worst_hours` | Hours of the day ranked from cleanest to dirtiest using RANK() |
| `mart_rolling_7_day_intensity` | Daily intensity with 7 day rolling average to smooth day-to-day variability |
| `mart_yearly_intensity_change` | Monthly intensity with year-on-year comparison using LAG() offset by 12 months |

## How to run

### Prerequisites
- Docker
- Python 3.11+
- dbt-postgres

### Spin up the database
```bash
docker compose up -d
```

### Install dependencies
```bash
pip install -r requirements.txt
```

### Run the backfill
Set the following environment variables in a `.env` file:
BACKFILL_START_DATE=2024-01-01T00:00Z
BACKFILL_END_DATE=2026-04-01T00:00Z
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=carbonintensity
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password

```bash
PYTHONPATH=. python src/main.py
```

### Run dbt
```bash
dbt build
```

### Run tests
```bash
pytest tests/
```

## Findings

### Carbon intensity by hour of day
Analysis of two years of data (April 2024 — April 2026) shows a clear daily intensity curve:

| Period | Hours | Avg Intensity | Explanation |
|---|---|---|---|
| Overnight trough | 00:00 — 04:00 | ~107-109 | Low demand, wind and nuclear doing most of the work |
| Morning peak | 05:00 — 07:00 | ~128-136 | Kettles, showers, lighting — demand surges quickly |
| Midday dip | 09:00 — 13:00 | ~111-120 | Demand stabilises, solar contributing in summer |
| Evening peak | 16:00 — 19:00 | ~148-158 | People returning home, cooking, heating — gas peakers firing |

The evening peak at 18:00 (158 gCO2eq/kWh) is roughly 50% higher carbon intensity than the overnight trough at 02:00 (107 gCO2eq/kWh). This means **the carbon footprint of electricity consumption varies significantly by time of day**.

### Practical implication — EV charging
Charging an electric vehicle overnight (00:00 — 04:00) rather than during the evening peak (17:00 — 19:00) reduces the carbon intensity of that charge by approximately 50 gCO2eq/kWh. For a 60kWh battery this represents a saving of around 3kg of CO2 per full charge — purely from timing the same activity differently.

### Year-on-year decarbonisation
The `mart_yearly_intensity_change` model allows comparison of the same month across years. A consistent negative `yearly_change` value across months would indicate the UK grid is getting progressively cleaner — directly visible in the data as offshore wind capacity has grown.