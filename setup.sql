CREATE TABLE raw_carbon_intensity (
    from_ts TIMESTAMPTZ NOT NULL,
    to_ts TIMESTAMPTZ NOT NULL,
    forecast_intensity INT,
    actual_intensity INT,
    intensity_index VARCHAR(20) NOT NULL CHECK (
        intensity_index IN ('very low', 'low', 'moderate', 'high', 'very high')
    ),
    ingestion_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (from_ts, to_ts)
);

CREATE DATABASE carbonintensity_test;