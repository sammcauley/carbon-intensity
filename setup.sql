CREATE TABLE raw_carbon_intensity (
    from_ts TIMESTAMPTZ,
    to_ts TIMESTAMPTZ,
    forecast_intensity INT,
    actual_intensity INT,
    intensity_index VARCHAR(20) CHECK (
        intensity_index IN ('very low', 'low', 'moderate', 'high', 'very high')
    ),
    ingestion_ts TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (from_ts, to_ts)
);

