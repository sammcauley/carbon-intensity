import pytest
import psycopg2
from src.pipeline import run_backfill
from datetime import datetime, timezone
from  unittest.mock import patch

@pytest.fixture
def test_conn(db_config):
    conn = psycopg2.connect(**db_config)

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_carbon_intensity (
                    from_ts TIMESTAMPTZ NOT NULL,
                    to_ts TIMESTAMPTZ NOT NULL,
                    forecast_intensity INT NOT NULL,
                    actual_intensity INT,
                    intensity_index VARCHAR(20) NOT NULL CHECK (
                        intensity_index IN ('very low', 'low', 'moderate', 'high', 'very high')
                    ),
                    ingestion_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (from_ts, to_ts)
                );
            """)
        
        conn.commit()

    yield conn

    conn2 = psycopg2.connect(**db_config)

    with conn2.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS raw_carbon_intensity;")
        conn2.commit()
    
    conn2.close()


@patch("src.pipeline.extract_intensity_date_range")
def test_run_backfill_inserts_rows(mock_extract, test_conn, db_config):
    mock_extract.return_value = [
        {
            "from": "2026-01-01T00:00Z",
            "to": "2026-01-02T00:00Z",
            "intensity": {"forecast": 200, "actual": 195, "index": "low"}
        }
    ]

    from_date = datetime(2026,1,1,tzinfo=timezone.utc)
    to_date = datetime(2026,1,2,tzinfo=timezone.utc)

    run_backfill(test_conn,from_date,to_date)

    # check mock was actually called
    print(f"mock called: {mock_extract.called}")
    print(f"mock call count: {mock_extract.call_count}")

    conn2 = psycopg2.connect(**db_config)

    with conn2.cursor() as cur:
        cur.execute("SELECT * FROM raw_carbon_intensity;")
        rows = cur.fetchall()
        print(f"rows in table: {rows}")
        count = len(rows)
        #cur.execute("SELECT COUNT(*) FROM raw_carbon_intensity;")
        #count = cur.fetchone()[0]
    conn2.close()

    assert count == 1


@patch("src.pipeline.extract_intensity_date_range")
def test_run_backfill_skips_duplicates(mock_extract, test_conn, db_config):
    mock_extract.return_value = [
        {
            "from": "2026-01-01T00:00Z",
            "to": "2026-01-01T00:30Z",
            "intensity": {"forecast": 200, "actual": 195, "index": "low"}
        }
    ]

    from_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    to_date = datetime(2026, 1, 2, tzinfo=timezone.utc)

    run_backfill(test_conn, from_date, to_date)
    run_backfill(test_conn, from_date, to_date)

    conn2 = psycopg2.connect(**db_config)

    with conn2.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_carbon_intensity;")
        count = cur.fetchone()[0]
    conn2.close()

    assert count == 1


@patch("src.pipeline.extract_intensity_date_range")
def test_run_backfill_rolls_back_on_failure(mock_extract, test_conn, db_config):
    mock_extract.side_effect = [
        [{"from": "2026-01-01T00:00Z", "to": "2026-01-01T00:30Z",
          "intensity": {"forecast": 200, "actual": 195, "index": "low"}}],
        Exception("API failure on second chunk")
    ]

    from_date = datetime(2026, 1, 1, tzinfo=timezone.utc)
    to_date = datetime(2026, 1, 16, tzinfo=timezone.utc)

    with pytest.raises(Exception):
        run_backfill(test_conn, from_date, to_date)

    conn2 = psycopg2.connect(**db_config)
    
    with conn2.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_carbon_intensity;")
        count = cur.fetchone()[0]
    conn2.close()

    assert count == 0