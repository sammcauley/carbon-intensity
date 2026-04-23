import requests
import psycopg2
from dotenv import load_dotenv, find_dotenv
import os
import logging
from datetime import timedelta
from utils.datetime import parse_timestamp, format_for_api

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

load_dotenv(find_dotenv())

path = "https://api.carbonintensity.org.uk/intensity"
headers = {"Accept": "application/json"}


def extract_intensity_date_range(from_date, to_date):
    
    logger.info(f"Fetching data from {from_date} to {to_date}")

    r = requests.get(f"{path}/{from_date}/{to_date}", headers=headers)

    r.raise_for_status()

    data = r.json()["data"]
    logger.info(f"Fetched {len(data)} records")

    return data


def add_rows_to_db(data, cur):
    query = f"""INSERT INTO raw_carbon_intensity VALUES (
        %s, %s, %s, %s, %s, NOW()
    ) ON CONFLICT (from_ts, to_ts) DO NOTHING;
    """
    rows = [
        (
            record["from"],
            record["to"],
            record["intensity"]["forecast"],
            record["intensity"]["actual"],
            record["intensity"]["index"]
        )
        for record in data
    ]

    cur.executemany(query, rows)
    logger.info(f"Staged {cur.rowcount} rows, skipped duplicates.")


if __name__ == "__main__":
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD")
    )

    from_date_str = os.environ.get("BACKFILL_START_DATE")
    to_date_str = os.environ.get("BACKFILL_END_DATE")

    from_date = parse_timestamp(from_date_str)
    to_date = parse_timestamp(to_date_str)

    if from_date > to_date:
        raise ValueError("BACKFILL_START_DATE must be before BACKFILL_END_DATE")

    max_span = timedelta(days=14)

    try:
        with conn:
            with conn.cursor() as cur:
                current_start = from_date

                while current_start < to_date:
                    current_end = min(current_start + max_span, to_date)

                    data = extract_intensity_date_range(
                        format_for_api(current_start),
                        format_for_api(current_end)
                    )

                    add_rows_to_db(data, cur)
                    current_start = current_end

    finally:
        logger.info("Rows successfully inserted.")
        conn.close()
