import requests
import logging
from datetime import timedelta
from src.utils.datetime import parse_timestamp, format_for_api


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)


PATH = "https://api.carbonintensity.org.uk/intensity"
HEADERS = {"Accept": "application/json"}
MAX_SPAN = timedelta(days=14)


def extract_intensity_date_range(from_date, to_date):

    logger.info(f"Fetching data from {from_date} to {to_date}")

    r = requests.get(f"{PATH}/{from_date}/{to_date}", headers=HEADERS)

    r.raise_for_status()

    data = r.json()["data"]
    logger.info(f"Fetched {len(data)} records")

    return data


def transform_intensity_data(data):
    return [
        (
            record["from"],
            record["to"],
            record["intensity"]["forecast"],
            record["intensity"]["actual"],
            record["intensity"]["index"],
        )
        for record in data
    ]


def add_rows_to_db(data, cur):
    query = f"""INSERT INTO raw_carbon_intensity VALUES (
        %s, %s, %s, %s, %s, NOW()
    ) ON CONFLICT (from_ts, to_ts) DO NOTHING;
    """
    rows = transform_intensity_data(data)

    cur.executemany(query, rows)
    logger.info(f"Staged {cur.rowcount} rows, skipped duplicates.")


def run_backfill(conn, from_date, to_date):
    try:
        with conn:
            with conn.cursor() as cur:
                current_start = from_date

                while current_start < to_date:
                    current_end = min(current_start + MAX_SPAN, to_date)

                    data = extract_intensity_date_range(
                        format_for_api(current_start), format_for_api(current_end)
                    )

                    add_rows_to_db(data, cur)
                    current_start = current_end

    finally:
        logger.info("Rows successfully inserted.")
        conn.close()