import requests
import psycopg2
from dotenv import load_dotenv, find_dotenv
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

load_dotenv(find_dotenv())

path = "https://api.carbonintensity.org.uk/intensity"
headers = {"Accept": "application/json"}

def extract_intensity_date_range():
    from_date = os.environ.get("BACKFILL_START_DATE")
    to_date = os.environ.get("BACKFILL_END_DATE")

    logger.info(f"Fetching data from {from_date} to {to_date}")

    r = requests.get(f"{path}/{from_date}/{to_date}", headers=headers)

    r.raise_for_status()

    data = r.json()["data"]
    logger.info(f"Fetched {len(data)} records")

    return data


def add_row_to_db(data, conn):
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
    try:
        with conn.cursor() as cur:
            cur.executemany(query, rows)
            conn.commit()
            logger.info(f"Inserted {cur.rowcount} rows, skipped duplicates.")
        
    except (Exception, psycopg2.DatabaseError) as error:
        logger.exception("Error inserting row")
        conn.rollback()



if __name__ == "__main__":
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD")
    )

    try:
        data = extract_intensity_date_range()
        
        add_row_to_db(data, conn)
    finally:
        conn.close()
