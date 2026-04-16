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

def extract_one_week():
    from_date = "2026-04-09T15:00Z"
    to_date = "2026-04-16T15:00Z"

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
    try:
        with conn.cursor() as cur:
            cur.execute(query, (
                data['from'],
                data['to'],
                data['intensity']['forecast'],
                data['intensity']['actual'],
                data['intensity']['index']
            ))

            conn.commit()

            if cur.rowcount == 0:
                logger.info("Duplicate skipped")
            else:
                logger.info("Row inserted")
        
    except (Exception, psycopg2.DatabaseError) as error:
        logger.exception("Error inserting row")
        conn.rollback()



if __name__ == "__main__":
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="carbonintensity",
        user="sm",
        password=os.environ.get("POSTGRES_PASSWORD")
    )

    try:
        data = extract_one_week()
        
        for record in data:
            add_row_to_db(record, conn)
    finally:
        conn.close()
