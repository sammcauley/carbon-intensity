import psycopg2
from dotenv import load_dotenv, find_dotenv
import os
import logging
from src.utils.datetime import parse_timestamp
from src.pipeline import run_backfill

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

load_dotenv(find_dotenv())


def get_env_dates():
    from_date_str = os.environ.get("BACKFILL_START_DATE")
    to_date_str = os.environ.get("BACKFILL_END_DATE")

    from_date = parse_timestamp(from_date_str)
    to_date = parse_timestamp(to_date_str)

    if from_date > to_date:
        raise ValueError("BACKFILL_START_DATE must be before BACKFILL_END_DATE")
    
    return from_date, to_date


if __name__ == "__main__":
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", 5432),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
    )

    from_date, to_date = get_env_dates()
    try:
        run_backfill(conn, from_date, to_date)
    finally:
        conn.close()
