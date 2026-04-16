import requests
import psycopg2
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())

path = "https://api.carbonintensity.org.uk/intensity"
headers = {"Accept": "application/json"}


def extract_one_week():
    from_date = "2026-04-02T15:00Z"
    to_date = "2026-04-09T15:00Z"

    r = requests.get(f"{path}/{from_date}/{to_date}", headers=headers)

    r.raise_for_status()

    return r.json()["data"]


def add_row_to_db(data):
    query = f"""INSERT INTO raw_carbon_intensity VALUES (
        %s, %s, %s, %s, %s, NOW()
    )
    """
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="carbonintensity",
        user="sm",
        password=os.environ.get("POSTGRES_PASSWORD")
    )
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
        
        print("Row successfully inserted.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    data = extract_one_week()
    for record in data:
        add_row_to_db(record)
