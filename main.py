import requests

path = "https://api.carbonintensity.org.uk/intensity"
headers = {"Accept": "application/json"}


def extract_one_week():
    from_date = "2026-04-02T15:00Z"
    to_date = "2026-04-09T15:00Z"

    r = requests.get(f"{path}/{from_date}/{to_date}", headers=headers)

    r.raise_for_status()

    return r.json()["data"]


if __name__ == "__main__":
    data = extract_one_week()
    for record in data:
        print(record)
