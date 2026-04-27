from datetime import datetime, timezone

def parse_timestamp(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)

def format_for_api(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%MZ")