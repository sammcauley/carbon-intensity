from src.pipeline import add_rows_to_db
from unittest.mock import MagicMock

def test_add_rows_to_db():
    cur = MagicMock()

    data = [{
        "from": "a",
        "to": "b",
        "intensity": {"forecast": 1, "actual": 2, "index": "low"}
    }]

    add_rows_to_db(data, cur)

    cur.executemany.assert_called_once()