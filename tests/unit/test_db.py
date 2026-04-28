from src.pipeline import add_rows_to_db
from unittest.mock import MagicMock

def test_add_rows_to_db(sample_api_data):
    cur = MagicMock()

    add_rows_to_db(sample_api_data, cur)

    cur.executemany.assert_called_once()


def test_add_rows_to_db_maps_cols_correctly(sample_api_data):
    cur = MagicMock()

    add_rows_to_db(sample_api_data, cur)

    call_args = cur.executemany.call_args
    rows = call_args.args[1]

    first_row = rows[0]
    assert first_row[0] == "2026-01-01T15:00Z"
    assert first_row[1] == "2026-01-02T15:30Z"
    assert first_row[2] == 210
    assert first_row[3] == 199
    assert first_row[4] == "low"


def test_add_rows_to_db_handles_null_actual(sample_api_data_null_actual):
    cur = MagicMock()

    add_rows_to_db(sample_api_data_null_actual, cur)

    call_args = cur.executemany.call_args
    rows = call_args.args[1]
    assert rows[0][3] is None


def test_add_rows_to_db_inserts_all_records(sample_api_data):
    cur = MagicMock()

    add_rows_to_db(sample_api_data, cur)

    call_args = cur.executemany.call_args
    rows = call_args.args[1]
    assert len(rows) == len(sample_api_data)