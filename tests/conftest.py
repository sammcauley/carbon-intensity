import pytest

@pytest.fixture
def sample_api_data():
    return [
        {
            "from": "2026-01-01T15:00Z",
            "to": "2026-01-02T15:30Z",
            "intensity": {
                "forecast": 210,
                "actual": 199,
                "index": "low"
            }
        },
        {
            "from": "2026-04-01T15:30Z",
            "to": "2026-04-02T16:00Z",
            "intensity": {
                "forecast": 225,
                "actual": 215,
                "index": "moderate"
            }
        }
    ]


@pytest.fixture
def sample_api_data_null_actual():
    return [
        {
            "from": "2026-01-01T15:00Z",
            "to": "2026-01-02T15:30Z",
            "intensity": {
                "forecast": 210,
                "actual": None,
                "index": "low"
            }
        }
    ]