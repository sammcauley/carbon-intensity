from src.pipeline import transform_intensity_data

def test_transform_intensity_data():
    data = [
        {
            "from": "2024-01-01",
            "to": "2024-01-02",
            "intensity": {
                "forecast": 10,
                "actual": 12,
                "index": "low"
            }
        }
    ]

    result = transform_intensity_data(data)

    assert result == [
        ("2024-01-01", "2024-01-02", 10, 12, "low")
    ]

def test_transform_empty():
    assert transform_intensity_data([]) == []