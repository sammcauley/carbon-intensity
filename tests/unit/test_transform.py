from src.pipeline import transform_intensity_data

def test_transform_intensity_data(sample_api_data):
    result = transform_intensity_data(sample_api_data)

    assert result == [
        ("2026-01-01T15:00Z", "2026-01-02T15:30Z", 210, 199, "low"),
        ("2026-04-01T15:30Z", "2026-04-02T16:00Z", 225, 215, "moderate")
    ]

def test_transform_empty():
    assert transform_intensity_data([]) == []