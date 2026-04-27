from  unittest.mock import patch, MagicMock
from src.pipeline import extract_intensity_date_range
import pytest

@patch("src.pipeline.requests.get")
def test_extract_success(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "data": [{"from": "a"}]
    }
    mock_response.raise_for_status = MagicMock()

    mock_get.return_value = mock_response

    result = extract_intensity_date_range("2026-01-01", "2026-01-02")

    assert result == [{"from": "a"}]

    mock_get.assert_called_once()


@patch("src.pipeline.requests.get")
def test_extract_http_error(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("API error")
    mock_get.return_value = mock_response

    with pytest.raises(Exception):
        extract_intensity_date_range("a", "b")