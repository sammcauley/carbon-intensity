from  unittest.mock import patch, MagicMock
from src.pipeline import extract_intensity_date_range
import pytest
from requests.exceptions import HTTPError

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
def test_extract_raises_on_http_error(mock_get):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")
    mock_get.return_value = mock_response

    with pytest.raises(HTTPError):
        extract_intensity_date_range("2026-01-01T00:00Z", "2026-01-02T00:00Z")


@patch("src.pipeline.requests.get")
def test_extract_calls_correct_url(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": []}
    mock_get.return_value = mock_response

    extract_intensity_date_range("2026-01-01T00:00Z", "2026-01-02T00:00Z")

    mock_get.assert_called_once_with(
        "https://api.carbonintensity.org.uk/intensity/2026-01-01T00:00Z/2026-01-02T00:00Z",
        headers={"Accept": "application/json"}
    )


@patch("src.pipeline.requests.get")
def test_extract_empty_data(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": []}
    mock_get.return_value = mock_response

    result = extract_intensity_date_range("2026-01-01T00:00Z", "2026-01-02T00:00Z")

    assert result == []


@patch("src.pipeline.requests.get")
def test_extract_calls_raise_for_status(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": []}
    mock_get.return_value = mock_response

    extract_intensity_date_range("2026-01-01T00:00Z", "2026-01-02T00:00Z")

    mock_response.raise_for_status.assert_called_once()