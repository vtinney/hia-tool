"""Tests for the CDC Wonder HTTP client."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backend.etl.cdc_wonder.client import CdcWonderClient


@pytest.fixture
def tmp_cache(tmp_path: Path) -> Path:
    return tmp_path / "raw"


def _mock_response(text: str, status: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.ok = status < 400
    return resp


def test_client_caches_response_to_disk(tmp_cache: Path):
    client = CdcWonderClient(cache_root=tmp_cache, request_delay=0)
    with patch("backend.etl.cdc_wonder.client.requests.post") as mock_post:
        mock_post.return_value = _mock_response("<xml>BODY</xml>")
        body = client.fetch(
            database="D158", year=2019, icd_group="cvd",
            xml_body="<req/>",
        )
        assert body == "<xml>BODY</xml>"

    cached = tmp_cache / "D158" / "2019" / "cvd.xml"
    assert cached.exists()
    assert cached.read_text() == "<xml>BODY</xml>"


def test_client_skips_http_when_cache_exists(tmp_cache: Path):
    cached = tmp_cache / "D158" / "2019" / "cvd.xml"
    cached.parent.mkdir(parents=True)
    cached.write_text("<xml>CACHED</xml>")

    client = CdcWonderClient(cache_root=tmp_cache, request_delay=0)
    with patch("backend.etl.cdc_wonder.client.requests.post") as mock_post:
        body = client.fetch(
            database="D158", year=2019, icd_group="cvd",
            xml_body="<req/>",
        )
        assert body == "<xml>CACHED</xml>"
        mock_post.assert_not_called()


def test_client_retries_on_429(tmp_cache: Path):
    client = CdcWonderClient(cache_root=tmp_cache, request_delay=0, max_retries=3)
    responses = [
        _mock_response("", status=429),
        _mock_response("", status=429),
        _mock_response("<xml>OK</xml>"),
    ]
    with patch("backend.etl.cdc_wonder.client.requests.post") as mock_post:
        mock_post.side_effect = responses
        with patch("backend.etl.cdc_wonder.client.time.sleep"):
            body = client.fetch(
                database="D158", year=2019, icd_group="cvd",
                xml_body="<req/>",
            )
        assert body == "<xml>OK</xml>"
        assert mock_post.call_count == 3


def test_client_raises_after_max_retries(tmp_cache: Path):
    client = CdcWonderClient(cache_root=tmp_cache, request_delay=0, max_retries=2)
    with patch("backend.etl.cdc_wonder.client.requests.post") as mock_post:
        mock_post.return_value = _mock_response("", status=500)
        with patch("backend.etl.cdc_wonder.client.time.sleep"):
            with pytest.raises(RuntimeError, match="CDC Wonder request failed"):
                client.fetch(
                    database="D158", year=2019, icd_group="cvd",
                    xml_body="<req/>",
                )
