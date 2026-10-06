"""Unit tests for the YouTube ingestion Lambda (no real AWS or network calls)."""

import importlib.util
import io
import json
import logging
import urllib.error
import urllib.parse
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws

HANDLER_PATH = (
    Path(__file__).resolve().parents[2] / "src" / "lambdas" / "youtube_ingestion" / "handler.py"
)
BUCKET = "test-bronze-bucket"
API_KEY = "AIzaSy-SUPER-SECRET-KEY"
FIXED_NOW = datetime(2026, 4, 1, 12, 30, tzinfo=UTC)


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------
@pytest.fixture
def handler():
    """Load a fresh copy of the module per test (resets the API-key cache)."""
    spec = importlib.util.spec_from_file_location("youtube_ingestion_handler", HANDLER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def aws(monkeypatch):
    """Fake S3 bucket + Secrets Manager secret, and the env vars the Lambda needs."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket=BUCKET)

        secrets = boto3.client("secretsmanager", region_name="us-east-1")
        arn = secrets.create_secret(Name="yt/test/key", SecretString=API_KEY)["ARN"]

        monkeypatch.setenv("BRONZE_BUCKET", BUCKET)
        monkeypatch.setenv("YOUTUBE_SECRET_ARN", arn)
        monkeypatch.setenv("YOUTUBE_REGIONS", "US,GB")
        yield s3


def install_fake_api(handler, monkeypatch, failing_regions=(), empty_regions=()):
    """Replace the HTTP layer with canned YouTube responses."""

    def fake_get(url):
        parsed = urllib.parse.urlparse(url)
        region = urllib.parse.parse_qs(parsed.query)["regionCode"][0]
        if region in failing_regions:
            raise handler.YouTubeAPIError("HTTP 400 invalidRegion")
        if parsed.path.endswith("/videoCategories"):
            return {"kind": "youtube#videoCategoryListResponse", "items": [{"id": "10"}]}
        items = [] if region in empty_regions else [{"id": f"{region}-{i}"} for i in range(3)]
        return {"kind": "youtube#videoListResponse", "items": items}

    monkeypatch.setattr(handler, "http_get_json", fake_get)
    monkeypatch.setattr(handler, "_utc_now", lambda: FIXED_NOW)


def list_keys(s3):
    response = s3.list_objects_v2(Bucket=BUCKET)
    return sorted(obj["Key"] for obj in response.get("Contents", []))


def http_error(code, body=None):
    payload = json.dumps(body or {}).encode()
    return urllib.error.HTTPError(
        "https://example.test", code, "err", Message(), io.BytesIO(payload)
    )


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def test_trending_key_layout(handler):
    key = handler.build_trending_key("US", FIXED_NOW)
    assert key == (
        "youtube/raw_statistics/region=US/date=2026-04-01/hour=12/trending_20260401T12.json"
    )


def test_reference_key_layout(handler):
    assert handler.build_reference_key("GB") == (
        "youtube/raw_statistics_reference_data/region=GB/GB_category_id.json"
    )


def test_regions_from_event_override_env(handler, monkeypatch):
    monkeypatch.setenv("YOUTUBE_REGIONS", "US,GB")
    assert handler.resolve_regions({"regions": ["fr"]}) == ["FR"]


def test_regions_fall_back_to_env_and_dedupe(handler, monkeypatch):
    monkeypatch.setenv("YOUTUBE_REGIONS", "US, gb ,US")
    assert handler.resolve_regions({}) == ["US", "GB"]


@pytest.mark.parametrize("bad", [["USA"], ["1A"], ["../etc"], ["U"]])
def test_invalid_regions_rejected(handler, bad):
    with pytest.raises(handler.IngestionError):
        handler.resolve_regions({"regions": bad})


def test_no_regions_rejected(handler, monkeypatch):
    monkeypatch.setenv("YOUTUBE_REGIONS", "")
    with pytest.raises(handler.IngestionError):
        handler.resolve_regions({})


# --------------------------------------------------------------------------
# End-to-end handler behaviour (moto S3 + Secrets Manager)
# --------------------------------------------------------------------------
def test_successful_run_writes_expected_objects(handler, aws, monkeypatch):
    install_fake_api(handler, monkeypatch)

    result = handler.lambda_handler({"regions": ["US"]}, None)

    assert result["status"] == "SUCCESS"
    assert result["failed"] == []
    assert result["succeeded"][0]["videos"] == 3

    assert list_keys(aws) == [
        "youtube/raw_statistics/region=US/date=2026-04-01/hour=12/trending_20260401T12.json",
        "youtube/raw_statistics_reference_data/region=US/US_category_id.json",
    ]

    obj = aws.get_object(Bucket=BUCKET, Key=handler.build_trending_key("US", FIXED_NOW))
    body = json.loads(obj["Body"].read())
    assert [v["id"] for v in body["items"]] == ["US-0", "US-1", "US-2"]
    assert obj["Metadata"]["region"] == "US"
    assert obj["Metadata"]["source"] == "youtube-data-api-v3"
    assert obj["ContentType"] == "application/json"


def test_rerun_in_same_hour_overwrites_not_duplicates(handler, aws, monkeypatch):
    install_fake_api(handler, monkeypatch)
    handler.lambda_handler({"regions": ["US"]}, None)
    handler.lambda_handler({"regions": ["US"]}, None)
    assert len(list_keys(aws)) == 2  # one trending file + one reference file


def test_default_regions_come_from_env(handler, aws, monkeypatch):
    install_fake_api(handler, monkeypatch)
    result = handler.lambda_handler({}, None)
    assert [r["region"] for r in result["succeeded"]] == ["US", "GB"]


def test_partial_failure_keeps_good_regions(handler, aws, monkeypatch):
    install_fake_api(handler, monkeypatch, failing_regions={"GB"})

    result = handler.lambda_handler({"regions": ["US", "GB"]}, None)

    assert result["status"] == "PARTIAL"
    assert [r["region"] for r in result["succeeded"]] == ["US"]
    assert result["failed"][0]["region"] == "GB"
    assert any("region=US" in k for k in list_keys(aws))
    assert not any("region=GB" in k for k in list_keys(aws))


def test_all_regions_failing_raises(handler, aws, monkeypatch):
    install_fake_api(handler, monkeypatch, failing_regions={"US", "GB"})
    with pytest.raises(handler.IngestionError):
        handler.lambda_handler({"regions": ["US", "GB"]}, None)


def test_empty_video_list_counts_as_failure(handler, aws, monkeypatch):
    install_fake_api(handler, monkeypatch, empty_regions={"GB"})
    result = handler.lambda_handler({"regions": ["US", "GB"]}, None)
    assert result["status"] == "PARTIAL"
    assert "no videos" in result["failed"][0]["error"]


def test_api_key_is_never_logged(handler, aws, monkeypatch, caplog):
    install_fake_api(handler, monkeypatch, failing_regions={"GB"})
    with caplog.at_level(logging.DEBUG):
        handler.lambda_handler({"regions": ["US", "GB"]}, None)
    assert API_KEY not in caplog.text


def test_secret_is_fetched_once_per_container(handler, aws, monkeypatch):
    install_fake_api(handler, monkeypatch)
    first = handler.get_api_key()
    # delete the secret: a second call must still work because of the cache
    boto3.client("secretsmanager", region_name="us-east-1").delete_secret(
        SecretId="yt/test/key", ForceDeleteWithoutRecovery=True
    )
    assert handler.get_api_key() == first == API_KEY


# --------------------------------------------------------------------------
# HTTP retry behaviour
# --------------------------------------------------------------------------
class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return json.dumps(self._payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_http_retries_transient_errors_then_succeeds(handler, monkeypatch):
    urlopen = MagicMock(side_effect=[http_error(503), http_error(503), FakeResponse({"ok": 1})])
    monkeypatch.setattr(handler.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(handler.time, "sleep", lambda s: None)

    assert handler.http_get_json("https://example.test") == {"ok": 1}
    assert urlopen.call_count == 3


def test_http_gives_up_after_max_attempts(handler, monkeypatch):
    urlopen = MagicMock(side_effect=http_error(503))
    monkeypatch.setattr(handler.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(handler.time, "sleep", lambda s: None)

    with pytest.raises(handler.YouTubeAPIError, match="Gave up"):
        handler.http_get_json("https://example.test")
    assert urlopen.call_count == handler.MAX_ATTEMPTS


def test_quota_exceeded_is_not_retried_and_hides_url(handler, monkeypatch):
    body = {"error": {"message": "quota gone", "errors": [{"reason": "quotaExceeded"}]}}
    urlopen = MagicMock(side_effect=http_error(403, body))
    monkeypatch.setattr(handler.urllib.request, "urlopen", urlopen)

    with pytest.raises(handler.YouTubeAPIError) as excinfo:
        handler.http_get_json(f"https://example.test?key={API_KEY}")

    assert urlopen.call_count == 1
    assert "quotaExceeded" in str(excinfo.value)
    assert API_KEY not in str(excinfo.value)