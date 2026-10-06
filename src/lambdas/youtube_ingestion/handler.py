"""YouTube trending ingestion Lambda (Bronze layer).

For every requested region this function:
  1. fetches the most-popular videos   (YouTube Data API v3  videos.list)
  2. fetches the video category list   (videoCategories.list)
  3. stores both responses, unmodified, in the Bronze S3 bucket

S3 layout (matches the original project and the Kaggle backfill):
  youtube/raw_statistics/region=US/date=2026-04-01/hour=12/trending_20260401T12.json
  youtube/raw_statistics_reference_data/region=US/US_category_id.json

Design notes
  * Standard library + boto3 only, so there is nothing to package or layer.
  * The API key comes from Secrets Manager and is never logged.
  * Object keys are deterministic per region/hour, so a retry overwrites
    instead of duplicating (idempotent).
  * One failing region does not stop the others. The function raises only
    if EVERY region fails; otherwise it reports status "PARTIAL".
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# SECURITY: at DEBUG level botocore logs full response bodies, which would write
# the Secrets Manager value (our API key) into CloudWatch. Keep SDK logs quiet
# no matter what LOG_LEVEL is set to.
for _noisy_logger in ("boto3", "botocore", "urllib3"):
    logging.getLogger(_noisy_logger).setLevel(logging.WARNING)

API_BASE = "https://www.googleapis.com/youtube/v3"
MAX_RESULTS = 50  # API maximum per page
HTTP_TIMEOUT_SECONDS = 10
MAX_ATTEMPTS = 3
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
REGION_PATTERN = re.compile(r"^[A-Z]{2}$")

RAW_PREFIX = "youtube/raw_statistics"
REFERENCE_PREFIX = "youtube/raw_statistics_reference_data"

_api_key_cache: str | None = None  # reused across warm invocations


class IngestionError(Exception):
    """The run cannot proceed (bad input, or every region failed)."""


class YouTubeAPIError(Exception):
    """A single YouTube API call failed or returned unusable data."""


# --------------------------------------------------------------------------
# Small helpers (pure functions: easy to unit test)
# --------------------------------------------------------------------------
def _utc_now() -> datetime:
    return datetime.now(UTC)


def resolve_regions(event: dict[str, Any] | None) -> list[str]:
    """Regions come from the event if given, else the YOUTUBE_REGIONS env var."""
    raw = (event or {}).get("regions") or os.environ.get("YOUTUBE_REGIONS", "")
    if isinstance(raw, str):
        raw = raw.split(",")

    regions = [str(r).strip().upper() for r in raw if str(r).strip()]
    if not regions:
        raise IngestionError("No regions provided (event 'regions' or YOUTUBE_REGIONS).")

    invalid = [r for r in regions if not REGION_PATTERN.match(r)]
    if invalid:
        raise IngestionError(f"Invalid region code(s): {invalid}. Expected 2 letters, e.g. US.")

    return list(dict.fromkeys(regions))  # de-duplicate, keep order


def build_trending_key(region: str, now: datetime) -> str:
    return (
        f"{RAW_PREFIX}/region={region}/date={now:%Y-%m-%d}/hour={now:%H}/"
        f"trending_{now:%Y%m%dT%H}.json"
    )


def build_reference_key(region: str) -> str:
    return f"{REFERENCE_PREFIX}/region={region}/{region}_category_id.json"


# --------------------------------------------------------------------------
# Secrets Manager
# --------------------------------------------------------------------------
def get_api_key() -> str:
    global _api_key_cache
    if _api_key_cache is None:
        secret_arn = os.environ["YOUTUBE_SECRET_ARN"]
        client = boto3.client("secretsmanager")
        value = client.get_secret_value(SecretId=secret_arn)["SecretString"]
        _api_key_cache = value.strip()
    return _api_key_cache


# --------------------------------------------------------------------------
# YouTube API access
# --------------------------------------------------------------------------
def _describe_http_error(exc: urllib.error.HTTPError) -> str:
    """Turn an HTTP error into a short message WITHOUT echoing the URL (it has the key)."""
    detail = ""
    try:
        body = json.loads(exc.read().decode("utf-8"))
        error = body.get("error", {})
        reason = (error.get("errors") or [{}])[0].get("reason", "")
        detail = f"{reason}: {error.get('message', '')}".strip(": ")
    except (ValueError, OSError):
        pass
    return f"HTTP {exc.code} {detail}".strip()


def http_get_json(url: str) -> dict[str, Any]:
    """GET a URL and parse JSON. Retries transient failures with backoff."""
    last_error: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:  # must come before URLError (subclass)
            if exc.code not in RETRYABLE_STATUS:
                # e.g. 400 bad request, 403 quotaExceeded / invalid key: retrying won't help
                raise YouTubeAPIError(_describe_http_error(exc)) from None
            last_error = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc

        if attempt < MAX_ATTEMPTS:
            time.sleep(2 ** (attempt - 1))  # 1s, 2s

    raise YouTubeAPIError(f"Gave up after {MAX_ATTEMPTS} attempts: {last_error}")


def _api_url(endpoint: str, params: dict[str, Any], api_key: str) -> str:
    query = urllib.parse.urlencode({**params, "key": api_key})
    return f"{API_BASE}/{endpoint}?{query}"


def fetch_trending_videos(api_key: str, region: str) -> dict[str, Any]:
    params = {
        "part": "snippet,contentDetails,statistics",
        "chart": "mostPopular",
        "regionCode": region,
        "maxResults": MAX_RESULTS,
    }
    return http_get_json(_api_url("videos", params, api_key))


def fetch_categories(api_key: str, region: str) -> dict[str, Any]:
    params = {"part": "snippet", "regionCode": region, "hl": "en_US"}
    return http_get_json(_api_url("videoCategories", params, api_key))


# --------------------------------------------------------------------------
# S3 + per-region workflow
# --------------------------------------------------------------------------
def put_json(
    s3: Any, bucket: str, key: str, payload: dict[str, Any], metadata: dict[str, str]
) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        Metadata=metadata,
    )


def ingest_region(s3: Any, bucket: str, api_key: str, region: str, now: datetime) -> dict[str, Any]:
    videos = fetch_trending_videos(api_key, region)
    items = videos.get("items") or []
    if not items:
        raise YouTubeAPIError("API returned no videos for this region")

    categories = fetch_categories(api_key, region)

    metadata = {
        "region": region,
        "fetched-at": now.isoformat(),
        "source": "youtube-data-api-v3",
    }
    trending_key = build_trending_key(region, now)
    reference_key = build_reference_key(region)

    put_json(s3, bucket, trending_key, videos, metadata)
    put_json(s3, bucket, reference_key, categories, metadata)

    return {
        "region": region,
        "videos": len(items),
        "trending_key": trending_key,
        "reference_key": reference_key,
    }


# --------------------------------------------------------------------------
# Lambda entry point
# --------------------------------------------------------------------------
def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    bucket = os.environ["BRONZE_BUCKET"]
    regions = resolve_regions(event)
    api_key = get_api_key()
    now = _utc_now()
    s3 = boto3.client("s3")

    logger.info("Starting ingestion for regions=%s bucket=%s", regions, bucket)

    succeeded: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []

    for region in regions:
        try:
            result = ingest_region(s3, bucket, api_key, region, now)
        except (YouTubeAPIError, ClientError) as exc:
            logger.error("Region %s failed: %s", region, exc)
            failed.append({"region": region, "error": str(exc)})
        else:
            logger.info(
                "Region %s ok: %s videos -> %s", region, result["videos"], result["trending_key"]
            )
            succeeded.append(result)

    if not succeeded:
        raise IngestionError(f"All regions failed: {failed}")

    return {
        "status": "SUCCESS" if not failed else "PARTIAL",
        "run_time": now.isoformat(),
        "bucket": bucket,
        "succeeded": succeeded,
        "failed": failed,
    }