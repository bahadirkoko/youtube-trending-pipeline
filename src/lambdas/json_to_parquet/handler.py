"""Category reference data: JSON (Bronze) -> Parquet (Silver).

Two Bronze sources hold YouTube video categories in the same JSON shape:
  api    youtube/raw_statistics_reference_data/region=US/US_category_id.json   (live API)
  kaggle youtube/kaggle_reference/region=US/US_category_id.json                (history)

For each region this function merges both sources (the API wins when the same
category_id appears in both) and writes ONE Parquet file to Silver:

  youtube/categories/region=US/categories.parquet
  columns: category_id (int32), category_name (string), assignable (bool)
  `region` is not stored in the file; it comes from the partition folder.

Design notes
  * Needs pyarrow, which the AWS SDK for pandas managed layer provides.
  * The output key is fixed per region, so re-running overwrites (idempotent).
  * One bad region does not stop the others. Raises only if EVERY region fails.
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION_PATTERN = re.compile(r"^[A-Z]{2}$")
REGION_FOLDER = re.compile(r"/region=([A-Z]{2})/$")

# Listed lowest priority first: later sources overwrite earlier ones.
SOURCES = (
    ("kaggle", "youtube/kaggle_reference"),
    ("api", "youtube/raw_statistics_reference_data"),
)
SILVER_PREFIX = "youtube/categories"

SCHEMA = pa.schema(
    [
        pa.field("category_id", pa.int32()),
        pa.field("category_name", pa.string()),
        pa.field("assignable", pa.bool_()),
    ]
)


class CategoryError(Exception):
    """The run cannot proceed (bad input, or every region failed)."""


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def source_key(prefix: str, region: str) -> str:
    return f"{prefix}/region={region}/{region}_category_id.json"


def silver_key(region: str) -> str:
    return f"{SILVER_PREFIX}/region={region}/categories.parquet"


def parse_categories(payload: dict[str, Any]) -> dict[int, dict[str, Any]]:
    """Return {category_id: row}. Skips items without a numeric id or a title."""
    rows: dict[int, dict[str, Any]] = {}
    for item in payload.get("items") or []:
        raw_id = str(item.get("id", "")).strip()
        snippet = item.get("snippet") or {}
        title = snippet.get("title")
        if not raw_id.isdigit() or not title:
            logger.warning("Skipping malformed category item: id=%r", raw_id)
            continue
        rows[int(raw_id)] = {
            "category_id": int(raw_id),
            "category_name": str(title),
            "assignable": bool(snippet.get("assignable", False)),
        }
    return rows


def merge_sources(
    parsed_in_priority_order: list[dict[int, dict[str, Any]]],
) -> list[dict[str, Any]]:
    merged: dict[int, dict[str, Any]] = {}
    for rows in parsed_in_priority_order:  # later (higher priority) overwrites earlier
        merged.update(rows)
    return [merged[cid] for cid in sorted(merged)]


def to_parquet_bytes(rows: list[dict[str, Any]]) -> bytes:
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


# --------------------------------------------------------------------------
# S3 access
# --------------------------------------------------------------------------
def read_json(s3: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """Return parsed JSON, or None if the object does not exist."""
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(body.decode("utf-8"))


def discover_regions(s3: Any, bucket: str) -> list[str]:
    """Regions that have a category file in ANY source (folders named region=XX)."""
    found: set[str] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for _, prefix in SOURCES:
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/", Delimiter="/"):
            for common in page.get("CommonPrefixes", []):
                match = REGION_FOLDER.search(common["Prefix"])
                if match:
                    found.add(match.group(1))
    return sorted(found)


def resolve_regions(event: dict[str, Any] | None) -> list[str] | None:
    """Regions from the event, validated. None means 'discover them from S3'."""
    raw = (event or {}).get("regions")
    if not raw:
        return None
    if isinstance(raw, str):
        raw = raw.split(",")
    regions = [str(r).strip().upper() for r in raw if str(r).strip()]
    invalid = [r for r in regions if not REGION_PATTERN.match(r)]
    if invalid:
        raise CategoryError(f"Invalid region code(s): {invalid}. Expected 2 letters, e.g. US.")
    return list(dict.fromkeys(regions))


# --------------------------------------------------------------------------
# Per-region workflow + entry point
# --------------------------------------------------------------------------
def convert_region(s3: Any, bronze: str, silver: str, region: str) -> dict[str, Any]:
    parsed = []
    used = []
    for name, prefix in SOURCES:
        payload = read_json(s3, bronze, source_key(prefix, region))
        if payload is None:
            continue
        rows = parse_categories(payload)
        if rows:
            parsed.append(rows)
            used.append(name)

    if not parsed:
        raise CategoryError("no usable category data in any source")

    rows = merge_sources(parsed)
    key = silver_key(region)
    s3.put_object(
        Bucket=silver,
        Key=key,
        Body=to_parquet_bytes(rows),
        ContentType="application/octet-stream",
    )
    return {"region": region, "categories": len(rows), "sources": used, "key": key}


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    bronze = os.environ["BRONZE_BUCKET"]
    silver = os.environ["SILVER_BUCKET"]
    s3 = boto3.client("s3")

    regions = resolve_regions(event)
    if regions is None:
        regions = discover_regions(s3, bronze)
    if not regions:
        raise CategoryError("No category files found in Bronze and no regions were given.")

    logger.info("Converting categories for regions=%s", regions)

    succeeded: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    for region in regions:
        try:
            result = convert_region(s3, bronze, silver, region)
        except (CategoryError, ClientError, ValueError) as exc:
            logger.error("Region %s failed: %s", region, exc)
            failed.append({"region": region, "error": str(exc)})
        else:
            logger.info(
                "Region %s ok: %s categories (%s)", region, result["categories"], result["sources"]
            )
            succeeded.append(result)

    if not succeeded:
        raise CategoryError(f"All regions failed: {failed}")

    return {
        "status": "SUCCESS" if not failed else "PARTIAL",
        "silver_bucket": silver,
        "succeeded": succeeded,
        "failed": failed,
    }