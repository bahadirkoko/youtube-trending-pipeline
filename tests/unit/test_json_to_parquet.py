"""Unit tests for the json_to_parquet Lambda (fake S3 only, no real AWS)."""

import importlib.util
import io
import json
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

HANDLER_PATH = (
    Path(__file__).resolve().parents[2] / "src" / "lambdas" / "json_to_parquet" / "handler.py"
)
BRONZE = "test-bronze"
SILVER = "test-silver"


def category_json(*items):
    """Build a payload shaped like the YouTube videoCategories response."""
    return {
        "kind": "youtube#videoCategoryListResponse",
        "items": [
            {"id": cid, "snippet": {"title": title, "assignable": assignable}}
            for cid, title, assignable in items
        ],
    }


@pytest.fixture
def handler():
    spec = importlib.util.spec_from_file_location("json_to_parquet_handler", HANDLER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def s3(monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BRONZE)
        client.create_bucket(Bucket=SILVER)
        monkeypatch.setenv("BRONZE_BUCKET", BRONZE)
        monkeypatch.setenv("SILVER_BUCKET", SILVER)
        yield client


def put(s3, handler, source, region, payload):
    prefix = dict(handler.SOURCES)[source]
    s3.put_object(
        Bucket=BRONZE,
        Key=handler.source_key(prefix, region),
        Body=json.dumps(payload).encode(),
    )


def read_silver(s3, region):
    body = s3.get_object(
        Bucket=SILVER, Key=f"youtube/categories/region={region}/categories.parquet"
    )
    return pq.read_table(io.BytesIO(body["Body"].read()))


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------
def test_keys(handler):
    assert handler.silver_key("US") == "youtube/categories/region=US/categories.parquet"
    assert handler.source_key("youtube/kaggle_reference", "GB") == (
        "youtube/kaggle_reference/region=GB/GB_category_id.json"
    )


def test_parse_skips_malformed_items(handler):
    payload = {
        "items": [
            {"id": "10", "snippet": {"title": "Music", "assignable": True}},
            {"id": "abc", "snippet": {"title": "Bad id"}},
            {"id": "12", "snippet": {}},
            {"snippet": {"title": "No id"}},
            {"id": "13", "snippet": {"title": "No assignable flag"}},
        ]
    }
    rows = handler.parse_categories(payload)
    assert sorted(rows) == [10, 13]
    assert rows[13]["assignable"] is False


def test_parse_handles_empty_payload(handler):
    assert handler.parse_categories({}) == {}
    assert handler.parse_categories({"items": None}) == {}


def test_merge_later_source_wins_and_is_sorted(handler):
    low = {
        2: {"category_id": 2, "category_name": "Old", "assignable": True},
        1: {"category_id": 1, "category_name": "Film", "assignable": True},
    }
    high = {2: {"category_id": 2, "category_name": "New", "assignable": False}}
    merged = handler.merge_sources([low, high])
    assert [r["category_id"] for r in merged] == [1, 2]
    assert merged[1]["category_name"] == "New"


@pytest.mark.parametrize("bad", [["USA"], ["1A"], ["../x"]])
def test_invalid_regions_rejected(handler, bad):
    with pytest.raises(handler.CategoryError):
        handler.resolve_regions({"regions": bad})


def test_regions_normalised(handler):
    assert handler.resolve_regions({"regions": "us, gb ,US"}) == ["US", "GB"]
    assert handler.resolve_regions({}) is None


# --------------------------------------------------------------------------
# End-to-end (moto S3)
# --------------------------------------------------------------------------
def test_kaggle_only_region(handler, s3):
    put(
        s3,
        handler,
        "kaggle",
        "CA",
        category_json(("1", "Film & Animation", True), ("2", "Autos", True)),
    )

    result = handler.lambda_handler({"regions": ["CA"]}, None)

    assert result["status"] == "SUCCESS"
    assert result["succeeded"][0]["sources"] == ["kaggle"]
    table = read_silver(s3, "CA")
    assert table.num_rows == 2
    assert table.column("category_name").to_pylist() == ["Film & Animation", "Autos"]


def test_output_schema_and_compression(handler, s3):
    put(s3, handler, "api", "US", category_json(("10", "Music", True)))
    handler.lambda_handler({"regions": ["US"]}, None)

    body = s3.get_object(Bucket=SILVER, Key=handler.silver_key("US"))["Body"].read()
    parquet_file = pq.ParquetFile(io.BytesIO(body))
    assert parquet_file.schema_arrow.equals(handler.SCHEMA)
    assert parquet_file.schema_arrow.field("category_id").type == pa.int32()
    assert parquet_file.metadata.row_group(0).column(0).compression == "SNAPPY"
    assert "region" not in parquet_file.schema_arrow.names  # comes from the folder instead


def test_api_overrides_kaggle_and_sources_are_merged(handler, s3):
    put(s3, handler, "kaggle", "US", category_json(("1", "Film", True), ("2", "Old name", True)))
    put(s3, handler, "api", "US", category_json(("2", "New name", True), ("10", "Music", True)))

    result = handler.lambda_handler({"regions": ["US"]}, None)

    assert result["succeeded"][0]["sources"] == ["kaggle", "api"]
    table = read_silver(s3, "US")
    assert table.column("category_id").to_pylist() == [1, 2, 10]
    assert table.column("category_name").to_pylist() == ["Film", "New name", "Music"]


def test_regions_are_discovered_from_both_sources(handler, s3):
    put(s3, handler, "kaggle", "CA", category_json(("1", "Film", True)))
    put(s3, handler, "kaggle", "US", category_json(("1", "Film", True)))
    put(s3, handler, "api", "US", category_json(("10", "Music", True)))
    put(s3, handler, "api", "GB", category_json(("10", "Music", True)))

    result = handler.lambda_handler({}, None)

    assert [r["region"] for r in result["succeeded"]] == ["CA", "GB", "US"]


def test_rerun_overwrites_instead_of_duplicating(handler, s3):
    put(s3, handler, "api", "US", category_json(("10", "Music", True)))
    handler.lambda_handler({"regions": ["US"]}, None)
    handler.lambda_handler({"regions": ["US"]}, None)

    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=SILVER)["Contents"]]
    assert keys == ["youtube/categories/region=US/categories.parquet"]
    assert read_silver(s3, "US").num_rows == 1


def test_partial_failure_keeps_good_regions(handler, s3):
    put(s3, handler, "api", "US", category_json(("10", "Music", True)))
    # DE has a file, but nothing usable in it
    put(s3, handler, "api", "DE", {"items": [{"id": "x", "snippet": {"title": "Bad"}}]})

    result = handler.lambda_handler({"regions": ["US", "DE", "FR"]}, None)

    assert result["status"] == "PARTIAL"
    assert [r["region"] for r in result["succeeded"]] == ["US"]
    assert sorted(f["region"] for f in result["failed"]) == ["DE", "FR"]


def test_all_regions_failing_raises(handler, s3):
    with pytest.raises(handler.CategoryError):
        handler.lambda_handler({"regions": ["US"]}, None)


def test_empty_bronze_raises(handler, s3):
    with pytest.raises(handler.CategoryError, match="No category files"):
        handler.lambda_handler({}, None)