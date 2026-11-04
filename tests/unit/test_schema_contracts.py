"""Keeps the column lists that live in several places in step.

The same table definition is written out in the Spark job, the Terraform Glue table,
the Athena DDL file and (for Silver) the quality gate. Change one and forget another,
and Athena returns NULLs or the gate rejects good data. These tests read the source
files as text, so they need no Spark and no AWS.
"""

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SILVER_JOB = ROOT / "src" / "glue_jobs" / "bronze_to_silver.py"
GOLD_JOB = ROOT / "src" / "glue_jobs" / "silver_to_gold.py"
QUALITY_GATE = ROOT / "src" / "lambdas" / "data_quality" / "handler.py"
SILVER_TF = ROOT / "infra" / "modules" / "silver_tables" / "main.tf"
GOLD_TF = ROOT / "infra" / "modules" / "gold_tables" / "main.tf"
DDL_DIR = ROOT / "sql" / "ddl"

# Terraform/Athena column type -> the type family the quality gate checks for
TYPE_FAMILY = {
    "string": "string",
    "date": "date",
    "int": "integer",
    "bigint": "integer",
    "timestamp": "timestamp",
    "array<string>": "list",
    "boolean": "boolean",
    "double": "float",
}


def module_assignment(path: Path, name: str) -> ast.expr:
    """The value assigned to a module-level name, found without importing the module."""
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) == name:
            assert node.value is not None
            return node.value
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", None) == name:
            return node.value
    raise KeyError(f"{name} not found in {path}")


def silver_columns() -> list[str]:
    return ast.literal_eval(module_assignment(SILVER_JOB, "SILVER_COLUMNS"))


def gold_schemas() -> dict[str, list[tuple[str, str]]]:
    return ast.literal_eval(module_assignment(GOLD_JOB, "GOLD_SCHEMAS"))


def gate_families() -> dict[str, str]:
    """Column -> type family the quality gate expects, from its EXPECTED_TYPES dict."""
    node = module_assignment(QUALITY_GATE, "EXPECTED_TYPES")
    assert isinstance(node, ast.Dict)
    result = {}
    for key, value in zip(node.keys, node.values, strict=True):
        assert isinstance(key, ast.Constant) and isinstance(value, ast.Tuple)
        first = value.elts[0]
        assert isinstance(first, ast.Constant)
        result[key.value] = first.value
    return result


def silver_table_columns() -> list[tuple[str, str]]:
    text = SILVER_TF.read_text()
    section = text[text.index('resource "aws_glue_catalog_table" "trending_videos"') :]
    return re.findall(r'columns\s*\{\s*name\s*=\s*"(\w+)"\s*type\s*=\s*"([\w<>]+)"', section)


def gold_table_columns() -> dict[str, list[tuple[str, str]]]:
    text = GOLD_TF.read_text()
    locals_block = text[: text.index('resource "aws_glue_catalog_table"')]
    tables = {}
    for match in re.finditer(r"^\s{4}(\w+)\s*=\s*\{\n(.*?)^\s{4}\}", locals_block, re.M | re.S):
        columns = re.findall(
            r'\{\s*name\s*=\s*"(\w+)",\s*type\s*=\s*"([\w<>]+)"\s*\}', match.group(2)
        )
        tables[match.group(1)] = columns
    return tables


def ddl_columns(table: str) -> list[tuple[str, str]]:
    text = (DDL_DIR / f"gold_{table}.sql").read_text()
    body = text.split(f"{table} (", 1)[1].split("\n)", 1)[0]
    return [(name, kind) for name, kind in re.findall(r"^\s*(\w+)\s+([\w<>]+),?\s*$", body, re.M)]


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------
def test_silver_job_columns_match_the_terraform_table():
    terraform = [name for name, _ in silver_table_columns()]
    job = [name for name in silver_columns() if name != "region"]  # region is the partition
    assert terraform == job


def test_quality_gate_checks_every_silver_column():
    expected = set(silver_columns()) - {"region"}
    assert set(gate_families()) == expected


def test_quality_gate_types_agree_with_the_terraform_table():
    families = gate_families()
    for name, tf_type in silver_table_columns():
        assert TYPE_FAMILY[tf_type] == families[name], f"{name}: {tf_type}"


# ---------------------------------------------------------------------------
# Gold
# ---------------------------------------------------------------------------
def test_terraform_defines_exactly_the_gold_tables_the_job_writes():
    assert set(gold_table_columns()) == set(gold_schemas())


@pytest.mark.parametrize("table", sorted(gold_schemas()))
def test_gold_job_columns_match_the_terraform_table(table):
    assert gold_table_columns()[table] == gold_schemas()[table]


@pytest.mark.parametrize("table", sorted(gold_schemas()))
def test_gold_job_columns_match_the_ddl_file(table):
    assert ddl_columns(table) == gold_schemas()[table]


def test_every_gold_table_has_a_ddl_file():
    found = {p.stem.removeprefix("gold_") for p in DDL_DIR.glob("gold_*.sql")}
    assert found == set(gold_schemas())
