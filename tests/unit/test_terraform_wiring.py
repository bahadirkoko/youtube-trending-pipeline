"""Checks that each environment wires the region list into every module that needs it.

Athena only sees region=XX folders that are in the tables' partition projection. If a
table module stopped getting `var.youtube_regions`, a new region would be ingested but
never be queryable. These tests read the Terraform files as text: no Terraform, no AWS.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
ENVS = ["dev", "prod"]
REGION_MODULES = ["ingestion", "silver_tables", "gold_tables"]


def module_block(env: str, name: str) -> str:
    """The body of `module "<name>" { ... }` in infra/envs/<env>/main.tf."""
    text = (ROOT / "infra" / "envs" / env / "main.tf").read_text()
    match = re.search(rf'^module "{name}" \{{\n(.*?)^\}}', text, re.MULTILINE | re.DOTALL)
    assert match, f'module "{name}" not found in infra/envs/{env}/main.tf'
    return match.group(1)


@pytest.mark.parametrize("env", ENVS)
@pytest.mark.parametrize("module", REGION_MODULES)
def test_module_gets_the_environment_region_list(env, module):
    body = module_block(env, module)
    assert re.search(r"^\s*regions\s*=\s*var\.youtube_regions\b", body, re.MULTILINE), (
        f'module "{module}" in infra/envs/{env}/main.tf must set regions = var.youtube_regions'
    )


@pytest.mark.parametrize("module", ["silver_tables", "gold_tables"])
def test_table_modules_have_no_default_region_list(module):
    """A default would let an environment forget to pass the list without any error."""
    text = (ROOT / "infra" / "modules" / module / "variables.tf").read_text()
    match = re.search(r'variable "regions" \{(.*?)\n\}', text, re.DOTALL)
    assert match, f'variable "regions" not found in infra/modules/{module}/variables.tf'
    assert "default" not in match.group(1)
