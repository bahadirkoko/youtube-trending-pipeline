"""Shared pytest setup."""

import sys

import pytest

# Don't litter src/ with __pycache__ when tests load Lambda code by path.
sys.dont_write_bytecode = True


@pytest.fixture(autouse=True)
def fake_aws_credentials(monkeypatch):
    """Guarantee tests can never touch a real AWS account."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)