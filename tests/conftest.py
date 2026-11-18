"""Shared pytest setup."""

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

# Don't litter src/ with __pycache__ when tests load Lambda code by path.
sys.dont_write_bytecode = True

# Glue 5.0 runs Spark on Java 17, and CI tests with Java 17.
REQUIRED_JAVA_MAJOR = 17


@pytest.fixture(autouse=True)
def fake_aws_credentials(monkeypatch):
    """Guarantee tests can never touch a real AWS account."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)


# --------------------------------------------------------------------------
# Java check for the Spark tests
# --------------------------------------------------------------------------
def java_problem() -> str | None:
    """Why Spark cannot start here, or None if Java 17 is usable.

    Finds Java the way Spark does: $JAVA_HOME/bin/java first, then `java` on the PATH.
    It actually runs `java -version`, because on macOS /usr/bin/java exists even with
    no JDK installed and only fails when run.
    """
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin" / "java") if java_home else shutil.which("java")
    if java is None:
        return "no `java` found on the PATH and JAVA_HOME is not set"
    try:
        result = subprocess.run(
            [java, "-version"], capture_output=True, text=True, timeout=30, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"could not run {java}: {exc}"
    output = (result.stderr or result.stdout).strip()
    match = re.search(r'version "(?:1\.)?(\d+)', output)
    if result.returncode != 0 or match is None:
        first_line = output.splitlines()[0] if output else "no output"
        return f"{java} does not work: {first_line}"
    major = int(match.group(1))
    if major != REQUIRED_JAVA_MAJOR:
        return f"{java} is Java {major}, but the Spark tests need Java {REQUIRED_JAVA_MAJOR}"
    return None


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    """Stop the run if Spark tests are selected but Java 17 is not usable.

    Runs after `-m`/`-k` deselection, so `make test-fast` (-m "not spark") never needs
    Java. Without this, Spark would fail inside every test with a long Java gateway error.
    """
    if not any(item.get_closest_marker("spark") for item in items):
        return
    problem = java_problem()
    if problem:
        raise pytest.UsageError(
            f"Spark tests are selected but Java {REQUIRED_JAVA_MAJOR} is not usable: "
            f"{problem.rstrip('.')}.\n"
            "Install it (macOS: brew install openjdk@17) and put it on the PATH or set "
            "JAVA_HOME. See docs/development.md.\n"
            'To run only the non-Spark tests: make test-fast  (pytest -m "not spark")'
        )
