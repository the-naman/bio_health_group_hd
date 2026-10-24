"""Shared test helpers (same file in both repos). Tests run from the repo root: python -m pytest -q"""

import re
import shutil
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
CODE = yaml.safe_load((ROOT / "config" / "company.yml").read_text())["code"]
STREAM_COMPANIES = yaml.safe_load((ROOT / "config" / "stream.yml").read_text()).get("companies", [])


def repo_copy(dest, alerts_live=None):
    """A throw-away copy of the repo (no .git); alerts_live=False switches scheduled alerts off in the copy."""
    shutil.copytree(ROOT, dest, ignore=shutil.ignore_patterns(".git", "__pycache__", ".ruff_cache", ".pytest_cache"))
    if alerts_live is not None:
        p = dest / "config" / "alerts.yml"
        flag = "true" if alerts_live else "false"
        text = re.sub(r"live:\s*\{[^}]*\}", f"live: {{dev: {flag}, test: {flag}, prod: {flag}}}", p.read_text())
        p.write_text(text)
    return dest


@pytest.fixture
def copy_of_repo(tmp_path):
    return lambda alerts_live=None: repo_copy(tmp_path / "repo", alerts_live)
