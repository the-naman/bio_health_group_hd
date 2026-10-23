"""End-to-end scenarios on local Spark with fake Databricks and mail (tests/scenarios). Each runs in its own process
on a copy of the repo and must end with "-> PASS"."""

import os
import subprocess
import sys

import pytest

from tests.conftest import CODE, ROOT, STREAM_COMPANIES

# (scenario, alerts live in the copy, needs Spark, only for stream companies)
SCENARIOS = [
    ("notify", None, False, False),  # who gets which promotion and deploy mail
    ("dev_activity", False, False, False),  # requirement 10: dev runs of the day to Developer and Team Lead
    ("alerts", False, True, False),  # each alert kind found once, mailed once, digest
    ("observe", None, True, False),  # new source columns found in Bronze _rescued_data
    ("stream_logic", None, True, True),  # watermark, de-duplication, windows, late and broken events
    ("stream_views", None, True, True),  # stream BI views, dashboard queries, masks on the Gold windows only
]


@pytest.mark.parametrize(("name", "live", "spark", "stream_only"), SCENARIOS, ids=[s[0] for s in SCENARIOS])
def test_scenario(name, live, spark, stream_only, copy_of_repo, tmp_path):
    if stream_only and CODE not in STREAM_COMPANIES:
        pytest.skip(f"{CODE} has no stream")
    if spark:
        pytest.importorskip("pyspark")
    repo = copy_of_repo(live)
    work = tmp_path / "work"
    work.mkdir()
    script = ROOT / "tests" / "scenarios" / f"{name}.py"
    res = subprocess.run(
        [sys.executable, str(script), str(repo), str(work)],
        cwd=work,
        capture_output=True,
        text=True,
        timeout=900,
        env={**os.environ, "PYSPARK_PYTHON": sys.executable},  # Spark workers use the same Python as the test
    )
    out = res.stdout + res.stderr[-3000:]
    lines = [x for x in res.stdout.splitlines() if x.strip()]
    passed = res.returncode == 0 and lines and lines[-1].rstrip().endswith("PASS")
    assert passed and not any(x.startswith("FAIL") for x in lines), out
