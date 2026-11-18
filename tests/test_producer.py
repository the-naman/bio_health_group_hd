"""30a: the payment producer (FQF only) plants the velocity burst only in the configured IST hours when it runs on a
schedule or from the VPS timer; a send by hand always plants it. Dry runs only: nothing is sent."""

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
pytestmark = pytest.mark.skipif(not (ROOT / "stream" / "producer.py").exists(), reason="the stream is FQF only")


def dry(*args):
    out = subprocess.run(
        [sys.executable, "-m", "stream.producer", "--env", "test", "--dry-run", *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return out.splitlines()[0]


def test_burst_hours_are_two_per_day():
    hours = yaml.safe_load((ROOT / "config" / "stream.yml").read_text())["producer"]["burst_hours_ist"]
    assert hours == [8, 16]


@pytest.mark.parametrize(
    ("utc", "burst"),
    [
        ("2026-11-18T01:04:00Z", False),  # 06:34 IST
        ("2026-11-18T03:04:00Z", True),  # 08:34 IST
        ("2026-11-18T05:04:00Z", False),  # 10:34 IST
        ("2026-11-18T10:34:00Z", True),  # 16:04 IST
        ("2026-11-18T15:04:00Z", False),
    ],  # 20:34 IST
)
def test_scheduled_sends_plant_the_burst_only_in_burst_hours(utc, burst):
    line = dry("--scheduled", "--at", utc)
    assert ("4 burst" in line) is burst and (("no burst" in line) is not burst)


def test_a_send_by_hand_always_plants_the_burst():
    assert "4 burst" in dry("--at", "2026-11-18T01:04:00Z")
