"""ci/flow.py: which pull request brought a deployed commit; retries when GitHub is busy, never fails the deploy."""

import io
import json
import runpy
import urllib.error

import pytest

from tests.conftest import CODE, ROOT


def run(monkeypatch, tmp_path, answers):
    """answers: one item per call to GitHub: a list of pull requests, an HTTP status code, or an exception."""
    calls, sleeps = [], []

    def urlopen(req, timeout):
        a = answers[min(len(calls), len(answers) - 1)]
        calls.append(req.full_url)
        if isinstance(a, int):
            raise urllib.error.HTTPError(req.full_url, a, "x", {}, None)
        if isinstance(a, Exception):
            raise a
        return io.BytesIO(json.dumps(a).encode())

    out = tmp_path / "out.txt"
    monkeypatch.chdir(ROOT)
    monkeypatch.setattr("urllib.request.urlopen", urlopen)
    monkeypatch.setattr("time.sleep", sleeps.append)
    for k, v in {
        "GITHUB_TOKEN": "t",
        "GITHUB_REPOSITORY": "o/r",
        "GITHUB_SHA": "abc1234def",
        "GITHUB_REF_NAME": f"{CODE}_prod",
        "GITHUB_OUTPUT": str(out),
    }.items():
        monkeypatch.setenv(k, v)
    runpy.run_path(str(ROOT / "ci" / "flow.py"), run_name="__main__")
    return dict(line.split("=", 1) for line in out.read_text().split()), len(calls), sleeps


def pr(head, number=7, base=None):
    return {
        "merged_at": "2026-10-24T00:00:00Z",
        "number": number,
        "head": {"ref": head},
        "base": {"ref": base or f"{CODE}_prod"},
    }


def test_hotfix_found_first_time(monkeypatch, tmp_path):
    out, calls, sleeps = run(monkeypatch, tmp_path, [[pr(f"{CODE}_hotfix_fix1")]])
    assert out == {"flow": "hotfix", "head": f"{CODE}_hotfix_fix1", "pr": "7"} and calls == 1 and sleeps == []


def test_promotion_found_first_time(monkeypatch, tmp_path):
    out, calls, _ = run(monkeypatch, tmp_path, [[pr(f"{CODE}_test"), pr("other", 9, base="x")]])
    assert out["flow"] == "promotion" and out["pr"] == "7" and calls == 1


@pytest.mark.parametrize("busy", [[], 502, 429, TimeoutError()])
def test_retries_while_github_is_busy_or_not_linked_yet(monkeypatch, tmp_path, busy):
    out, calls, sleeps = run(monkeypatch, tmp_path, [busy, busy, [pr(f"{CODE}_hotfix_x")]])
    assert out["flow"] == "hotfix" and calls == 3 and sleeps == [5, 10]


def test_a_4xx_answer_is_final_and_the_deploy_goes_on(monkeypatch, tmp_path):
    out, calls, sleeps = run(monkeypatch, tmp_path, [404])
    assert out == {"flow": "promotion", "head": "", "pr": ""} and calls == 1 and sleeps == []


def test_gives_up_after_three_tries_as_a_promotion(monkeypatch, tmp_path):
    out, calls, sleeps = run(monkeypatch, tmp_path, [503])
    assert out["flow"] == "promotion" and calls == 3 and sleeps == [5, 10]
