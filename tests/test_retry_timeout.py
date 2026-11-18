"""30a: bronze, masks and stream_bronze retry once after a timeout too; every other step keeps retry_on_timeout off."""

import copy

import pytest
import yaml

from lh import jobs
from tests.conftest import ROOT

CODE = yaml.safe_load((ROOT / "config" / "company.yml").read_text())["code"]
ON = {"bronze", "masks", "stream_bronze"}


def built(kind, env):
    cfg = jobs.load_jobs(str(ROOT), kind)
    if not jobs.tasks_for(cfg, CODE):
        return []
    role = "developer" if env == "dev" else None
    return jobs.build(cfg, CODE, env, home="/Users/x", run_as_id="id", role=role)["tasks"]


@pytest.mark.parametrize("env", ["dev", "test", "prod"])
@pytest.mark.parametrize("kind", jobs.KINDS)
def test_only_the_three_steps_retry_after_a_timeout(kind, env):
    for t in built(kind, env):
        if t["max_retries"]:
            assert t["retry_on_timeout"] is (t["task_key"] in ON), t["task_key"]
            assert t["min_retry_interval_millis"] == 300000
        else:
            assert "retry_on_timeout" not in t, t["task_key"]


def test_the_daily_job_has_bronze_and_masks_on():
    keys = {t["task_key"] for t in built("daily", "prod") if t.get("retry_on_timeout")}
    assert keys == {"bronze", "masks"}


@pytest.mark.parametrize("bad", [False, "yes", 1])
def test_only_true_is_accepted(bad):
    cfg = copy.deepcopy(jobs.load_jobs(str(ROOT), "daily"))
    tasks = jobs.tasks_for(cfg, CODE)
    tasks[0]["retry_on_timeout"] = bad
    with pytest.raises(ValueError, match="retry_on_timeout"):
        jobs.check(tasks)


def test_not_allowed_without_retries():
    cfg = copy.deepcopy(jobs.load_jobs(str(ROOT), "daily"))
    tasks = jobs.tasks_for(cfg, CODE)
    smoke = next(t for t in tasks if t["key"] == "smoke")
    smoke["retry_on_timeout"] = True
    with pytest.raises(ValueError, match="retry_on_timeout"):
        jobs.check(tasks)
