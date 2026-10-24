"""Asset Bundle (docs/adr/0011): databricks.yml is generated from the job config and matches it (no Spark)."""

import re

import pytest
import yaml

from lh import bundle, jobs
from tests.conftest import ROOT

CODE = yaml.safe_load((ROOT / "config" / "company.yml").read_text())["code"]
DOC = bundle.build(str(ROOT), CODE)


def test_committed_file_is_what_the_config_generates():
    assert (ROOT / "databricks.yml").read_text() == bundle.render(str(ROOT), CODE)


def test_test_and_prod_only_with_one_job_per_kind_the_company_has():
    assert list(DOC["targets"]) == ["test", "prod"]  # dev jobs are per person, deployed by each person
    kinds = [k for k in jobs.KINDS if jobs.tasks_for(jobs.load_jobs(str(ROOT), k), CODE)]
    for env, t in DOC["targets"].items():
        assert set(t["resources"]["jobs"]) == {f"{CODE}_{k}" for k in kinds}
        names = {j["name"] for j in t["resources"]["jobs"].values()}
        assert names == {jobs.job_name(jobs.load_jobs(str(ROOT), k), CODE, env) for k in kinds}


def test_same_tasks_schedule_and_permissions_as_the_deployer_builds():
    for env, t in DOC["targets"].items():
        for kind in jobs.KINDS:
            cfg = jobs.load_jobs(str(ROOT), kind)
            if not jobs.tasks_for(cfg, CODE):
                continue
            job = t["resources"]["jobs"][f"{CODE}_{kind}"]
            deployed = jobs.build(cfg, CODE, env, home="/Users/x", run_as_id="id")
            assert [x["task_key"] for x in job["tasks"]] == [x["task_key"] for x in deployed["tasks"]]
            assert job["schedule"] == deployed["schedule"]
            assert {p["group_name"]: p["level"] for p in job["permissions"]} == cfg["permissions"][env]


def test_job_account_by_name_and_no_ids_or_addresses_in_git():
    text = bundle.render(str(ROOT), CODE)
    for env, t in DOC["targets"].items():
        assert t["variables"]["job_account"]["lookup"]["service_principal"] == f"sp_{CODE}_{env}"
        assert t["run_as"] == {"service_principal_name": "${var.job_account}"}
    assert "@" not in text
    assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", text)
    assert "email_notifications" not in text  # addresses come from the secret scope at deploy time


def test_every_notebook_is_a_file_in_this_repo():
    for t in DOC["targets"].values():
        for job in t["resources"]["jobs"].values():
            for task in job["tasks"]:
                path = task["notebook_task"]["notebook_path"]
                assert path.startswith("./") and (ROOT / path).is_file(), path


def test_schema_patterns_with_unicode_classes_are_checked():
    pytest.importorskip("jsonschema")
    pytest.importorskip("regex")
    schema = {"type": "object", "properties": {"v": {"type": "string", "pattern": r"^\$\{[\p{L}_][\p{L}\p{N}_.]*\}$"}}}
    ok, bad = bundle.validator(schema), [e.message for e in bundle.validator(schema).iter_errors({"v": "${9x}"})]
    assert not list(ok.iter_errors({"v": "${var.job_account}"})) and bad and "does not match" in bad[0]
