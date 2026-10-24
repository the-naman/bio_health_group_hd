"""Fast checks of the config-driven rules (no Spark): jobs, mail routing, privacy plan, BI views."""

import yaml

from lh import bi, jobs, privacy
from tests.conftest import CODE, ROOT, STREAM_COMPANIES

ENVS = ("dev", "test", "prod")


def build(kind, env):
    cfg = jobs.load_jobs(str(ROOT), kind)
    role = sorted(cfg["dev_roles"].values())[0] if env == "dev" else None
    sp = None if env == "dev" else "00000000-0000-0000-0000-000000000000"
    return cfg, jobs.build(cfg, CODE, env, "/Users/ci", sp, role=role)


def test_dev_jobs_have_no_schedule_and_run_as_the_person():
    for kind in jobs.KINDS:
        cfg = jobs.load_jobs(str(ROOT), kind)
        if not jobs.tasks_for(cfg, CODE):
            continue
        _, s = build(kind, "dev")
        assert "schedule" not in s and "run_as" not in s


def test_test_and_prod_jobs_run_as_job_account_on_their_own_schedule():
    for kind in jobs.KINDS:
        cfg = jobs.load_jobs(str(ROOT), kind)
        if not jobs.tasks_for(cfg, CODE):
            continue
        crons = {}
        for env in ("test", "prod"):
            _, s = build(kind, env)
            assert s["run_as"]["service_principal_name"]
            assert s["max_concurrent_runs"] == 1
            crons[env] = s["schedule"]["quartz_cron_expression"]
        assert crons["test"] != crons["prod"], "test and prod must not start at the same time"


def test_daily_jobs_never_overlap_and_prod_runs_first():
    cfg = jobs.load_jobs(str(ROOT), "daily")
    hour = {
        (env, code): int(jobs.schedule_of(cfg, code, env).split()[2])
        for env in ("test", "prod")
        for code in ("bhg", "fqf")
    }
    assert len(set(hour.values())) == 4, hour
    assert max(hour[("prod", c)] for c in ("bhg", "fqf")) < min(hour[("test", c)] for c in ("bhg", "fqf"))
    assert hour[("prod", "fqf")] < hour[("prod", "bhg")], "the group view reads FQF Gold, so FQF runs first"


def test_task_graph_has_no_unknown_or_circular_dependency():
    for kind in jobs.KINDS:
        cfg = jobs.load_jobs(str(ROOT), kind)
        tasks = jobs.tasks_for(cfg, CODE)
        jobs.check(tasks, cfg=cfg)
        seen = set()
        for t in tasks:
            assert set(t["after"]) <= seen, f"{t['key']} runs before {t['after']}"
            seen.add(t["key"])


def test_stream_job_only_for_stream_companies():
    has = bool(jobs.tasks_for(jobs.load_jobs(str(ROOT), "stream"), CODE))
    assert has == (CODE in STREAM_COMPANIES)


def test_failed_deploy_never_mails_the_business_manager():
    ci = yaml.safe_load((ROOT / "config" / "ci.yml").read_text())
    for sect in (ci["promotion"]["test"], ci["promotion"]["prod"], ci["hotfix"]["test"], ci["hotfix"]["prod"]):
        assert "bizmanager" not in sect["deploy_failed"]
        assert "management" in sect["deploy_failed"]


def test_privacy_masks_never_land_on_a_streaming_source():
    p = privacy.load()
    skip = tuple(p.get("gold_tables", []))
    silver = {
        t: dict.fromkeys(c, "string") for t, c in p["personal"].items() if not t.startswith(("dim_", "fact_", *skip))
    }
    for env in ENVS:
        cols = {"payment_events": {"value": "string", "customer_ref": "string", "loan_ref": "string"}}
        assert privacy.plan(p, env, "bronze", cols, {}) == []
        assert [r for r in privacy.plan(p, env, "silver", {**silver, **cols}, {}) if r[0] == "payment_events"] == []


def test_ids_are_tokens_only_in_test_and_prod():
    p = privacy.load()
    ids = p.get("ids", {})
    assert set(ids.get("environments", [])) == {"test", "prod"}
    assert "dev" not in ids.get("environments", [])


def test_bi_views_have_no_personal_columns_in_their_sql():
    personal = ("full_name", "phone", "aadhaar", "dob", "monthly_income", "id_fingerprint", "contact_fingerprint")
    for name, sql in bi.wanted({"code": CODE}).items():
        select = sql.split("FROM")[0].lower()
        assert not [c for c in personal if c in select], name


def test_views_that_may_wait_for_a_stream_have_an_empty_fallback():
    for name in bi.wanted({"code": CODE}):
        if "payment" in name:
            assert name in bi.EMPTY_UNTIL
