"""Controlled refresh (5.11), run by .github/workflows/refresh.yml after approval in the <code>_refresh environment,
as the deploy account sp_lh_deploy (OAuth M2M). Same file in both repos. Refresh FQF first, then BHG (BHG's group
view reads FQF's Gold of the same environment).
  1. put REFRESH_HASH_KEY into secret scope lh_refresh (only the source job accounts may read it)
  2. run refresh/01_refresh_out from the source environment's Git folder, as the source job account
  3. target test: run refresh/02_refresh_in as sp_<code>_test, then the normal test job (Gold, smoke)
     target dev: each person loads it in their own dev Git folder (02_refresh_in, then their dev job)
Never prints a person, an address, an id or the key. Usage: python ci/refresh.py --target test"""

import argparse
import os
import sys
import time
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as J

p = argparse.ArgumentParser()
p.add_argument("--target", required=True, choices=["test", "dev"])
a = p.parse_args()
key = os.environ.get("REFRESH_HASH_KEY", "")
if len(key) < 32:
    sys.exit("REFRESH_HASH_KEY is missing or too short")
code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
jcfg = yaml.safe_load(Path("config/jobs.yml").read_text())
flow = yaml.safe_load(Path("config/refresh.yml").read_text())["flows"][a.target]
source, repo = flow["from"], jcfg["repo"][code]
w = WorkspaceClient()
me = w.current_user.me().user_name
print(f"::add-mask::{me}")


def app_id(name):
    found = w.api_client.do(
        "GET", "/api/2.0/preview/scim/v2/ServicePrincipals", query={"filter": f'displayName eq "{name}"'}
    ).get("Resources", [])
    if len(found) != 1:
        sys.exit(f"job account {name}: found {len(found)}")
    return found[0]["applicationId"]


def run(task, env, notebook, params):
    sp = f"sp_{code}_{env}"
    print(f"\n== {task}: {notebook} {params} as {sp}", flush=True)
    started = time.time()
    run_id = w.jobs.submit(
        run_name=f"ci_{code}_refresh_{task}",
        run_as=J.JobRunAs(service_principal_name=app_id(sp)),
        tasks=[
            J.SubmitTask(
                task_key=task,
                notebook_task=J.NotebookTask(
                    notebook_path=f"/Users/{me}/{repo}_{env}/{notebook}", base_parameters=params
                ),
            )
        ],
    ).run_id
    while True:
        r = w.jobs.get_run(run_id)
        if (
            r.state
            and r.state.life_cycle_state
            and r.state.life_cycle_state.value in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")
        ):
            break
        if time.time() - started > 3600:
            w.jobs.cancel_run(run_id)
            sys.exit(f"{task}: no result after 60 minutes, cancelled")
        time.sleep(20)
    t = r.tasks[0]
    state = t.state.result_state.value if t.state and t.state.result_state else "UNKNOWN"
    out = w.jobs.get_run_output(t.run_id)
    print(
        f"{task}: {state} in {time.time() - started:.0f}s",
        (out.notebook_output.result if out.notebook_output else "") or "",
    )
    if state != "SUCCESS":
        sys.exit(f"{task} failed: {(out.error or state)[:500]}")


print(f"refresh {code}: {source} -> {a.target} ({flow['pct']}% of people)")
w.secrets.put_secret(scope="lh_refresh", key="refresh_key", string_value=key)
print("key put into scope lh_refresh")
run("out", source, "refresh/01_refresh_out", {"target": a.target})
if a.target == "test":
    run("in", "test", "refresh/02_refresh_in", {"env": "test"})
    name = jcfg["name"].format(code=code, env="test")
    jobs = list(w.jobs.list(name=name))
    if len(jobs) != 1:
        sys.exit(f"{name}: found {len(jobs)} jobs")
    print(f"\n== {name}: normal test run (id maps, reconciliation, Gold, smoke)", flush=True)
    res = w.jobs.run_now(job_id=jobs[0].job_id).result(timeout=__import__("datetime").timedelta(hours=3))
    state = res.state.result_state.value if res.state and res.state.result_state else "UNKNOWN"
    print(f"{name}: {state}")
    if state != "SUCCESS":
        sys.exit(f"{name} did not succeed: {state}")
else:
    print("\ndev: each person now runs refresh/02_refresh_in (env dev) in their own dev Git folder, then their dev job")
print("\nDONE")
