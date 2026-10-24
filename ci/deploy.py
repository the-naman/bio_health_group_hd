"""After a merge into <code>_test or <code>_prod (same file in both repos). Run by GitHub Actions as the deploy
service principal sp_lh_deploy (OAuth M2M: DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET), never a
person's token. It may only: manage the test/prod jobs (group lh_deploy in config/jobs.yml), act as the job accounts
("Use"), read the mail keys of lh_test/lh_prod, and pull its own Git folders (read-only GitHub token).
  0. the first time: create the environment's Git folder (<repo>_<env>) in its own home; every time: the job account
     sp_<code>_<env> may read it (CAN_READ), so the job can open the notebooks
  1. pull that Git folder to the merged commit
  2. redeploy the daily job from it (jobs/01_deploy_jobs, mode apply) - job account, paused schedule, failure mails
  3. run checks/t28_jobs from it (deployed jobs = config)
Never prints the home folder, a person or an address. Usage: python ci/deploy.py --env test --sha <commit>"""

import argparse
import os
import sys
import time
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import jobs as J

p = argparse.ArgumentParser()
p.add_argument("--env", required=True, choices=["test", "prod"])
p.add_argument("--sha", required=True)
a = p.parse_args()

code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
jcfg = yaml.safe_load(Path("config/jobs.yml").read_text())
repo = jcfg["repo"][code]
w = WorkspaceClient()
me = w.current_user.me().user_name
print(f"::add-mask::{me}")  # GitHub hides it from the log from here on
folder = f"/Users/{me}/{repo}_{a.env}"
branch = f"{code}_{a.env}"


def step(name):
    print(f"\n== {name}", flush=True)


# 0. Git folder in the deploy account's own home (created once); the job account may read it
step(f"Git folder {repo}_{a.env}")
try:
    repo_id = w.workspace.get_status(folder).object_id
except NotFound:
    url = f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ['GITHUB_REPOSITORY']}.git"
    w.workspace.mkdirs(f"/Users/{me}")
    repo_id = w.repos.create(url=url, provider="gitHub", path=folder).id
    print("created (first deploy as the deploy account)")
job_sp = jcfg["run_as"][a.env].format(code=code)
found = w.api_client.do(
    "GET", "/api/2.0/preview/scim/v2/ServicePrincipals", query={"filter": f'displayName eq "{job_sp}"'}
).get("Resources", [])
if len(found) != 1:
    sys.exit(f"job account {job_sp}: found {len(found)}")
w.api_client.do(
    "PATCH",
    f"/api/2.0/permissions/repos/{repo_id}",
    body={
        "access_control_list": [{"service_principal_name": found[0]["applicationId"], "permission_level": "CAN_READ"}]
    },
)
print(f"{job_sp} may read it")

# 1. Git folder -> merged commit
step(f"pull Git folder {repo}_{a.env} to {branch}")
w.repos.update(repo_id, branch=branch)
head = w.repos.get(repo_id).head_commit_id or ""
print(f"Git folder at {head[:7]}, merged commit {a.sha[:7]}")
if not head.startswith(a.sha[:7]):
    sys.exit("Git folder is not at the merged commit")


def run_notebook(task, notebook, params):
    """One-time run of a notebook from the env Git folder, as the deploy account. Exits if it fails."""
    step(f"{task}: {notebook} {params}")
    started = time.time()
    run_id = w.jobs.submit(
        run_name=f"ci_{code}_{a.env}_{task}",
        tasks=[
            J.SubmitTask(
                task_key=task,
                notebook_task=J.NotebookTask(notebook_path=f"{folder}/{notebook}", base_parameters=params),
            )
        ],
    ).run_id
    while True:
        run = w.jobs.get_run(run_id)
        life = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else ""
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        if time.time() - started > 1800:
            w.jobs.cancel_run(run_id)
            sys.exit(f"{task}: no result after 30 minutes, cancelled")
        time.sleep(15)
    t = run.tasks[0]
    state = t.state.result_state.value if t.state and t.state.result_state else life
    print(f"{task}: {state} in {time.time() - started:.0f}s")
    out = w.jobs.get_run_output(t.run_id)
    if out.notebook_output and out.notebook_output.result:
        print(out.notebook_output.result)
    if state != "SUCCESS":
        sys.exit(f"{task} failed: {(out.error or state)[:500]}")


# 2. redeploy the job, 3. check it
run_notebook("deploy_job", "jobs/01_deploy_jobs", {"env": a.env, "mode": "apply"})
run_notebook("check_jobs", "checks/t28_jobs", {"env": a.env})
print("\nDONE")
