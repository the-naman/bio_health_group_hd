"""After a merge into <code>_test or <code>_prod (same file in both repos). Run by GitHub Actions with the admin
token (DATABRICKS_HOST / DATABRICKS_TOKEN secrets):
  1. pull the environment's Git folder (<repo>_<env> in the token owner's home) to the merged commit
  2. redeploy the daily job from it (jobs/01_deploy_jobs, mode apply) - job account, paused schedule, failure mails
  3. run checks/t28_jobs from it (deployed jobs = config)
Never prints the home folder, a person or an address. Usage: python ci/deploy.py --env test --sha <commit>"""
import argparse, sys, time
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as J

p = argparse.ArgumentParser()
p.add_argument("--env", required=True, choices=["test", "prod"])
p.add_argument("--sha", required=True)
a = p.parse_args()

code = yaml.safe_load(open("config/company.yml"))["code"]
repo = yaml.safe_load(open("config/jobs.yml"))["repo"][code]
w = WorkspaceClient()
me = w.current_user.me().user_name
print(f"::add-mask::{me}")                                  # GitHub hides it from the log from here on
folder = f"/Users/{me}/{repo}_{a.env}"
branch = f"{code}_{a.env}"


def step(name):
    print(f"\n== {name}", flush=True)


# 1. Git folder -> merged commit
step(f"pull Git folder {repo}_{a.env} to {branch}")
repo_id = w.workspace.get_status(folder).object_id
w.repos.update(repo_id, branch=branch)
head = w.repos.get(repo_id).head_commit_id or ""
print(f"Git folder at {head[:7]}, merged commit {a.sha[:7]}")
if not head.startswith(a.sha[:7]):
    sys.exit("Git folder is not at the merged commit")


def run_notebook(task, notebook, params):
    """One-time run of a notebook from the env Git folder, as the token owner (admin). Exits if it fails."""
    step(f"{task}: {notebook} {params}")
    started = time.time()
    run_id = w.jobs.submit(
        run_name=f"ci_{code}_{a.env}_{task}",
        tasks=[J.SubmitTask(task_key=task,
                            notebook_task=J.NotebookTask(notebook_path=f"{folder}/{notebook}", base_parameters=params))],
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


