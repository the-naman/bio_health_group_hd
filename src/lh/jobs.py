"""Daily pipeline jobs as code (same file in both repos). Settings in config/jobs.yml.

build() turns the config into Jobs API settings for one company and environment:
  - tasks of other companies are dropped and their dependants rewired (FQF has no group task, so bi_views
    follows gold there)
  - every task is a notebook in that environment's own Git folder, with the env as a parameter
  - dev: no schedule, runs as whoever deploys it; test/prod: schedule (paused until go-live) and run as the
    environment's job account
  - silver_rebuild runs only if silver fails; gold runs if either of them succeeded
deploy() creates or updates the job by name and sets its permissions exactly. plan mode changes nothing.
Folder paths and job-account IDs are looked up at deploy time, so no person or ID is stored in the repo."""
import os
import yaml

RUN_IF = {"ALL_SUCCESS", "AT_LEAST_ONE_SUCCESS", "NONE_FAILED", "ALL_DONE", "AT_LEAST_ONE_FAILED", "ALL_FAILED"}


def load_jobs(root):
    with open(os.path.join(root, "config", "jobs.yml")) as f:
        return yaml.safe_load(f)


def tasks_for(cfg, code):
    """This company's tasks; a dependency on a task it does not have is replaced by that task's own dependencies."""
    all_tasks = {t["key"]: t for t in cfg["tasks"]}
    keep = [t for t in cfg["tasks"] if code in t.get("companies", [code])]
    kept = {t["key"] for t in keep}

    def resolve(key):
        if key in kept:
            return [key]
        return [k for dep in all_tasks[key].get("after", []) for k in resolve(dep)]

    out = []
    for t in keep:
        deps = []
        for d in t.get("after", []):
            deps += [k for k in resolve(d) if k not in deps]
        out.append(dict(t, after=deps))
    return out


def check(tasks, root=None):
    """Raise if a dependency is unknown, the graph has a loop, a run_if is invalid or a notebook is missing."""
    keys = [t["key"] for t in tasks]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate task key")
    for t in tasks:
        for d in t["after"]:
            if d not in keys:
                raise ValueError(f"{t['key']}: unknown dependency {d}")
        if t.get("run_if", "ALL_SUCCESS") not in RUN_IF:
            raise ValueError(f"{t['key']}: bad run_if")
        if root and not any(os.path.exists(os.path.join(root, t["notebook"] + ext)) for ext in ("", ".py", ".ipynb", ".sql")):
            raise ValueError(f"{t['key']}: notebook {t['notebook']} not found in the repo")
    done, todo = set(), {t["key"]: set(t["after"]) for t in tasks}
    while todo:
        ready = [k for k, deps in todo.items() if deps <= done]
        if not ready:
            raise ValueError(f"dependency loop among {sorted(todo)}")
        for k in ready:
            done.add(k)
            del todo[k]


def build(cfg, code, env, home, run_as_id=None):
    """Jobs API settings for one company + environment. home = the deployer's home folder (/Users/<name>);
    run_as_id = the job account's application id (test/prod)."""
    repo = cfg["repo"][code]
    folder = f"{home}/{cfg['folder'][env].format(repo=repo)}"
    tasks = tasks_for(cfg, code)
    check(tasks)
    out_tasks = []
    for t in tasks:
        task = {"task_key": t["key"],
                "notebook_task": {"notebook_path": f"{folder}/{t['notebook']}", "source": "WORKSPACE",
                                  "base_parameters": {"env": env, **t.get("params", {})}},
                "timeout_seconds": t["timeout"] * 60,
                "max_retries": t["retries"]}
        if t["retries"]:
            task.update(min_retry_interval_millis=300000, retry_on_timeout=False)
        if t["after"]:
            task["depends_on"] = [{"task_key": d} for d in t["after"]]
        if t.get("run_if"):
            task["run_if"] = t["run_if"]
        out_tasks.append(task)
    settings = {"name": cfg["name"].format(code=code, env=env),
                "tags": {"project": "lh", "company": code, "env": env},
                "max_concurrent_runs": 1,
                "timeout_seconds": cfg["job_timeout_minutes"] * 60,
                "queue": {"enabled": True},
                "tasks": out_tasks}
    if env != "dev":
        if not run_as_id:
            raise ValueError(f"{env} needs the job account's application id")
        settings["run_as"] = {"service_principal_name": run_as_id}
        settings["schedule"] = {"quartz_cron_expression": cfg["schedule"][code], "timezone_id": cfg["timezone"],
                                "pause_status": "PAUSED" if cfg["paused"] else "UNPAUSED"}
    return settings


def acl(cfg, env, owner):
    """owner = ("user_name", <name>) or ("service_principal_name", <application id>)."""
    return ([{owner[0]: owner[1], "permission_level": "IS_OWNER"}]
            + [{"group_name": g, "permission_level": p} for g, p in cfg["permissions"][env].items()])


class Api:
    """Workspace REST API of whoever runs the deploy notebook."""
    def __init__(self, w):
        self.w = w

    def call(self, method, path, body=None, query=None):
        return self.w.api_client.do(method, path, body=body, query=query) or {}

    def me(self):
        return self.call("GET", "/api/2.0/preview/scim/v2/Me")["userName"]

    def service_principal(self, name):
        res = self.call("GET", "/api/2.0/preview/scim/v2/ServicePrincipals", query={"filter": f'displayName eq "{name}"'})
        found = res.get("Resources", [])
        if len(found) != 1:
            raise ValueError(f"job account {name}: found {len(found)}")
        return found[0]["applicationId"]

    def find_job(self, name):
        jobs = self.call("GET", "/api/2.2/jobs/list", query={"name": name}).get("jobs", [])
        if len(jobs) > 1:
            raise ValueError(f"{len(jobs)} jobs named {name}; remove the extra ones first")
        return jobs[0]["job_id"] if jobs else None

    def create(self, settings):
        return self.call("POST", "/api/2.2/jobs/create", body=settings)["job_id"]

    def reset(self, job_id, settings):
        self.call("POST", "/api/2.2/jobs/reset", body={"job_id": job_id, "new_settings": settings})

    def set_acl(self, job_id, entries):
        self.call("PUT", f"/api/2.0/permissions/jobs/{job_id}", body={"access_control_list": entries})

    def get_job(self, job_id):
        return self.call("GET", "/api/2.2/jobs/get", query={"job_id": job_id})

    def get_acl(self, job_id):
        return self.call("GET", f"/api/2.0/permissions/jobs/{job_id}")


def deploy(api, cfg, root, code, env, apply=False):
    """Create or update this company's job for env. Returns a summary row (no names of people)."""
    check(tasks_for(cfg, code), root)
    me = api.me()
    if env == "dev":
        run_as_id, owner = None, ("user_name", me)
    else:
        run_as_id = api.service_principal(cfg["run_as"][env].format(code=code))
        owner = ("service_principal_name", run_as_id)
    settings = build(cfg, code, env, f"/Users/{me}", run_as_id)
    job_id = api.find_job(settings["name"])
    action = "update" if job_id else "create"
    if apply:
        if job_id:
            api.reset(job_id, settings)
        else:
            job_id = api.create(settings)
        api.set_acl(job_id, acl(cfg, env, owner))
    return {"job": settings["name"], "action": action, "job_id": job_id,
            "tasks": " -> ".join(t["task_key"] for t in settings["tasks"]),
            "run_as": "deployer" if env == "dev" else cfg["run_as"][env].format(code=code),
            "schedule": (settings["schedule"]["quartz_cron_expression"] + " " + settings["schedule"]["pause_status"])
                        if "schedule" in settings else "on demand"}
    
