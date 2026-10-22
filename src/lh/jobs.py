"""Daily pipeline jobs as code (same file in both repos). Settings in config/jobs.yml.

build() turns the config into Jobs API settings for one company and environment:
  - tasks of other companies are dropped and their dependants rewired (FQF has no group task, so bi_views
    follows gold there)
  - every task is a notebook in that environment's own Git folder, with the env as a parameter
  - dev: one job per person (lh_<code>_dev_daily_<role>), no schedule, runs as that person from their own Git folder;
    test/prod: schedule per environment (schedule_of; staggered so test never runs with prod) and run as the
    environment's job account; paused: true in config/jobs.yml pauses every schedule
  - silver_rebuild runs only if silver fails; masks (then gold) run if either of them succeeded
  - failure emails (tier 1, 5.10): dev = only the person whose job it is; test/prod = the roles in notify.<env>;
  - dev success emails (5.13): the person whose job it is hears about every run, success too; optional dev_inform
    adds other roles (empty since go-live: requirement 10 is the daily dev-activity digest, ci/alerts.py);
    the addresses come from the secret scope lh_<env> at deploy time
deploy() creates or updates the job by name and sets its permissions exactly. plan mode changes nothing.
Folder paths, job-account IDs and mail addresses are looked up at deploy time, so no person or ID is stored in the repo."""
import os
import yaml

RUN_IF = {"ALL_SUCCESS", "AT_LEAST_ONE_SUCCESS", "NONE_FAILED", "ALL_DONE", "AT_LEAST_ONE_FAILED", "ALL_FAILED"}
ROLES = {"developer", "teamlead", "manager", "bizmanager", "management"}


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


def job_name(cfg, code, env, role=None):
    """lh_<code>_<env>_daily; in dev one job per person: lh_<code>_dev_daily_<role>."""
    base = cfg["name"].format(code=code, env=env)
    if env == "dev":
        if role not in cfg["dev_roles"].values():
            raise ValueError(f"dev jobs belong to a role ({sorted(cfg['dev_roles'].values())}), got {role}")
        return f"{base}_{role}"
    return base


def my_role(cfg, env, is_member):
    """dev: the caller's role from their group (is_member(group) -> bool); test/prod: None."""
    if env != "dev":
        return None
    roles = [r for g, r in cfg["dev_roles"].items() if is_member(g)]
    if len(roles) != 1:
        raise ValueError(f"dev jobs are deployed by one role login (Developer, Team Lead or Manager); found {roles or 'none'}")
    return roles[0]


def schedule_of(cfg, code, env):
    """Quartz cron of one company in one environment: schedule.<env>.<code> (or the older flat schedule.<code>)."""
    s = cfg["schedule"]
    return s[env][code] if env in s else s[code]


def recipients(cfg, env, role=None):
    """Roles that get the tier-1 failure mail: dev = the job's own person, else notify.<env>."""
    return [role] if env == "dev" else list(cfg["notify"][env])


def informed(cfg, env, role=None):
    """Other roles told about this person's dev runs (success and failure), from dev_inform. Empty outside dev."""
    return list(cfg.get("dev_inform", {}).get(role, [])) if env == "dev" else []


def notify_plan(cfg, code, env, role=None):
    """Roles to mail: {"job": [...] on failure, "tasks": {task_key: [...]} on failure, "success": [...] on success}.
    No addresses here."""
    who = recipients(cfg, env, role)
    told = informed(cfg, env, role)
    mine = who if env == "dev" else []                  # 5.13: in dev the owner also hears about a success
    return {"job": who + [r for r in told if r not in who],
            "tasks": {t["key"]: who for t in tasks_for(cfg, code) if t.get("notify")},
            "success": mine + [r for r in told if r not in mine]}


def mail_roles(cfg, env, role=None):
    """Roles whose address this deploy needs (= keys mail_<role> in scope lh_<env>)."""
    return sorted(set(recipients(cfg, env, role)) | set(informed(cfg, env, role)))


def scope_roles(cfg, env):
    """Every mail_<role> key scope lh_<env> must hold (dev: every role that may own a dev job)."""
    return sorted(set(cfg["dev_roles"].values()) if env == "dev" else set(cfg["notify"][env]))


def check(tasks, root=None, cfg=None):
    """Raise if a dependency is unknown, the graph has a loop, a run_if is invalid, a notebook is missing
    or a notify setting is not valid."""
    if cfg:
        if cfg["notify"]["dev"] != ["self"]:
            raise ValueError("notify.dev must be [self]: in dev only the person whose job it is gets mail")
        for env in ("test", "prod"):
            bad = set(cfg["notify"][env]) - ROLES
            if bad:
                raise ValueError(f"notify.{env}: unknown role {sorted(bad)}")
        bad = set(cfg["dev_roles"].values()) - ROLES
        if bad:
            raise ValueError(f"dev_roles: unknown role {sorted(bad)}")
        dev = set(cfg["dev_roles"].values())
        for owner, told in cfg.get("dev_inform", {}).items():
            if owner not in dev or not set(told) <= dev or owner in told:
                raise ValueError(f"dev_inform.{owner}: must name other dev roles ({sorted(dev)}), not itself")
    for t in tasks:
        if "notify" in t and t["notify"] is not True:
            raise ValueError(f"{t['key']}: notify must be true or left out")
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


def build(cfg, code, env, home, run_as_id=None, mail=None, role=None):
    """Jobs API settings for one company + environment. home = the deployer's home folder (/Users/<name>);
    run_as_id = the job account's application id (test/prod); mail = {role: address} (None = no emails);
    role = the person's role (dev)."""
    repo = cfg["repo"][code]
    folder = f"{home}/{cfg['folder'][env].format(repo=repo)}"
    tasks = tasks_for(cfg, code)
    check(tasks, cfg=cfg)
    name = job_name(cfg, code, env, role)
    plan = notify_plan(cfg, code, env, role)
    if mail:
        missing = sorted(set(mail_roles(cfg, env, role)) - set(mail))
        if missing:
            raise ValueError(f"no mail address for {missing} (scope lh_{env}, keys mail_<role>)")
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
        if mail and t["key"] in plan["tasks"]:
            task["email_notifications"] = {"on_failure": [mail[r] for r in plan["tasks"][t["key"]]]}
        out_tasks.append(task)
    tags = {"project": "lh", "company": code, "env": env}
    if env == "dev":
        tags["owner_role"] = role
    settings = {"name": name, "tags": tags,
                "max_concurrent_runs": 1,
                "timeout_seconds": cfg["job_timeout_minutes"] * 60,
                "queue": {"enabled": True},
                "tasks": out_tasks}
    if mail:
        settings["email_notifications"] = {"on_failure": [mail[r] for r in plan["job"]]}
        if plan["success"]:                                   # dev: the owner (and dev_inform roles) hear about every run
            settings["email_notifications"]["on_success"] = [mail[r] for r in plan["success"]]
        settings["notification_settings"] = {"no_alert_for_skipped_runs": True, "no_alert_for_canceled_runs": True}
    if env != "dev":
        if not run_as_id:
            raise ValueError(f"{env} needs the job account's application id")
        settings["run_as"] = {"service_principal_name": run_as_id}
        settings["schedule"] = {"quartz_cron_expression": schedule_of(cfg, code, env), "timezone_id": cfg["timezone"],
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

    def list_jobs(self, prefix):
        """[(job_id, name)] of every job this login can see whose name starts with prefix."""
        out, token = [], None
        while True:
            q = {"limit": 100, **({"page_token": token} if token else {})}
            res = self.call("GET", "/api/2.2/jobs/list", query=q)
            out += [(j["job_id"], j["settings"]["name"]) for j in res.get("jobs", [])
                    if j.get("settings", {}).get("name", "").startswith(prefix)]
            token = res.get("next_page_token")
            if not token:
                return out

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


def deploy(api, cfg, root, code, env, apply=False, mail=None, role=None):
    """Create or update this company's job for env (dev: the caller's own job, role from my_role()).
    mail = {role: address} read from scope lh_<env> by the notebook. Returns a summary row (no names, no addresses)."""
    check(tasks_for(cfg, code), root, cfg)
    me = api.me()
    if env == "dev":
        run_as_id, owner = None, ("user_name", me)
    else:
        run_as_id = api.service_principal(cfg["run_as"][env].format(code=code))
        owner = ("service_principal_name", run_as_id)
    settings = build(cfg, code, env, f"/Users/{me}", run_as_id, mail, role)
    job_id = api.find_job(settings["name"])
    action = "update" if job_id else "create"
    if not job_id and env == "dev":
        old = api.find_job(cfg["name"].format(code=code, env=env))     # 5.9 shared dev job becomes this person's job
        if old:
            job_id, action = old, "rename 5.9 dev job"
    if apply:
        if job_id:
            api.reset(job_id, settings)
        else:
            job_id = api.create(settings)
        api.set_acl(job_id, acl(cfg, env, owner))
    plan = notify_plan(cfg, code, env, role)
    return {"job": settings["name"], "action": action, "job_id": job_id,
            "tasks": " -> ".join(t["task_key"] for t in settings["tasks"]),
            "run_as": f"you ({role})" if env == "dev" else cfg["run_as"][env].format(code=code),
            "schedule": (settings["schedule"]["quartz_cron_expression"] + " " + settings["schedule"]["pause_status"])
                        if "schedule" in settings else "on demand",
            "notify": "; ".join([f"job: {', '.join(plan['job'])}"] + [f"{k}: {', '.join(v)}" for k, v in plan["tasks"].items()]
                                + ([f"success: {', '.join(plan['success'])}"] if plan["success"] else []))
                      if mail else "none"}
    
