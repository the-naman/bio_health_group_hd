"""Databricks Asset Bundle (docs/adr/0011): databricks.yml generated from config/jobs.yml and config/stream.yml.

One source of truth: the job settings come from jobs.build, the same function our deployer uses, so the bundle and
the deployed jobs cannot disagree. Test and prod only (dev jobs are per person, deployed by each person's own run of
jobs/01_deploy_jobs). Differences from the deployed jobs, all on purpose:
  - notebooks are paths in this repo (the bundle syncs them); the deployer points at the environment's Git folder
  - the job account is looked up by its name (variable job_account), so no application id is written in Git
  - failure-mail addresses stay out of Git: the deployer reads them from the secret scope at deploy time
CI regenerates the file and fails if it differs, and validates it against the CLI's bundle JSON schema (ci/bundle.py).
"""

import os

import yaml

from lh import jobs

ENVS = ("test", "prod")
HEADER = (
    "# GENERATED from config/jobs.yml and config/stream.yml by: python ci/bundle.py --write   (do not edit by hand)\n"
    "# Databricks Asset Bundle of this repo's test and prod jobs (docs/adr/0011). Jobs are deployed by our own deployer\n"
    "# (ci/deploy.py -> jobs/01_deploy_jobs); this file is the standard, portable description of the same jobs.\n"
)


def _notebook(path, folder):
    """Deployer path <home>/<repo>_<env>/<notebook> -> path of the notebook file in this repo."""
    rel = path[len(folder) + 1 :] if path.startswith(folder + "/") else path
    return f"./{rel}.ipynb"


def job_resource(cfg, code, env, repo_root=None):
    """Bundle job resource (Jobs API settings minus run_as, plus permissions) for one company, environment, kind."""
    folder = cfg["folder"][env].format(repo=cfg["repo"][code])
    s = jobs.build(cfg, code, env, home="", run_as_id="${var.job_account}")
    del s["run_as"]  # set once per target (run_as below)
    for t in s["tasks"]:
        nb = t["notebook_task"]
        nb["notebook_path"] = _notebook(nb["notebook_path"], "/" + folder)
        nb.pop("source", None)  # bundle notebooks are synced workspace files
        if repo_root and not os.path.exists(os.path.join(repo_root, nb["notebook_path"])):
            raise ValueError(f"{t['task_key']}: notebook {nb['notebook_path']} is not in the repo")
    s["permissions"] = [{"level": lvl, "group_name": g} for g, lvl in cfg["permissions"][env].items()]
    return s


def build(root, code):
    """The whole databricks.yml as a dict."""
    targets = {}
    for env in ENVS:
        resources = {}
        for kind in jobs.KINDS:
            cfg = jobs.load_jobs(root, kind)
            if jobs.tasks_for(cfg, code):
                resources[f"{code}_{kind}"] = job_resource(cfg, code, env, root)
        account = jobs.load_jobs(root)["run_as"][env].format(code=code)
        targets[env] = {
            "variables": {"job_account": {"lookup": {"service_principal": account}}},
            "run_as": {"service_principal_name": "${var.job_account}"},
            "resources": {"jobs": resources},
        }
    return {
        "bundle": {"name": f"lh_{code}"},
        "variables": {
            "job_account": {"description": "application id of the job account sp_<code>_<env>, looked up by name"}
        },
        "targets": targets,
    }


def render(root, code):
    return HEADER + yaml.safe_dump(build(root, code), sort_keys=False, width=120, allow_unicode=True)


def validator(schema):
    """jsonschema Draft 7 validator for the CLI's bundle schema. Its patterns use \\p{L} / \\p{N} (Unicode letter / digit),
    which Python's re does not know, so "pattern" is checked with the regex package (same syntax, plus \\p)."""
    import jsonschema
    import regex

    def pattern(validator, patrn, instance, schema):
        if validator.is_type(instance, "string") and not regex.search(patrn, instance):
            yield jsonschema.ValidationError(f"{instance!r} does not match {patrn!r}")

    return jsonschema.validators.extend(jsonschema.Draft7Validator, {"pattern": pattern})(schema)
