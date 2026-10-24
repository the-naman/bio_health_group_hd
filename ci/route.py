"""Route a pull request into <code>_test or <code>_prod (same file in both repos). Run by promote.yml (job "route").
Normal promotion: test only from <code>_dev, prod only from <code>_test.
Hotfix (10j, config/ci.yml "hotfix"): a branch <code>_hotfix_<name> may go to test, then to prod, if
  - it is based on the current <code>_prod (prod is fully inside it),
  - it carries nothing from <code>_dev (only prod + the fix),
  - for prod: it is already merged into <code>_test (the fix was tested and approved there first).
Needs the full history (actions/checkout fetch-depth: 0). Env: BASE, HEAD (branch names), HEAD_SHA, GITHUB_OUTPUT.
"""

import os
import subprocess
import sys
from pathlib import Path

import yaml


def fail(msg):
    print(f"::error::{msg}")
    sys.exit(1)


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True)


def is_ancestor(a, b):
    return git("merge-base", "--is-ancestor", a, b).returncode == 0


code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
ci = yaml.safe_load(Path("config/ci.yml").read_text())
base, head, sha = os.environ["BASE"], os.environ["HEAD"], os.environ["HEAD_SHA"]
env = base.removeprefix(f"{code}_")
rule = ci["promotion"].get(env)
if not rule or base != f"{code}_{env}":
    fail(f"{base} is not a promotion branch")

hot_prefix = f"{code}_{ci['hotfix']['prefix']}"
if head == f"{code}_{rule['from']}":
    flow, environment, approver = "promotion", f"{code}_{env}", rule["approver"]
elif head.startswith(hot_prefix) and len(head) > len(hot_prefix):
    flow = "hotfix"
    hot = ci["hotfix"][env]
    environment, approver = f"{code}_{hot['environment']}", hot["approver"]
    prod, dev, test = (f"origin/{code}_{e}" for e in ("prod", "dev", "test"))
    if not is_ancestor(prod, sha):
        fail(f"{head} is not based on the current {code}_prod: merge {code}_prod into it (or branch again from it)")
    own = git("rev-list", "--count", sha, f"^{prod}").stdout.strip()
    not_dev = git("rev-list", "--count", sha, f"^{prod}", f"^{dev}").stdout.strip()
    if own != not_dev:
        fail(f"{head} contains commits from {code}_dev; a hotfix must hold only {code}_prod + the fix")
    if env == "prod" and not is_ancestor(sha, test):
        fail(f"merge {head} into {code}_test first (URGENT test approval, 'Create a merge commit'), then come back")
    print(f"hotfix: {own} commit(s) on top of {code}_prod")
else:
    allowed = f"{code}_{rule['from']} or {hot_prefix}<name>"
    fail(f"{base} only accepts pull requests from {allowed} (this one is from {head})")

print(f"{flow} {head} -> {base}, approver: {approver}, environment: {environment}")
with open(os.environ["GITHUB_OUTPUT"], "a") as f:
    f.write(f"code={code}\nenv={env}\nflow={flow}\nenvironment={environment}\n")
