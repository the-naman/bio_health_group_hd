"""Shared-code drift guard (same file in both repos). The two company repos share one framework (docs/adr/0007).
Every shared file must be byte-identical in both repos, and a file that exists in only one repo must be declared as
company-only, so a change made in one repo and forgotten in the other is caught before it is promoted.

    python ci/shared.py                      # compare with the other repo's branch for the same environment
    python ci/shared.py --other <folder>     # compare with a local copy (tests, offline)

Rules: config/ci.yml -> shared. Prints file paths only. Exit 1 on drift."""

import argparse
import filecmp
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml


def listed(root, cfg):
    """Files under the shared roots plus the single shared files, relative to root (git-tracked only)."""
    out = subprocess.run(["git", "-C", str(root), "ls-files"], capture_output=True, text=True, check=True).stdout
    tracked = set(out.split())
    keep = {f for f in tracked if f.startswith(tuple(cfg["roots"]))} | (set(cfg["files"]) & tracked)
    return {f for f in keep if f not in cfg.get("per_company", [])}


def compare(here, other, here_code, other_code, cfg):
    """[(problem, path)] for every shared file that differs or is missing on one side without being declared."""
    mine, theirs = listed(here, cfg), listed(other, cfg)
    only = cfg.get("company_only", {})
    problems = [("differs", f) for f in sorted(mine & theirs) if not filecmp.cmp(here / f, other / f, shallow=False)]
    problems += [
        (f"only in {here_code}, not declared", f) for f in sorted(mine - theirs) if f not in only.get(here_code, [])
    ]
    problems += [
        (f"only in {other_code}, not declared", f) for f in sorted(theirs - mine) if f not in only.get(other_code, [])
    ]
    return problems


def branch_env():
    """dev / test / prod from the branch being promoted (GITHUB_HEAD_REF in a pull request), else None."""
    ref = (
        os.environ.get("GITHUB_HEAD_REF")
        or subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True).stdout.strip()
    )
    env = ref.rsplit("_", 1)[-1]
    return env if env in ("dev", "test", "prod") and ref.count("_") == 1 else None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--other", help="local folder of the other repo (default: clone its branch for this environment)")
    a = p.parse_args()
    here = Path.cwd()
    code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
    cfg = yaml.safe_load(Path("config/ci.yml").read_text())["shared"]
    other_code = next(c for c in cfg["repos"] if c != code)
    if a.other:
        problems = compare(here, Path(a.other), code, other_code, cfg)
    else:
        env = branch_env()
        if env is None:
            print("shared files: not an environment branch (hotfix or feature); checked again when it reaches dev")
            return 0
        with tempfile.TemporaryDirectory() as tmp:
            url = f"https://github.com/{cfg['repos'][other_code]}.git"
            branch = f"{other_code}_{env}"
            subprocess.run(["git", "clone", "-q", "--depth", "1", "-b", branch, url, tmp], check=True)
            problems = compare(here, Path(tmp), code, other_code, cfg)
        print(f"shared files compared with {other_code}_{env}")
    for what, f in problems:
        print(f"     {what}: {f}")
    print("shared files:", "identical" if not problems else f"{len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
