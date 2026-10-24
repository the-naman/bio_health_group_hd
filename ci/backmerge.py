"""10j back-merge (same file in both repos). Run by backmerge.yml after every push to <code>_prod.
If prod holds a change that dev does not have (a hotfix), open a pull request <code>_prod -> <code>_dev, so the
fix is not lost at the next promotion, and mail the roles in config/ci.yml (hotfix.backmerge).
"Dev has it" = merging prod into dev would not change dev's files (works for merge and squash commits alike).
Needs the full history (fetch-depth: 0). Env: GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_SERVER_URL, GITHUB_OUTPUT.
"""

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

import yaml

code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
prod, dev = f"{code}_prod", f"{code}_dev"
repo, server = os.environ["GITHUB_REPOSITORY"], os.environ.get("GITHUB_SERVER_URL", "https://github.com")


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True)


def out(**kv):
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        f.writelines(f"{k}={v}\n" for k, v in kv.items())


merged = git("merge-tree", "--write-tree", f"origin/{dev}", f"origin/{prod}")
dev_tree = git("rev-parse", f"origin/{dev}^{{tree}}").stdout.strip()
if merged.returncode == 0 and merged.stdout.split("\n")[0].strip() == dev_tree:
    print(f"{dev} already has everything in {prod}: no back-merge needed")
    out(needed="no")
    sys.exit(0)
conflict = merged.returncode != 0
missing = git("log", "--no-merges", "--format=- %h %s", f"origin/{dev}..origin/{prod}").stdout.strip()
print(f"{dev} is missing changes from {prod}{' (with conflicts)' if conflict else ''}:\n{missing}")


def api(method, path, body=None):
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}{path}",
        method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}", "Accept": "application/vnd.github+json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310 - fixed https://api.github.com URL
        return json.load(r)


owner = repo.split("/")[0]
url = f"{server}/{repo}/compare/{dev}...{prod}?expand=1"
try:
    open_prs = api("GET", f"/pulls?state=open&head={owner}:{prod}&base={dev}")
    if open_prs:
        url = open_prs[0]["html_url"]
        print(f"back-merge pull request already open: {url}")
    else:
        pr = api(
            "POST",
            "/pulls",
            {
                "title": f"Back-merge {prod} into {dev} (hotfix)",
                "head": prod,
                "base": dev,
                "body": "Opened by backmerge.yml after a prod deploy: prod holds a change dev "
                "does not have (a hotfix). Merge with 'Create a merge commit'."
                + (" There are conflicts: resolve them on a branch from dev." if conflict else ""),
            },
        )
        url = pr["html_url"]
        print(f"back-merge pull request opened: {url}")
except urllib.error.HTTPError as e:
    print(f"::warning::could not open the pull request (HTTP {e.code}); open it by hand: {url}")
out(needed="yes", url=url, conflict="yes" if conflict else "no")
