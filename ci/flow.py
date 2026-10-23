"""Which pull request flow brought the pushed commit to <code>_test / <code>_prod (same file in both repos).
Run by deploy.yml before the mail. "hotfix" when the merged pull request came from <code>_hotfix_<name>
(config/ci.yml "hotfix.prefix"), else "promotion". Never fails the deploy: if GitHub cannot be asked, it says
"promotion" with a warning. Env: GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_SHA, GITHUB_REF_NAME, GITHUB_OUTPUT.
"""

import json
import os
import urllib.request
from pathlib import Path

import yaml

code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
prefix = f"{code}_" + yaml.safe_load(Path("config/ci.yml").read_text())["hotfix"]["prefix"]
repo, sha, base = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SHA"], os.environ["GITHUB_REF_NAME"]
flow, head, number = "promotion", "", ""
try:
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/commits/{sha}/pulls",
        headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}", "Accept": "application/vnd.github+json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310 - fixed https://api.github.com URL
        prs = [p for p in json.load(r) if p.get("merged_at") and p["base"]["ref"] == base]
    if prs:
        head, number = prs[0]["head"]["ref"], str(prs[0]["number"])
        if head.startswith(prefix) and len(head) > len(prefix):
            flow = "hotfix"
        print(f"{sha[:7]} came to {base} by pull request #{number} from {head}: {flow}")
    else:
        print(f"::warning::no merged pull request found for {sha[:7]}; mailing as a normal promotion")
except Exception as e:  # noqa: BLE001 - the deploy mail must still go out
    print(
        f"::warning::could not ask GitHub which pull request this was ({type(e).__name__}); mailing as a normal promotion"
    )
with open(os.environ["GITHUB_OUTPUT"], "a") as f:
    f.write(f"flow={flow}\nhead={head}\npr={number}\n")
