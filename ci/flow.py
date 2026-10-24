"""Which pull request flow brought the pushed commit to <code>_test / <code>_prod (same file in both repos).
Run by deploy.yml before the mail. "hotfix" when the merged pull request came from <code>_hotfix_<name>
(config/ci.yml "hotfix.prefix"), else "promotion". Never fails the deploy: if GitHub cannot be asked, it says
"promotion" with a warning. Asks up to 3 times (5 s, then 10 s apart) when GitHub is busy (HTTP 429 or 5xx), the
network fails, or the merged pull request is not listed yet (GitHub links it a moment after the merge); a 4xx answer
is final. Env: GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_SHA, GITHUB_REF_NAME, GITHUB_OUTPUT.
"""

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
prefix = f"{code}_" + yaml.safe_load(Path("config/ci.yml").read_text())["hotfix"]["prefix"]
repo, sha, base = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_SHA"], os.environ["GITHUB_REF_NAME"]
flow, head, number = "promotion", "", ""
TRIES, WAIT = 3, 5


def merged_prs():
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/commits/{sha}/pulls",
        headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}", "Accept": "application/vnd.github+json"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310 - fixed https://api.github.com URL
        return [p for p in json.load(r) if p.get("merged_at") and p["base"]["ref"] == base]


prs, why = [], ""
for attempt in range(1, TRIES + 1):
    try:
        prs = merged_prs()
        if prs:
            break
        why = "no merged pull request listed yet"
    except urllib.error.HTTPError as e:
        why = f"HTTP {e.code}"
        if e.code < 500 and e.code != 429:  # 4xx: asking again gives the same answer
            break
    except Exception as e:  # noqa: BLE001 - network or answer problem; the deploy mail must still go out
        why = type(e).__name__
    if attempt < TRIES:
        print(f"GitHub not ready ({why}), asking again in {WAIT * attempt} s")
        time.sleep(WAIT * attempt)
if prs:
    head, number = prs[0]["head"]["ref"], str(prs[0]["number"])
    if head.startswith(prefix) and len(head) > len(prefix):
        flow = "hotfix"
    print(f"{sha[:7]} came to {base} by pull request #{number} from {head}: {flow}")
else:
    print(f"::warning::could not tell which pull request brought {sha[:7]} ({why}); mailing as a normal promotion")
with open(os.environ["GITHUB_OUTPUT"], "a") as f:
    f.write(f"flow={flow}\nhead={head}\npr={number}\n")
