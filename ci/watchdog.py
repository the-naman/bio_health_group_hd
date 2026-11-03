"""Watchdog for the monitoring workflows (28a, same file in both repos).

alerts.yml and notices.yml are the platform's eyes: while they fail (for example the Databricks Free Edition workspace
paused after a few days without a sign-in, 29 Oct - 2 Nov 2026), no alert and no notice is sent, and silence looks like
"all fine". This script runs as the last step of every scheduled run of those two workflows and mails the fixers:

    stopped  - this run failed and the previous scheduled run was fine (or there is none): mailed at once
    still    - still failing: at most one reminder per IST day (the first failed run of the day)
    back     - this run worked and the previous one had failed: "monitoring is back"

Recipients: config/ci.yml -> watchdog (roles; addresses from the MAIL_<ROLE> secrets, never printed). Business roles
never get these mails. If GitHub cannot be asked about earlier runs, a failed run is mailed anyway (better one mail too
many than silence) and a good run sends nothing. Manual runs (workflow_dispatch) send nothing: someone is watching.
Python standard library only (plus pyyaml when installed), so it still works when an earlier setup step failed.

    python3 ci/watchdog.py --workflow alerts --env prod --result failure
Env: GITHUB_EVENT_NAME, GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_RUN_ID, GITHUB_SERVER_URL, SMTP_USER,
     SMTP_APP_PASSWORD, MAIL_<ROLE>.
"""

import argparse
import json
import os
import re
import smtplib
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone, UTC
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mailer import build  # noqa: E402 - ci/ is not a package (standard library only, like this script)

IST = timezone(timedelta(hours=5, minutes=30))
FAILED = {"failure", "timed_out", "startup_failure"}
DEFAULT_ROLES = ["developer", "teamlead", "manager", "management"]
TRIES, WAIT = 3, 5


def when(iso):
    """GitHub timestamp ('2026-11-02T11:41:00Z') -> aware datetime."""
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def decide(result, current_id, runs, now):
    """What to mail. runs = completed scheduled runs of this workflow, newest first (id, conclusion, created_at).
    Returns 'stopped', 'still', 'back' or None. Cancelled or skipped runs are ignored."""
    earlier = [r for r in runs if r["id"] != current_id and (r["conclusion"] in FAILED or r["conclusion"] == "success")]
    prev = earlier[0] if earlier else None
    if result == "success":
        return "back" if prev and prev["conclusion"] in FAILED else None
    if prev is None or prev["conclusion"] not in FAILED:
        return "stopped"
    today = now.astimezone(IST).date()
    failed_today = any(
        r["conclusion"] in FAILED and when(r["created_at"]).astimezone(IST).date() == today for r in earlier
    )
    return None if failed_today else "still"


def scheduled_runs(repo, workflow):
    """Completed scheduled runs of <workflow>.yml, newest first; None when GitHub cannot be asked."""
    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}.yml/runs?event=schedule&status=completed&per_page=30"
    req = urllib.request.Request(  # noqa: S310 - fixed https://api.github.com URL
        url,
        headers={"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}", "Accept": "application/vnd.github+json"},
    )
    why = ""
    for attempt in range(1, TRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:  # noqa: S310 - fixed https://api.github.com URL
                return [
                    {"id": x["id"], "conclusion": x.get("conclusion") or "", "created_at": x["created_at"]}
                    for x in json.load(r).get("workflow_runs", [])
                ]
        except urllib.error.HTTPError as e:
            why = f"HTTP {e.code}"
            if e.code < 500 and e.code != 429:  # 4xx: asking again gives the same answer
                break
        except Exception as e:  # noqa: BLE001 - network or answer problem; decide without the history
            why = type(e).__name__
        if attempt < TRIES:
            time.sleep(WAIT * attempt)
    print(f"::warning::could not read earlier {workflow} runs ({why})")
    return None


def read_yaml(path):
    """pyyaml when available; None if it is not installed (the pip step may be the one that failed)."""
    try:
        import yaml
    except ImportError:
        return None
    return yaml.safe_load(Path(path).read_text())


def company_code():
    data = read_yaml("config/company.yml")
    if data:
        return str(data["code"])
    m = re.search(r"^code:\s*(\w+)", Path("config/company.yml").read_text(), re.M)
    return m.group(1) if m else "lh"


def company_name(code):
    data = read_yaml("config/company.yml")
    if data and data.get("name"):
        return str(data["name"])
    m = re.search(r"^name:\s*(.+)$", Path("config/company.yml").read_text(), re.M)
    return m.group(1).strip().strip("'\"") if m else code.upper()


def roles():
    data = read_yaml("config/ci.yml")
    return list((data or {}).get("watchdog") or DEFAULT_ROLES)


def message(kind, workflow, env, now, link):
    what = {"alerts": "data alerts", "notices": "business notices"}.get(workflow, workflow)
    at = now.astimezone(IST).strftime("%d %b %Y %H:%M IST")
    if kind == "back":
        return (
            f"Monitoring back: {workflow} ran again",
            f"The scheduled {workflow} check for {env} ran normally again at {at}.\n"
            f"The {what} work again; anything that was waiting is sent by the normal checks.\n\nRun: {link}",
        )
    head = "Monitoring stopped" if kind == "stopped" else "Monitoring still down"
    first = (
        f"The scheduled {workflow} check for {env} could not finish at {at}."
        if kind == "stopped"
        else f"The scheduled {workflow} check for {env} is still failing ({at}). This is the one reminder for today."
    )
    return (
        f"{head}: {workflow} could not run",
        f"{first}\nWhile it fails, no {what} are sent, so silence does not mean everything is fine.\n\n"
        "Most likely cause: the Databricks Free Edition workspace paused after a few days without a sign-in, or the "
        "SQL warehouse is stopped.\n\n"
        "What to do:\n"
        "1. Sign in to Databricks; under SQL Warehouses start the warehouse if it is stopped.\n"
        "2. Open the run below, read the failed step, then Re-run all jobs.\n"
        "3. A 'Monitoring back' mail follows by itself when a scheduled run works again.\n\n"
        f"Run: {link}",
    )


def send(subject, body, env, kind="stopped"):
    to = []
    for r in roles():
        addr = os.environ.get(f"MAIL_{r.upper()}", "")
        if addr.count("@") != 1:
            sys.exit(f"MAIL secret for {r} is missing")
        to.append(addr)
    code = company_code()
    tone, title = {"back": ("ok", "Monitoring back"), "still": ("fail", "Monitoring still down")}.get(
        kind, ("fail", "Monitoring stopped")
    )
    msg = build(
        subject,
        body,
        tone=tone,
        kicker=title,
        company=company_name(code),
        env=env,
        footer="sent by GitHub Actions; reply is not read.",
    )
    msg["Subject"] = f"[LH {code.upper()} {env}] {subject}"
    msg["From"] = os.environ["SMTP_USER"]
    msg["To"] = ", ".join(sorted(set(to)))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as s:
        s.login(os.environ["SMTP_USER"], os.environ["SMTP_APP_PASSWORD"])
        s.send_message(msg)
    print(f"watchdog mail sent to {', '.join(roles())}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--workflow", required=True, choices=["alerts", "notices"])
    p.add_argument("--env", required=True, choices=["dev", "test", "prod"])
    p.add_argument("--result", required=True)  # job.status: success, failure or cancelled
    a = p.parse_args()
    if os.environ.get("GITHUB_EVENT_NAME") != "schedule":
        print("watchdog: manual run, nothing to send")
        return
    if a.result == "cancelled":
        print("watchdog: run cancelled, nothing to send")
        return
    now = datetime.now(UTC)
    runs = scheduled_runs(os.environ["GITHUB_REPOSITORY"], a.workflow)
    if runs is None:
        kind = None if a.result == "success" else "stopped"
    else:
        kind = decide(a.result, int(os.environ.get("GITHUB_RUN_ID", "0")), runs, now)
    if not kind:
        print(f"watchdog: {a.workflow} {a.result}, nothing new to send")
        return
    link = f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ.get('GITHUB_RUN_ID', '')}"
    subject, body = message(kind, a.workflow, a.env, now, link)
    send(subject, body, a.env, kind)


if __name__ == "__main__":
    main()
