"""Promotion notices by Gmail (same file in both repos). Roles per event come from config/ci.yml; addresses from the
MAIL_<ROLE> environment variables (GitHub secrets). Addresses and the password are never printed.
    python ci/notify.py --event request --env test --subject "..." --body "..."
    python ci/notify.py --flow hotfix --event request --env test ...   # 10j: roles from ci.yml "hotfix", URGENT
    python ci/notify.py --flow hotfix --event backmerge --env prod ...  # 10j: back-merge pull request opened
    python ci/notify.py --flow hotfix --event deployed --env test ...   # 10j: HOTFIX deployed (deploy.yml, ci/flow.py)
For --event deployed, RESULT (set by deploy.yml to the job status) other than "success" sends to the roles under
deploy_failed instead (audit B1: a failed deploy goes to the fixers, not to business users).
"""

import argparse
import os
import smtplib
import ssl
import sys
from email.message import EmailMessage
from pathlib import Path

import yaml

p = argparse.ArgumentParser()
p.add_argument("--flow", default="promotion", choices=["promotion", "hotfix"])
p.add_argument("--event", required=True, choices=["request", "result", "deployed", "backmerge"])
p.add_argument("--env", required=True, choices=["test", "prod"])
p.add_argument("--subject", required=True)
p.add_argument("--body", required=True)
a = p.parse_args()

code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
ci = yaml.safe_load(Path("config/ci.yml").read_text())
if a.flow == "hotfix":
    roles = ci["hotfix"]["backmerge"] if a.event == "backmerge" else ci["hotfix"][a.env][a.event]
else:
    roles = ci["promotion"][a.env][a.event]
if a.event == "deployed" and os.environ.get("RESULT", "success") != "success":
    sect = ci["hotfix"][a.env] if a.flow == "hotfix" else ci["promotion"][a.env]
    roles = sect.get("deploy_failed", roles)
to = []
for r in roles:
    addr = os.environ.get(f"MAIL_{r.upper()}", "")
    if addr.count("@") != 1:
        sys.exit(f"MAIL secret for {r} is missing")
    to.append(addr)

urgent = "" if a.flow != "hotfix" else "URGENT " if a.event == "request" else "HOTFIX "
msg = EmailMessage()
msg["Subject"] = f"{urgent}[LH {code.upper()} {a.env}] {a.subject}"
msg["From"] = os.environ["SMTP_USER"]
msg["To"] = ", ".join(sorted(set(to)))
msg.set_content(a.body + "\n\n-- sent by GitHub Actions; reply is not read.")
with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as s:
    s.login(os.environ["SMTP_USER"], os.environ["SMTP_APP_PASSWORD"])
    s.send_message(msg)
print(f"mail sent: {a.flow} {a.event} ({a.env}) to {', '.join(roles)}")
