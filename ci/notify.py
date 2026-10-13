"""Promotion notices by Gmail (same file in both repos). Roles per event come from config/ci.yml; addresses from the
MAIL_<ROLE> environment variables (GitHub secrets). Addresses and the password are never printed.
    python ci/notify.py --event request --env test --subject "..." --body "..."
"""
import argparse, os, smtplib, ssl, sys
from email.message import EmailMessage
import yaml

p = argparse.ArgumentParser()
p.add_argument("--event", required=True, choices=["request", "result", "deployed"])
p.add_argument("--env", required=True, choices=["test", "prod"])
p.add_argument("--subject", required=True)
p.add_argument("--body", required=True)
a = p.parse_args()

code = yaml.safe_load(open("config/company.yml"))["code"]
roles = yaml.safe_load(open("config/ci.yml"))["promotion"][a.env][a.event]
to = []
for r in roles:
    addr = os.environ.get(f"MAIL_{r.upper()}", "")
    if addr.count("@") != 1:
        sys.exit(f"MAIL secret for {r} is missing")
    to.append(addr)

msg = EmailMessage()
msg["Subject"] = f"[LH {code.upper()} {a.env}] {a.subject}"
msg["From"] = os.environ["SMTP_USER"]
msg["To"] = ", ".join(sorted(set(to)))
msg.set_content(a.body + "\n\n-- sent by GitHub Actions; reply is not read.")
with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as s:
    s.login(os.environ["SMTP_USER"], os.environ["SMTP_APP_PASSWORD"])
    s.send_message(msg)
print(f"mail sent: {a.event} ({a.env}) to {', '.join(roles)}")

