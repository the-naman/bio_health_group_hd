"""10e: business notices by Gmail (same file in both repos). Run by .github/workflows/notices.yml every two hours as
service principal sp_lh_notices (group lh_ci, OAuth client id/secret, read-only grants) with the MAIL_* secrets.
Nothing is sent twice: every notice is recorded in <catalog>.ops.notice_log, and only a notice not recorded yet goes out.

  delayed    the last finished daily run did not complete Gold, group, BI views or publish (once per run), or a run
             is stuck (once per run), or there was no successful run for too long (once per day) -> "today's
             figures are delayed"; limits in config/jobs.yml business_notice.freshness
  recovered  the next good run after a "delayed" notice -> "figures are up to date again"
  merge      BHG only: a group run merged new FQF days into the group figures -> what was merged (once per group run)

Tier 1 (technical failure mails to the people who fix it) comes from Databricks itself, see config/jobs.yml (notify).
Roles per environment: config/jobs.yml, business_notice. Addresses and the password are never printed.
    python ci/notices.py --env prod                 # check and send
    python ci/notices.py --env test --dry-run       # check, print what would be sent, send and record nothing
    python ci/notices.py --env test --sample        # print the three notices with today's data, whatever the state
"""

import argparse
import os
import smtplib
import ssl
import sys
import time
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

p = argparse.ArgumentParser()
p.add_argument("--env", required=True, choices=["test", "prod"])
p.add_argument("--dry-run", action="store_true")
p.add_argument("--sample", action="store_true")
a = p.parse_args()

company = yaml.safe_load(Path("config/company.yml").read_text())
code, name = company["code"], company["name"]
jcfg = yaml.safe_load(Path("config/jobs.yml").read_text())
notice_cfg = jcfg["business_notice"]
roles = notice_cfg["roles"][a.env]
cat = f"{code}_{a.env}"
IST = timezone(timedelta(hours=5, minutes=30))
w = WorkspaceClient()
wh_name = yaml.safe_load(Path("config/bi.yml").read_text())["warehouse"]
wh = [x.id for x in w.warehouses.list() if x.name == wh_name]
if len(wh) != 1:
    sys.exit(f"warehouse '{wh_name}' not found once")


def sql(statement, **params):
    """Rows (lists of strings) of one SQL statement on the warehouse; params are passed as values, never pasted."""
    items = [StatementParameterListItem(name=k, value=str(v)) for k, v in params.items()]
    r = w.statement_execution.execute_statement(
        statement=statement, warehouse_id=wh[0], catalog=cat, parameters=items or None, wait_timeout="50s"
    )
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(3)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        sys.exit(f"SQL failed: {r.status.error.message if r.status.error else r.status.state}")
    return (r.result.data_array or []) if r.result else []


def ist(ms):
    return datetime.fromtimestamp(ms / 1000, IST).strftime("%d %b %Y %H:%M IST")


def money(x):
    return f"{float(x or 0):,.2f}"


def sent(notice, ref):
    return bool(sql("SELECT 1 FROM ops.notice_log WHERE notice = :n AND ref = :r", n=notice, r=ref))


def send(notice, ref, subject, body):
    print(f"\n--- {notice} ({ref}) to {', '.join(roles)}\nSubject: {subject}\n{body}")
    if a.dry_run or a.sample:
        print("(not sent: dry run)")
        return
    to = []
    for r in roles:
        addr = os.environ.get(f"MAIL_{r.upper()}", "")
        if addr.count("@") != 1:
            sys.exit(f"MAIL secret for {r} is missing")
        to.append(addr)
    msg = EmailMessage()
    msg["Subject"] = f"[{name} {a.env}] {subject}"
    msg["From"] = os.environ["SMTP_USER"]
    msg["To"] = ", ".join(sorted(set(to)))
    msg.set_content(body + "\n\n-- Lakehouse business notice. Please do not reply.")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as s:
        s.login(os.environ["SMTP_USER"], os.environ["SMTP_APP_PASSWORD"])
        s.send_message(msg)
    sql(
        "INSERT INTO ops.notice_log VALUES (:n, :r, current_timestamp(), :roles)",
        n=notice,
        r=ref,
        roles=",".join(roles),
    )
    print(f"mail sent: {notice} to {', '.join(roles)}")


sql("""CREATE TABLE IF NOT EXISTS ops.notice_log (notice STRING, ref STRING, sent_at TIMESTAMP, roles STRING)
       COMMENT '10e: business notices already sent (ci/notices.py); one row per notice, never resent'""")

# ---- 1. figures delayed / up to date again, from the last finished run of the daily job
job_name = jcfg["name"].format(code=code, env=a.env)
found = list(w.jobs.list(name=job_name))
if len(found) != 1:
    sys.exit(f"{job_name}: found {len(found)} jobs")
key_tasks = set(notice_cfg["tasks"])


def failed_tasks(run_id):
    """Business tasks of a run whose last attempt did not succeed (also skipped after an earlier failure)."""
    last = {}
    for t in w.jobs.get_run(run_id).tasks or []:
        if t.task_key in key_tasks and (
            t.task_key not in last or (t.attempt_number or 0) >= (last[t.task_key].attempt_number or 0)
        ):
            last[t.task_key] = t
    return sorted(
        k for k, t in last.items() if not (t.state and t.state.result_state and t.state.result_state.value == "SUCCESS")
    )


job_id = found[0].job_id
fresh = notice_cfg.get("freshness", {})
max_since_ok = float(fresh.get("max_hours_since_success", 26))
max_running = float(fresh.get("max_running_hours", 3))
now_ms = time.time() * 1000
hours = lambda ms: (now_ms - ms) / 3_600_000

runs = list(w.jobs.list_runs(job_id=job_id, completed_only=True, limit=20))
active = list(w.jobs.list_runs(job_id=job_id, active_only=True, limit=5))
if not runs and not active:  # environment not live yet (prod before its first run): nothing to report, not an error
    print(f"{job_name}: no run yet, nothing to report")
    sys.exit(0)

fact = f"fact_{code}_event"  # Gold may not exist yet if the very first run failed
has_fact = (
    sql("SELECT count(*) FROM information_schema.tables WHERE table_schema = 'gold' AND table_name = :t", t=fact)[0][0]
    != "0"
)
data_to = (
    sql(
        f"SELECT CAST(max(date_key) AS STRING) FROM gold.{fact} WHERE date_key <= :d"
        + (" AND event_type <> 'emi_due'" if code == "fqf" else ""),
        d=date.today().strftime("%Y%m%d"),
    )
    if has_fact
    else None
)
data_to = datetime.strptime(data_to[0][0], "%Y%m%d").strftime("%d %b %Y") if data_to and data_to[0][0] else None

run = runs[0] if runs else None
bad = failed_tasks(run.run_id) if run else []
ok_before = next((r for r in runs if not failed_tasks(r.run_id)), None) if bad or not run else run
if run:
    print(f"{job_name}: last finished run {ist(run.start_time)}, business tasks not done: {bad or 'none'}")
stuck = next((r for r in active if r.start_time and hours(r.start_time) > max_running), None)
ok_age = hours(ok_before.end_time or ok_before.start_time) if ok_before else None
stale = ok_age is None or ok_age > max_since_ok
print(
    f"freshness: last successful run {ist(ok_before.start_time) if ok_before else 'none'}"
    f"{f' ({ok_age:.0f} h ago)' if ok_age is not None else ''}, stuck run: {'yes' if stuck else 'no'}"
)
last = sql("SELECT notice FROM ops.notice_log WHERE notice IN ('delayed', 'recovered') ORDER BY sent_at DESC LIMIT 1")
last = last[0][0] if last else None

shown = (
    (
        "Dashboards still show the last complete update"
        + (f" ({ist(ok_before.start_time)})" if ok_before else "")
        + f", with data up to {data_to}.\n"
    )
    if data_to
    else "There are no figures on the dashboards yet.\n"
) + (
    "The data team has already been alerted and is working on it. You will get a second message as "
    "soon as the figures are up to date again."
)
why = {
    "failed": f"The daily update of the {name} figures that started {ist(run.start_time) if run else '-'} did not finish.\n",
    "stuck": f"The daily update of the {name} figures that started {ist((stuck or run).start_time) if (stuck or run) else '-'} is still "
    f"running after more than {max_running:.0f} hours.\n",
    "stale": f"There has been no complete daily update of the {name} figures for more than {max_since_ok:.0f} hours.\n",
}
today = datetime.now(IST).strftime("%Y-%m-%d")
candidates = []  # (reason, ref): each sent once; the first not yet sent goes out
if bad:
    candidates.append(("failed", str(run.run_id)))
if stuck:
    candidates.append(("stuck", f"stuck-{stuck.run_id}"))
problem = bool(candidates) or stale
if stale and not sql(
    "SELECT 1 FROM ops.notice_log WHERE notice = 'delayed' AND sent_at >= current_timestamp() - INTERVAL 20 HOURS"
):
    candidates.append(("stale", f"stale-{today}"))  # quiet problem: at most one "delayed" a day
recovered_body = (
    f"The {name} figures are up to date again (update started {ist(ok_before.start_time) if ok_before else '-'}).\n"
    + (f"Dashboards now show data up to {data_to}." if data_to else "")
)
if a.sample:
    for reason in ("failed", "stuck", "stale"):
        send("delayed", f"sample-{reason}", "Today's figures are delayed", why[reason] + shown)
    send("recovered", "sample", "Figures are up to date again", recovered_body)
else:
    todo = next(((r, ref) for r, ref in candidates if not sent("delayed", ref)), None)
    if todo:
        send("delayed", todo[1], "Today's figures are delayed", why[todo[0]] + shown)
    elif not problem and last == "delayed":
        send("recovered", str(ok_before.run_id), "Figures are up to date again", recovered_body)
    else:
        print("figures: nothing to send" + (" (problem already reported)" if problem else ""))

# ---- 2. merge notice (parent only): what came in from FQF in each group run that merged new FQF days
if code == "bhg":
    rows = sql(
        """SELECT CAST(run_ts AS STRING), CAST(fqf_data_to AS STRING), new_fqf_days, backlog_days, status
                  FROM ops.merge_log
                  WHERE env = :env AND run_ts >= current_timestamp() - INTERVAL 3 DAYS
                    AND (new_fqf_days > 0 OR :sample = 'yes')
                  ORDER BY run_ts""",
        env=a.env,
        sample="yes" if a.sample else "no",
    )
    if a.sample:
        rows = rows[-1:]
    if not rows:
        print("merge: no group run merged new FQF days in the last 3 days")
    for run_ts, f_to, new_days, backlog, status in rows:
        if not a.sample and sent("merge", run_ts):
            continue
        end = datetime.strptime(f_to, "%Y-%m-%d").date() if f_to else date.today()
        new = int(new_days or 0)  # 0 only in --sample: the latest group run found no new FQF day
        days = max(new, 1)
        start = end - timedelta(days=days - 1)
        fig = {
            m: (n, amt, ic)
            for m, n, amt, ic in sql(
                """SELECT measure, CAST(sum(events) AS STRING), CAST(sum(amount) AS STRING), CAST(sum(intercompany) AS STRING)
               FROM gold.fact_group_daily WHERE company_code = 'fqf' AND date_key BETWEEN :d1 AND :d2
               GROUP BY measure ORDER BY measure""",
                d1=start.strftime("%Y%m%d"),
                d2=end.strftime("%Y%m%d"),
            )
        }
        g = lambda m, i: fig.get(m, ("0", "0", "0"))[i]  # noqa: B023 - used only in this loop turn
        span = start.strftime("%d %b %Y") + ("" if days == 1 else f" to {end.strftime('%d %b %Y')}")
        head = (
            f"Flexi Quant Finance data for {span} ({days} day{'s' if days > 1 else ''}) has been merged into the "
            "group figures."
            if new
            else f"(Sample) The latest group run found no new Flexi Quant Finance day; figures below are for {span}."
        )
        body = (
            head + "\n\nWhat came in from Flexi Quant Finance:\n"
            f"  loans paid out (INR)                 {money(g('disbursed', 1)):>16}   {g('disbursed', 0)} loans\n"
            f"  of which paid BHG hospital bills     {money(g('disbursed', 2)):>16}   counted once in group totals\n"
            f"  instalments collected                {money(g('collected', 1)):>16}   {g('collected', 0)} payments\n"
            f"  interest fallen due                  {money(g('interest_due', 1)):>16}\n"
            f"  late fees                            {money(g('fees', 1)):>16}\n\n"
            f"Merge status: {status}. Days still waiting from Flexi Quant Finance: {backlog}."
        )
        send("merge", run_ts, f"Flexi Quant Finance data merged ({span})", body)
