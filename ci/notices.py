"""10e: business notices by Gmail (same file in both repos). Run by .github/workflows/notices.yml every two hours, with
the admin token (DATABRICKS_HOST / DATABRICKS_TOKEN) and the MAIL_* secrets. Nothing is sent twice: every notice is
recorded in <catalog>.ops.notice_log, and only a notice that is not recorded yet goes out.

  delayed    the last finished daily run did not complete Gold, group, BI views or publish -> "today's figures are
             delayed" (once per run)
  recovered  the next good run after a "delayed" notice -> "figures are up to date again"
  merge      BHG only: a group run merged new FQF days into the group figures -> what was merged (once per group run)

Tier 1 (technical failure mails to the people who fix it) comes from Databricks itself, see config/jobs.yml (notify).
Roles per environment: config/jobs.yml, business_notice. Addresses and the password are never printed.
    python ci/notices.py --env prod                 # check and send
    python ci/notices.py --env test --dry-run       # check, print what would be sent, send and record nothing
    python ci/notices.py --env test --sample        # print the three notices with today's data, whatever the state
"""
import argparse, os, smtplib, ssl, sys, time
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

p = argparse.ArgumentParser()
p.add_argument("--env", required=True, choices=["test", "prod"])
p.add_argument("--dry-run", action="store_true")
p.add_argument("--sample", action="store_true")
a = p.parse_args()

company = yaml.safe_load(open("config/company.yml"))
code, name = company["code"], company["name"]
jcfg = yaml.safe_load(open("config/jobs.yml"))
notice_cfg = jcfg["business_notice"]
roles = notice_cfg["roles"][a.env]
cat = f"{code}_{a.env}"
IST = timezone(timedelta(hours=5, minutes=30))
w = WorkspaceClient()
wh_name = yaml.safe_load(open("config/bi.yml"))["warehouse"]
wh = [x.id for x in w.warehouses.list() if x.name == wh_name]
if len(wh) != 1:
    sys.exit(f"warehouse '{wh_name}' not found once")


def sql(statement, **params):
    """Rows (lists of strings) of one SQL statement on the warehouse; params are passed as values, never pasted."""
    items = [StatementParameterListItem(name=k, value=str(v)) for k, v in params.items()]
    r = w.statement_execution.execute_statement(statement=statement, warehouse_id=wh[0], catalog=cat,
                                                parameters=items or None, wait_timeout="50s")
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
    sql("INSERT INTO ops.notice_log VALUES (:n, :r, current_timestamp(), :roles)", n=notice, r=ref, roles=",".join(roles))
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
        if t.task_key in key_tasks and (t.task_key not in last or (t.attempt_number or 0) >= (last[t.task_key].attempt_number or 0)):
            last[t.task_key] = t
    return sorted(k for k, t in last.items()
                  if not (t.state and t.state.result_state and t.state.result_state.value == "SUCCESS"))


runs = list(w.jobs.list_runs(job_id=found[0].job_id, completed_only=True, limit=20))
fact = f"fact_{code}_event"
data_to = sql(f"SELECT CAST(max(date_key) AS STRING) FROM gold.{fact} WHERE date_key <= :d"
              + (" AND event_type <> 'emi_due'" if code == "fqf" else ""), d=date.today().strftime("%Y%m%d"))
data_to = datetime.strptime(data_to[0][0], "%Y%m%d").strftime("%d %b %Y") if data_to and data_to[0][0] else "unknown"

if not runs:
    print(f"{job_name}: no finished run yet, nothing to report")
else:
    run, bad = runs[0], failed_tasks(runs[0].run_id)
    print(f"{job_name}: last finished run {ist(run.start_time)}, business tasks not done: {bad or 'none'}")
    ok_before = next((r for r in runs[1:] if not failed_tasks(r.run_id)), None)
    last = sql("SELECT notice FROM ops.notice_log WHERE notice IN ('delayed', 'recovered') ORDER BY sent_at DESC LIMIT 1")
    last = last[0][0] if last else None
    delayed_body = (f"The daily update of the {name} figures that started {ist(run.start_time)} did not finish.\n"
                    f"Dashboards still show the last complete update"
                    + (f" ({ist(ok_before.start_time)})" if ok_before else "") + f", with data up to {data_to}.\n"
                    "The data team has already been alerted and is working on it. You will get a second message as "
                    "soon as the figures are up to date again.")
    recovered_body = (f"The {name} figures are up to date again (update started {ist(run.start_time)}).\n"
                      f"Dashboards now show data up to {data_to}.")
    if a.sample:
        send("delayed", "sample", "Today's figures are delayed", delayed_body)
        send("recovered", "sample", "Figures are up to date again", recovered_body)
    elif bad and not sent("delayed", str(run.run_id)):
        send("delayed", str(run.run_id), "Today's figures are delayed", delayed_body)
    elif not bad and last == "delayed":
        send("recovered", str(run.run_id), "Figures are up to date again", recovered_body)
    else:
        print("figures: nothing to send")

# ---- 2. merge notice (parent only): what came in from FQF in each group run that merged new FQF days
if code == "bhg":
    rows = sql("""SELECT CAST(run_ts AS STRING), CAST(fqf_data_to AS STRING), new_fqf_days, backlog_days, status
                  FROM ops.merge_log
                  WHERE env = :env AND run_ts >= current_timestamp() - INTERVAL 3 DAYS
                    AND (new_fqf_days > 0 OR :sample = 'yes')
                  ORDER BY run_ts""", env=a.env, sample="yes" if a.sample else "no")
    if a.sample:
        rows = rows[-1:]
    if not rows:
        print("merge: no group run merged new FQF days in the last 3 days")
    for run_ts, f_to, new_days, backlog, status in rows:
        if not a.sample and sent("merge", run_ts):
            continue
        end = datetime.strptime(f_to, "%Y-%m-%d").date() if f_to else date.today()
        days = max(int(new_days or 1), 1)
        start = end - timedelta(days=days - 1)
        fig = {m: (n, amt, ic) for m, n, amt, ic in sql(
            """SELECT measure, CAST(sum(events) AS STRING), CAST(sum(amount) AS STRING), CAST(sum(intercompany) AS STRING)
               FROM gold.fact_group_daily WHERE company_code = 'fqf' AND date_key BETWEEN :d1 AND :d2
               GROUP BY measure ORDER BY measure""", d1=start.strftime("%Y%m%d"), d2=end.strftime("%Y%m%d"))}
        g = lambda m, i: fig.get(m, ("0", "0", "0"))[i]
        span = start.strftime("%d %b %Y") + ("" if days == 1 else f" to {end.strftime('%d %b %Y')}")
        body = (f"Flexi Quant Finance data for {span} ({days} day{'s' if days > 1 else ''}) has been merged into the "
                f"group figures.\n\nWhat came in from Flexi Quant Finance:\n"
                f"  loans paid out (INR)                 {money(g('disbursed', 1)):>16}   {g('disbursed', 0)} loans\n"
                f"  of which paid BHG hospital bills     {money(g('disbursed', 2)):>16}   counted once in group totals\n"
                f"  instalments collected                {money(g('collected', 1)):>16}   {g('collected', 0)} payments\n"
                f"  interest fallen due                  {money(g('interest_due', 1)):>16}\n"
                f"  late fees                            {money(g('fees', 1)):>16}\n\n"
                f"Merge status: {status}. Days still waiting from Flexi Quant Finance: {backlog}.")
        send("merge", run_ts, f"Flexi Quant Finance data merged ({span})", body)
