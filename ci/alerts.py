"""5.12: operational alerts and the weekly digest by Gmail (same file in both repos). Run by
.github/workflows/alerts.yml as service principal sp_lh_notices (group lh_ci, read-only: gold views, ops tables).
Rules and recipients: config/alerts.yml. Addresses and the password are never printed.

Alerts (every two hours; all new findings in one mail; each finding sent once via <catalog>.ops.notice_log):
  rejected     Bronze refused a source file (audit row status 'rejected': file name and reason only)
  rebuild      Silver failed its checkpoint and was rebuilt from Bronze (the "restore" path of 9c)
  quarantine   unusually many new quarantine rows on the latest day (spike against the 7 days before)
  rescued      a source sends a column nobody planned for (Bronze _rescued_data, view v_health_rescued)
  backlog      BHG: FQF days waiting too long to be merged into the group figures
  over_limit   BHG: the number of people owing the group more than the policy limit changed
  velocity     FQF (5.14): a customer had too many failed payments in one 10-minute window (gold.payment_failure_windows)
Not repeated: a failed task (Databricks mails the fixers) and "figures delayed" (ci/notices.py).

Digest (Monday 09:15 IST): runs and failures of the week, freshness, quarantine, new columns, merge and group
numbers (BHG), alerts and notices sent. Once per ISO week.

Dev activity (daily 20:45 IST, requirement 10: Developer <-> Team Lead): every run of the per-person dev jobs
lh_<code>_dev_daily_<role> in the last 24 hours, with result, duration and failed tasks; no runs = no mail. Read from
the Jobs API only (lh_ci has Can View on the dev jobs), no data. Roles: config/alerts.yml dev_activity_roles.
    python ci/alerts.py --env prod                      # check and send new alerts
    python ci/alerts.py --env test --dry-run            # print what would be sent, send and record nothing
    python ci/alerts.py --env test --sample             # print every alert with today's data, even if sent before
    python ci/alerts.py --env prod --digest [--dry-run]
    python ci/alerts.py --env dev --dev-activity [--dry-run]
    --scheduled                                         # used by the schedule: quiet until config live: true
"""
import argparse, os, smtplib, ssl, sys, time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

p = argparse.ArgumentParser()
p.add_argument("--env", required=True, choices=["dev", "test", "prod"])
p.add_argument("--digest", action="store_true")
p.add_argument("--dev-activity", action="store_true")
p.add_argument("--dry-run", action="store_true")
p.add_argument("--sample", action="store_true")
p.add_argument("--scheduled", action="store_true")
a = p.parse_args()

company = yaml.safe_load(open("config/company.yml"))
code, name = company["code"], company["name"]
acfg = yaml.safe_load(open("config/alerts.yml"))
if (a.env == "dev") != a.dev_activity:
    sys.exit("--dev-activity goes with --env dev, and --env dev only with --dev-activity")
if a.scheduled and not acfg["live"].get(a.env):
    print(f"{a.env}: not live yet (config/alerts.yml live), nothing to do")
    sys.exit(0)
roles = (acfg["dev_activity_roles"] if a.dev_activity else acfg["digest_roles" if a.digest else "roles"][a.env])
cat = f"{code}_{a.env}"
IST = timezone(timedelta(hours=5, minutes=30))
now = datetime.now(IST)
today = now.strftime("%Y-%m-%d")
w = WorkspaceClient()


def mail(subject, body):
    """One mail to the roles (nothing recorded); dry run / sample only prints."""
    print(f"\n--- to {', '.join(roles)}\nSubject: [{name} {a.env}] {subject}\n{body}")
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
    msg.set_content(body + "\n\n-- Lakehouse operations alert. Please do not reply.")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as s:
        s.login(os.environ["SMTP_USER"], os.environ["SMTP_APP_PASSWORD"])
        s.send_message(msg)
    print(f"mail sent to {', '.join(roles)}")


# ---------------------------------------------------------------- daily dev activity (requirement 10)
if a.dev_activity:
    prefix = f"lh_{code}_dev_daily_"
    since = (time.time() - 24 * 3600) * 1000
    lines, n_ok, n_bad = [], 0, 0
    for job in sorted((j for j in w.jobs.list() if (j.settings and j.settings.name or "").startswith(prefix)),
                      key=lambda j: j.settings.name):
        owner = job.settings.name[len(prefix):]
        for r in w.jobs.list_runs(job_id=job.job_id, start_time_from=int(since), expand_tasks=True):
            st = r.state
            res = (st.result_state.value if st and st.result_state else (st.life_cycle_state.value if st and st.life_cycle_state else "?"))
            mins = f"{(r.end_time - r.start_time) / 60000:.0f} min" if r.end_time and r.start_time else "running"
            bad = sorted({t.task_key for t in (r.tasks or []) if t.state and t.state.result_state
                          and t.state.result_state.value not in ("SUCCESS", "EXCLUDED")})
            ok = res == "SUCCESS"
            n_ok, n_bad = n_ok + ok, n_bad + (not ok)
            when = datetime.fromtimestamp(r.start_time / 1000, IST).strftime("%d %b %H:%M IST")
            lines.append(f"  {owner:10} {when}  {res:10} {mins:>9}" + (f"  tasks not ok: {', '.join(bad)}" if bad else ""))
    print(f"dev activity: {len(lines)} run(s) in the last 24 h")
    if not lines:
        print("dev activity: nothing to send")
        sys.exit(0)
    mail(f"Dev activity: {len(lines)} run(s), {n_bad} not successful",
         f"Dev job runs of {name} in the last 24 hours (each person's own dev job, newest data in their own Git folder):\n\n"
         + "\n".join(lines) + "\n\nDetails: Jobs & Pipelines -> lh_" + code + "_dev_daily_<role> -> the run.")
    sys.exit(0)

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


def exists(schema, table):
    return bool(sql(f"SHOW TABLES IN {schema} LIKE '{table}'"))


def ist(ts):
    """'2026-10-22 04:23:21.5' (UTC, as Spark prints it) -> '22 Oct 2026 09:53 IST'."""
    t = datetime.strptime(ts.replace("T", " ")[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    return t.astimezone(IST).strftime("%d %b %Y %H:%M IST")


def num(x):
    return int(float(x or 0))


def send(subject, body, records):
    """One mail to the roles; then every (notice, ref) in records is written to ops.notice_log."""
    print(f"\n--- to {', '.join(roles)}\nSubject: [{name} {a.env}] {subject}\n{body}")
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
    msg.set_content(body + "\n\n-- Lakehouse operations alert. Please do not reply.")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as s:
        s.login(os.environ["SMTP_USER"], os.environ["SMTP_APP_PASSWORD"])
        s.send_message(msg)
    for notice, ref in records:
        sql("INSERT INTO ops.notice_log VALUES (:n, :r, current_timestamp(), :roles)", n=notice, r=ref,
            roles=",".join(roles))
    print(f"mail sent to {', '.join(roles)}, {len(records)} item(s) recorded")


sql("""CREATE TABLE IF NOT EXISTS ops.notice_log (notice STRING, ref STRING, sent_at TIMESTAMP, roles STRING)
       COMMENT '10e/5.12: notices and alerts already sent (ci/notices.py, ci/alerts.py); never resent'""")
days = int(acfg.get("lookback_days", 3))
has_audit = exists("ops", "audit_runs")


# ---------------------------------------------------------------- weekly digest
if a.digest:
    y, wk, _ = now.isocalendar()
    ref = f"{y}-W{wk:02d}"
    if not a.sample and not a.dry_run and sql("SELECT 1 FROM ops.notice_log WHERE notice = 'digest' AND ref = :r", r=ref):
        print(f"digest {ref} already sent")
        sys.exit(0)
    out = [f"Weekly data report for {name} ({a.env}), week {ref}, as of {now.strftime('%d %b %Y %H:%M IST')}.", ""]
    if exists("gold", "v_health_runs"):
        runs = sql("SELECT job, last_status, CAST(last_run AS STRING), runs_7d, failures_7d FROM gold.v_health_runs "
                   "ORDER BY failures_7d DESC, job")
        total, bad = sum(num(r[3]) for r in runs), sum(num(r[4]) for r in runs)
        out.append(f"Runs in the last 7 days: {total} steps, {bad} not successful (rejected files count here too).")
        out += [f"  {j:28} {f'{num(n)} not ok':>10}   last: {s} {ist(t) if t else ''}"
                for j, s, t, _, n in runs if num(n)]
    if exists("gold", "v_health_freshness"):
        f = sql("SELECT fact, CAST(data_to AS STRING), days_behind FROM gold.v_health_freshness")
        if f:
            out.append(f"Freshness: {f[0][0]} reaches {f[0][1]} ({f[0][2]} days behind today).")
    if exists("gold", "v_health_quarantine"):
        q = sql("SELECT table_name, sum(rows) FROM gold.v_health_quarantine WHERE status = 'open' "
                "GROUP BY table_name ORDER BY 2 DESC")
        out.append(f"Quarantine, rows still open: {sum(num(r[1]) for r in q)}"
                   + (" (" + ", ".join(f"{t} {num(n)}" for t, n in q[:6]) + ")" if q else ""))
    if exists("gold", "v_health_quarantine_daily"):
        n = sql("SELECT sum(rows) FROM gold.v_health_quarantine_daily WHERE day > date_sub(current_date(), 7)")
        out.append(f"New quarantine rows this week: {num(n[0][0]) if n else 0}")
    if exists("gold", "v_health_rescued"):
        r = sql("SELECT table_name, key FROM gold.v_health_rescued WHERE NOT planned ORDER BY 1, 2")
        out.append("New source columns not planned yet: " + (", ".join(f"{t}.{k}" for t, k in r) if r else "none"))
    if code == "bhg" and exists("gold", "v_health_merge"):
        m = sql("SELECT status, backlog_days, CAST(fqf_data_to AS STRING) FROM gold.v_health_merge "
                "WHERE env = :e ORDER BY run_ts DESC LIMIT 1", e=a.env)
        if m:
            out.append(f"Group merge: {m[0][0]}, FQF data to {m[0][2]}, days waiting {m[0][1]}.")
        k = dict(sql("SELECT kpi, CAST(value AS STRING) FROM gold.v_group_kpi")) if exists("gold", "v_group_kpi") else {}
        if k:
            out.append("Group: " + ", ".join(f"{x.replace('_', ' ')} {num(k.get(x))}" for x in
                       ("people", "people_shared", "people_over_limit", "reviews_open", "intercompany_breaks")))
    s = sql("SELECT notice, count(*) FROM ops.notice_log WHERE sent_at >= current_timestamp() - INTERVAL 7 DAYS "
            "AND notice <> 'digest' GROUP BY notice ORDER BY notice")
    out.append("Mails sent this week: " + (", ".join(f"{n} {c}" for n, c in s) if s else "none"))
    send(f"Weekly data report ({ref})", "\n".join(out), [("digest", ref)])
    sys.exit(0)


# ---------------------------------------------------------------- alerts
found = []                                       # (kind, ref, text)
if has_audit:
    for job, ts, msg in sql(f"""SELECT job, CAST(run_ts AS STRING), message FROM ops.audit_runs
                                WHERE status = 'rejected' AND run_ts >= current_timestamp() - INTERVAL {days} DAYS
                                ORDER BY run_ts"""):
        found.append(("rejected", f"{job}|{ts}", f"{job} refused a file at {ist(ts)}: {msg}"))
    for ts, status, msg in sql(f"""SELECT CAST(run_ts AS STRING), status, message FROM ops.audit_runs
                                   WHERE job = 'silver_rebuild' AND run_ts >= current_timestamp() - INTERVAL {days} DAYS
                                   ORDER BY run_ts"""):
        found.append(("rebuild", ts, f"Silver failed its checkpoint and was rebuilt from Bronze at {ist(ts)} "
                                     f"({status}): {msg}"))

if exists("gold", "v_health_quarantine_daily"):
    qs = acfg["quarantine_spike"]
    per_day = {d: num(n) for d, n in sql("""SELECT CAST(day AS STRING), sum(rows) FROM gold.v_health_quarantine_daily
                                            WHERE day >= date_sub(current_date(), 8) GROUP BY day""")}
    if per_day:
        last = max(per_day)
        ld = datetime.strptime(last, "%Y-%m-%d").date()
        before = [per_day.get(str(ld - timedelta(days=i)), 0) for i in range(1, 8)]
        avg = sum(before) / 7
        n = per_day[last]
        print(f"quarantine: {n} new rows on {last}, daily average before {avg:.1f}")
        if n >= qs["min_rows"] and n >= qs["factor"] * avg and (now.date() - ld).days <= 1:
            detail = sql("""SELECT table_name, rule, sum(rows) FROM gold.v_health_quarantine_daily WHERE day = :d
                            GROUP BY ALL ORDER BY 3 DESC LIMIT 8""", d=last)
            found.append(("quarantine", last, f"{n} new quarantine rows on {last} (daily average before: {avg:.1f}): "
                          + ", ".join(f"{t}/{r} {num(c)}" for t, r, c in detail)))

if exists("gold", "v_health_rescued"):
    for t, k, n, ts in sql("""SELECT table_name, key, CAST(rows AS STRING), CAST(first_seen AS STRING)
                              FROM gold.v_health_rescued WHERE NOT planned ORDER BY 1, 2"""):
        found.append(("rescued", f"{t}.{k}", f"{t}: the source sends a new column '{k}' ({num(n)} Bronze rows since "
                      f"{ist(ts) if ts else '-'}). It waits in _rescued_data; to use it add it to from_rescued in "
                      "config/silver.yml through a reviewed PR."))

if code == "bhg" and exists("gold", "v_health_merge"):
    m = sql("""SELECT backlog_days, status, CAST(fqf_data_to AS STRING) FROM gold.v_health_merge
               WHERE env = :e ORDER BY run_ts DESC LIMIT 1""", e=a.env)
    if m and num(m[0][0]) >= int(acfg["merge_backlog_days"]):
        found.append(("backlog", today, f"Group merge {m[0][1]}: {num(m[0][0])} FQF business days are still waiting "
                      f"(FQF data reaches {m[0][2]}). Check lh_fqf_{a.env}_daily."))

if code == "bhg" and exists("gold", "v_group_kpi"):
    v = sql("SELECT CAST(value AS STRING) FROM gold.v_group_kpi WHERE kpi = 'people_over_limit'")
    if v:
        n = num(v[0][0])
        prev = sql("SELECT ref FROM ops.notice_log WHERE notice = 'alert_over_limit' ORDER BY sent_at DESC LIMIT 1")
        p_n = int(prev[0][0].split("@")[0]) if prev else 0
        if n != p_n or a.sample:
            found.append(("over_limit", f"{n}@{today}", f"People owing the group more than the policy limit: {n} "
                          f"(was {p_n}). Details: dashboard group_overview_{a.env}, view v_group_exposure."))

if exists("gold", "payment_failure_windows"):
    for ws, cust, n, amt in sql(f"""SELECT CAST(window_start AS STRING), customer_ref, CAST(failed_payments AS STRING),
                                          CAST(failed_amount AS STRING) FROM gold.payment_failure_windows
                                   WHERE is_alert AND window_start >= current_timestamp() - INTERVAL {days} DAYS
                                   ORDER BY window_start"""):
        found.append(("velocity", f"{ws}|{cust}", f"{cust}: {num(n)} failed payments ({float(amt or 0):,.2f} INR) in the "
                      f"10 minutes from {ist(ws)}. Check the customer's payment method before the next instalment."))

if not a.sample:
    done = {(x, y) for x, y in sql("SELECT notice, ref FROM ops.notice_log WHERE notice LIKE 'alert_%'")}
    found = [f for f in found if (f"alert_{f[0]}", f[1]) not in done]
if not found:
    print("alerts: nothing new to send")
    sys.exit(0)
titles = {"rejected": "Files refused by Bronze", "rebuild": "Silver rebuilt from Bronze",
          "quarantine": "Quarantine spike", "rescued": "New source columns", "backlog": "Group merge waiting",
          "over_limit": "Over-limit customers", "velocity": "Failed-payment velocity"}
body = []
for kind in titles:
    items = [t for k, _, t in found if k == kind]
    if items:
        body += [f"{titles[kind]}:"] + [f"  - {t}" for t in items] + [""]
kinds = sorted({k for k, _, _ in found}, key=list(titles).index)
send(f"Data alert: {', '.join(titles[k].lower() for k in kinds)}", "\n".join(body).rstrip(),
     [(f"alert_{k}", r) for k, r, _ in found])
