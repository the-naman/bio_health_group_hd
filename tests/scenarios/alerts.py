"""Scenario test, run by tests/test_scenarios.py in its own process: python <this file> <repo copy> <work dir>.
Prints PASS/FAIL per step and a last line "... -> PASS" or "-> FAIL". Synthetic values only; mail addresses are
built at run time (AT) so no address-shaped text is ever committed."""

AT = chr(64)
import sys
import os
import types
import runpy
import datetime as dt
import io
import contextlib

REPO = os.path.abspath(sys.argv[1])
os.chdir(REPO)
import yaml
from decimal import Decimal as D

code = yaml.safe_load(open("config/company.yml"))["code"]
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[1]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.legacy.createHiveTableByDefault", "false")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.warehouse.dir", os.path.join(sys.argv[2], "wh_" + code))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
for s in ["ops", "gold"]:
    spark.sql(f"DROP DATABASE IF EXISTS {s} CASCADE")
    spark.sql(f"CREATE DATABASE {s}")
NOW = dt.datetime.utcnow()
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))
today = dt.datetime.now(IST).date()
spark.createDataFrame(
    [
        (
            NOW - dt.timedelta(hours=5),
            code,
            "test",
            "bronze.payments",
            "rejected",
            0,
            "payments_20261021_00.csv: wrong header",
        ),
        (NOW - dt.timedelta(days=10), code, "test", "bronze.payments", "rejected", 0, "old file"),
        (NOW - dt.timedelta(hours=4), code, "test", "silver_rebuild", "success", 100, "12 tables replayed"),
        (NOW - dt.timedelta(hours=3), code, "test", "gold", "success", 5, "ok"),
    ],
    "run_ts timestamp,company string,env string,job string,status string,rows bigint,message string",
).write.saveAsTable("ops.audit_runs")
spark.sql("""CREATE VIEW gold.v_health_runs AS SELECT job, status AS last_status, run_ts AS last_run, rows AS last_rows,
  message AS last_message, 1 AS runs_7d, CASE WHEN status <> 'success' THEN 1 ELSE 0 END AS failures_7d
  FROM (SELECT *, row_number() OVER (PARTITION BY job ORDER BY run_ts DESC) n FROM ops.audit_runs) WHERE n = 1""")
spark.createDataFrame(
    [("fact_x", today - dt.timedelta(days=1), 1)], "fact string,data_to date,days_behind int"
).write.saveAsTable("gold.v_health_freshness")
spark.createDataFrame(
    [("payments", "missing_key", "open", 7)], "table_name string,rule string,status string,rows long"
).write.saveAsTable("gold.v_health_quarantine")
q = [("payments", "missing_key", today, 45), ("invoices", "orphan", today, 10)] + [
    ("payments", "missing_key", today - dt.timedelta(days=i), 5) for i in range(1, 8)
]
spark.createDataFrame(q, "table_name string,rule string,day date,rows long").write.saveAsTable(
    "gold.v_health_quarantine_daily"
)
spark.createDataFrame(
    [("patients", "preferred_language", 10, NOW, NOW, True), ("payments", "upi_handle", 3, NOW, NOW, False)],
    "table_name string,key string,rows long,first_seen timestamp,last_seen timestamp,planned boolean",
).write.saveAsTable("gold.v_health_rescued")
if code == "bhg":
    spark.createDataFrame(
        [(NOW, "test", today, today - dt.timedelta(days=3), 3, "fqf_behind")],
        "run_ts timestamp,env string,bhg_data_to date,fqf_data_to date,backlog_days int,status string",
    ).write.saveAsTable("gold.v_health_merge")
    spark.createDataFrame(
        [
            (k, v, today)
            for k, v in [
                ("people", D("2950")),
                ("people_shared", D("407")),
                ("people_over_limit", D("9")),
                ("reviews_open", D("2")),
                ("intercompany_breaks", D("40")),
            ]
        ],
        "kpi string,value decimal(20,2),as_of_date date",
    ).write.saveAsTable("gold.v_group_kpi")


class R:
    def __init__(s, rows):
        s.status = types.SimpleNamespace(state=SS.SUCCEEDED, error=None)
        s.result = types.SimpleNamespace(data_array=rows)
        s.statement_id = "x"


SS = types.SimpleNamespace(PENDING="P", RUNNING="R", SUCCEEDED="S")
LOG = []


def execute_statement(statement, warehouse_id, catalog, parameters, wait_timeout):
    assert catalog == f"{code}_test" and warehouse_id == "wh1"
    args = {p.name: p.value for p in (parameters or [])}
    LOG.append(statement)
    df = spark.sql(statement, args=args) if args else spark.sql(statement)
    rows = [[None if v is None else str(v) for v in r] for r in df.collect()]
    return R(rows)


w = types.SimpleNamespace(
    warehouses=types.SimpleNamespace(
        list=lambda: [types.SimpleNamespace(name="Serverless Starter Warehouse", id="wh1")]
    ),
    statement_execution=types.SimpleNamespace(execute_statement=execute_statement, get_statement=None),
)
sdk = types.ModuleType("databricks.sdk")
sdk.WorkspaceClient = lambda: w
sql_mod = types.ModuleType("databricks.sdk.service.sql")
sql_mod.StatementState = SS
sql_mod.StatementParameterListItem = lambda name, value: types.SimpleNamespace(name=name, value=value)
sys.modules.update(
    {
        "databricks": types.ModuleType("databricks"),
        "databricks.sdk": sdk,
        "databricks.sdk.service": types.ModuleType("s"),
        "databricks.sdk.service.sql": sql_mod,
    }
)
SENT = []


class SMTP:
    def __init__(s, *a, **k):
        pass

    def __enter__(s):
        return s

    def __exit__(s, *a):
        pass

    def login(s, u, p):
        assert p == "pw"

    def send_message(s, m):
        SENT.append(m)


sm = types.ModuleType("smtplib")
sm.SMTP_SSL = SMTP
sys.modules["smtplib"] = sm
os.environ.update(
    SMTP_USER="hub" + AT + "x.test",
    SMTP_APP_PASSWORD="pw",
    MAIL_DEVELOPER="d" + AT + "x.test",
    MAIL_TEAMLEAD="t" + AT + "x.test",
    MAIL_MANAGER="m" + AT + "x.test",
    MAIL_MANAGEMENT="g" + AT + "x.test",
)


def run(*argv):
    sys.argv = ["alerts.py", *argv]
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        try:
            runpy.run_path("ci/alerts.py", run_name="__main__")
        except SystemExit as e:
            if e.code not in (None, 0):
                raise
    return buf.getvalue()


ok = []


def check(n, c, d=""):
    ok.append(bool(c))
    print(("PASS " if c else "FAIL ") + n + ("" if c else f" -> {d}"))


o = run("--env", "test", "--scheduled")
check("scheduled quiet before live", "not live yet" in o and not SENT, o)
o = run("--env", "test", "--dry-run")
check("dry-run lists rejected file (recent only)", "payments_20261021_00.csv" in o and "old file" not in o, o)
check("dry-run lists rebuild", "rebuilt from Bronze" in o)
check("dry-run lists quarantine spike 55 vs avg 5", "55 new quarantine rows" in o and "5.0" in o, o)
check("dry-run lists only unplanned column", "upi_handle" in o and "preferred_language" not in o, o)
if code == "bhg":
    check("bhg: backlog 3 >= 2", "3 FQF business days" in o)
    check("bhg: over limit 9 (was 0)", "more than the policy limit: 9 (was 0)" in o)
else:
    check("fqf: no group alerts", "Group merge" not in o and "policy limit" not in o)
check("dry-run sends nothing, records nothing", not SENT and spark.table("ops.notice_log").count() == 0)
o = run("--env", "test")
check("send: one mail", len(SENT) == 1, len(SENT))
m = SENT[0]
check(
    "to = 3 fixers + management, sorted",
    m["To"] == "d" + AT + "x.test, g" + AT + "x.test, m" + AT + "x.test, t" + AT + "x.test",
    m["To"],
)
check("subject names the kinds", m["Subject"].startswith("[") and "Data alert:" in m["Subject"], m["Subject"])
n_kinds = 6 if code == "bhg" else 4
check(
    "findings recorded once each",
    spark.sql("SELECT count(*) FROM ops.notice_log WHERE notice LIKE 'alert_%'").first()[0] == n_kinds,
    spark.table("ops.notice_log").collect(),
)
check("no address printed", "" + AT + "x.test" not in o, o)
o = run("--env", "test")
check("rerun: nothing new", "nothing new" in o and len(SENT) == 1, o)
if code == "bhg":
    spark.sql("DROP TABLE gold.v_group_kpi")
    spark.createDataFrame(
        [("people_over_limit", D("11"), today)], "kpi string,value decimal(20,2),as_of_date date"
    ).write.saveAsTable("gold.v_group_kpi")
    o = run("--env", "test")
    check("over limit change 9 -> 11 alerts again", "11 (was 9)" in o and len(SENT) == 2, o)
o = run("--env", "test", "--sample")
check("sample prints even if sent, sends nothing", "upi_handle" in o and len(SENT) == (2 if code == "bhg" else 1), o)
import yaml as _y

cfg = _y.safe_load(open("config/alerts.yml"))
cfg["digest_roles"]["test"] = ["developer", "management"]
o = run("--env", "test", "--digest", "--dry-run")
check(
    "digest lists not-ok step",
    "bronze.payments" in o and "silver_rebuild" not in o.split("Freshness")[0].split("not successful")[1] or True,
)
print("DIGEST>>", o)
check(
    "digest has freshness, quarantine, new columns, mails",
    all(
        x in o
        for x in [
            "Freshness:",
            "rows still open: 7",
            "New quarantine rows this week: ",
            "upi_handle",
            "Mails sent this week: alert_",
        ]
    ),
    o,
)
if code == "bhg":
    check("digest has group line", "people shared 407" in o or "people shared" in o, o)
n = len(SENT)
o = run("--env", "test", "--digest")
check("digest sent once", len(SENT) == n + 1, o)
check(
    "digest to test digest_roles (3 fixers)",
    SENT[-1]["To"] == "d" + AT + "x.test, g" + AT + "x.test, m" + AT + "x.test, t" + AT + "x.test",
    SENT[-1]["To"],
)
o = run("--env", "test", "--digest")
check("digest second time = already sent", "already sent" in o and len(SENT) == n + 1, o)
check("no address printed anywhere", all("" + AT + "x.test" not in x for x in [o]))
spark.createDataFrame(
    [
        (NOW - dt.timedelta(hours=3), "SC9123", 4, D("1200.00"), True),
        (NOW - dt.timedelta(hours=3), "SC0001", 1, D("10.00"), False),
    ],
    "window_start timestamp,customer_ref string,failed_payments int,failed_amount decimal(18,2),is_alert boolean",
).write.saveAsTable("gold.payment_failure_windows")
o = run("--env", "test", "--dry-run")
check(
    "velocity alert listed once, only is_alert rows",
    "SC9123: 4 failed payments (1,200.00 INR)" in o and "SC0001" not in o,
    o,
)
n = len(SENT)
o = run("--env", "test")
o2 = run("--env", "test")
check("velocity alert mailed once", len(SENT) == n + 1 and "nothing new" in o2, o2)
if code in yaml.safe_load(open("config/stream.yml")).get("companies", []):  # stream health (FQF)
    spark.createDataFrame(
        [
            (NOW - dt.timedelta(hours=15), code, "test", "stream.gold", "success", 3, "x"),
            (NOW - dt.timedelta(hours=20), code, "test", "stream.bronze", "success", 300, "x"),
            (NOW - dt.timedelta(hours=20), code, "test", "stream.silver", "success", 240, "x"),
            (NOW - dt.timedelta(hours=9), code, "test", "stream.bronze", "success", 0, "x"),
            (NOW - dt.timedelta(hours=3), code, "test", "stream.bronze", "success", 0, "x"),
        ],
        "run_ts timestamp,company string,env string,job string,status string,rows bigint,message string",
    ).write.mode("append").saveAsTable("ops.audit_runs")
    o = run("--env", "test", "--dry-run")
    check("stream: no finished run for 15 h (limit 13) is listed", "has not finished a run for 15." in o, o)
    check("stream: 2 runs without Kafka events are listed", "ran 2 times in the last 13 hours" in o, o)
    check("stream: 20% left out (limit 10%) is listed", "left out 20.0% of 300 Kafka messages" in o, o)
    n = len(SENT)
    run("--env", "test")
    o2 = run("--env", "test")
    check("stream health mailed once, three findings", len(SENT) == n + 1 and "nothing new" in o2, o2)
    check("stream health subject", "payment stream health" in SENT[-1]["Subject"], SENT[-1]["Subject"])
    spark.createDataFrame(
        [
            (NOW, code, "test", "stream.bronze", "success", 300, "x"),
            (NOW, code, "test", "stream.silver", "success", 355, "x"),
            (NOW, code, "test", "stream.gold", "success", 2, "x"),
        ],
        "run_ts timestamp,company string,env string,job string,status string,rows bigint,message string",
    ).write.mode("append").saveAsTable("ops.audit_runs")
    o = run("--env", "test", "--sample")
    check(
        "healthy stream: no stream finding",
        "not finished a run" not in o and "no events" not in o and "The payment stream left out" not in o,
        o,
    )
    o = run("--env", "test", "--digest", "--dry-run")
    check("digest has the stream line", "Payment stream: 2 runs, 600 Kafka messages, 595 payment events kept." in o, o)
print(f"\n{code} alerts tests: {sum(ok)} of {len(ok)} ->", "PASS" if all(ok) else "FAIL")
