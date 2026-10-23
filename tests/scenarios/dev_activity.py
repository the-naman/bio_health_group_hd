"""Scenario test, run by tests/test_scenarios.py in its own process: python <this file> <repo copy> <work dir>.
Prints PASS/FAIL per step and a last line "... -> PASS" or "-> FAIL". Synthetic values only; mail addresses are
built at run time (AT) so no address-shaped text is ever committed."""

AT = chr(64)
import sys
import os
import types
import runpy
import io
import contextlib
import time

REPO = os.path.abspath(sys.argv[1])
os.chdir(REPO)
NS = types.SimpleNamespace
E = lambda v: NS(value=v)
now = time.time() * 1000


def run_(start, res, tasks):
    return NS(
        start_time=start,
        end_time=start + 600000 if res != "RUNNING" else None,
        state=NS(result_state=E(res) if res != "RUNNING" else None, life_cycle_state=E(res)),
        tasks=[NS(task_key=k, state=NS(result_state=E(v))) for k, v in tasks],
    )


JOBS = [
    NS(job_id=1, settings=NS(name="lh_X_dev_daily_developer")),
    NS(job_id=2, settings=NS(name="lh_X_dev_daily_teamlead")),
    NS(job_id=3, settings=NS(name="lh_X_test_daily")),
]
RUNS = {
    1: [run_(now - 3600e3, "SUCCESS", [("bronze", "SUCCESS"), ("silver_rebuild", "EXCLUDED")])],
    2: [run_(now - 7200e3, "FAILED", [("masks", "FAILED"), ("gold", "UPSTREAM_FAILED")])],
    3: [run_(now, "SUCCESS", [])],
}
STATE = {"runs": RUNS}
calls = []


def list_runs(job_id, start_time_from, expand_tasks):
    calls.append(start_time_from)
    return [r for r in STATE["runs"].get(job_id, []) if r.start_time >= start_time_from]


import yaml

code = yaml.safe_load(open("config/company.yml"))["code"]
for j in JOBS:
    j.settings.name = j.settings.name.replace("X", code)
w = NS(
    jobs=NS(list=lambda: JOBS, list_runs=list_runs),
    warehouses=NS(list=lambda: (_ for _ in ()).throw(AssertionError("no warehouse in dev mode"))),
    statement_execution=NS(execute_statement=lambda **k: (_ for _ in ()).throw(AssertionError("no SQL in dev mode"))),
)
sdk = types.ModuleType("databricks.sdk")
sdk.WorkspaceClient = lambda: w
sq = types.ModuleType("databricks.sdk.service.sql")
sq.StatementState = NS()
sq.StatementParameterListItem = None
sys.modules.update(
    {
        "databricks": types.ModuleType("databricks"),
        "databricks.sdk": sdk,
        "databricks.sdk.service": types.ModuleType("s"),
        "databricks.sdk.service.sql": sq,
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
        pass

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
    code_ = 0
    with contextlib.redirect_stdout(buf):
        try:
            runpy.run_path("ci/alerts.py", run_name="__main__")
        except SystemExit as e:
            code_ = e.code
    return buf.getvalue(), code_


ok = []


def c(n, x, d=""):
    ok.append(bool(x))
    print(("PASS " if x else "FAIL ") + n + ("" if x else f" -> {d}"))


o, e = run("--env", "dev", "--dev-activity", "--scheduled")
c("scheduled quiet before live", "not live yet" in o and not SENT, o)
o, e = run("--env", "dev")
c("dev without --dev-activity refused", e and "goes with" in str(e), e)
o, e = run("--env", "test", "--dev-activity")
c("--dev-activity with test refused", e and "goes with" in str(e), e)
o, e = run("--env", "dev", "--dev-activity", "--dry-run")
c("2 dev runs listed, test job ignored", "2 run(s)" in o and "1 not successful" in o and "test_daily" not in o, o)
c("failed tasks named, excluded task not", "tasks not ok: gold, masks" in o and "silver_rebuild" not in o, o)
c("dry run sends nothing", not SENT)
c("window = last 24 h", all(abs((now - x) / 3.6e6 - 24) < 0.1 for x in calls), calls)
o, e = run("--env", "dev", "--dev-activity")
c(
    "send: one mail to developer + teamlead only",
    len(SENT) == 1 and SENT[0]["To"] == "d" + AT + "x.test, t" + AT + "x.test",
    SENT and SENT[0]["To"],
)
c("no address printed", "" + AT + "x.test" not in o)
STATE["runs"] = {}
o, e = run("--env", "dev", "--dev-activity")
c("no runs = no mail", "nothing to send" in o and len(SENT) == 1, o)
print(f"\n{code} dev-activity tests: {sum(ok)} of {len(ok)} ->", "PASS" if all(ok) else "FAIL")
