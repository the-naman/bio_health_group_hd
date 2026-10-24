"""Scenario test, run by tests/test_scenarios.py in its own process: python <this file> <repo copy> <work dir>.
Prints PASS/FAIL per step and a last line "... -> PASS" or "-> FAIL". Synthetic values only; mail addresses are
built at run time (AT) so no address-shaped text is ever committed."""

AT = chr(64)
import os
import sys
import runpy
import smtplib
import io
import contextlib

REPO = sys.argv[1]
os.chdir(REPO)
sent = []


class FakeSMTP:
    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def login(self, *a):
        pass

    def send_message(self, m):
        sent.append(m)


smtplib.SMTP_SSL = FakeSMTP
for r in ["developer", "teamlead", "manager", "bizmanager", "management"]:
    os.environ[f"MAIL_{r.upper()}"] = f"{r}" + AT + "x.test"
os.environ.update(SMTP_USER="hub" + AT + "x.test", SMTP_APP_PASSWORD="x")
ok = []


def run(result, flow, env):
    if result is None:
        os.environ.pop("RESULT", None)
    else:
        os.environ["RESULT"] = result
    sys.argv = ["notify.py", "--flow", flow, "--event", "deployed", "--env", env, "--subject", "s", "--body", "b"]
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        runpy.run_path("ci/notify.py", run_name="__main__")
    return sent[-1]["To"], buf.getvalue()


def check(n, c, d=""):
    ok.append(c)
    print(("PASS " if c else "FAIL ") + n + ("" if c else f" -> {d}"))


for flow in ("promotion", "hotfix"):
    for env in ("test", "prod"):
        to, out = run("failure", flow, env)
        check(
            f"{flow} {env} failure: no Business Manager",
            "bizmanager" + AT + "x.test" not in to and "management" + AT + "x.test" in to,
            to,
        )
        check(f"{flow} {env} failure: no address printed", "@" not in out, out)
to, _ = run("success", "promotion", "prod")
check("promotion prod success: Business Manager still told", "bizmanager" + AT + "x.test" in to, to)
to, _ = run(None, "promotion", "prod")
check("no RESULT = success list (other callers unchanged)", "bizmanager" + AT + "x.test" in to, to)
to, _ = run("cancelled", "promotion", "test")
check("cancelled counts as failure list", "management" + AT + "x.test" in to, to)
print(f"notify tests: {sum(ok)} of {len(ok)} ->", "PASS" if all(ok) else "FAIL")
