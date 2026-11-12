"""ci/watchdog.py (28a): a failed scheduled alerts/notices run is mailed once to the fixers, one reminder per IST day
while it stays down, "monitoring back" after it recovers; manual runs and cancelled runs send nothing."""

import io
import json
import runpy
import urllib.error
from datetime import datetime, UTC

import pytest
import yaml

from tests.conftest import ROOT

AT = chr(64)  # addresses are built at run time, so no address is ever written in a committed file (ci/checks.py)

WD = runpy.run_path(str(ROOT / "ci" / "watchdog.py"), run_name="watchdog")
decide, FAILED = WD["decide"], WD["FAILED"]
NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)  # 17:30 IST


def run(i, conclusion, created):
    return {"id": i, "conclusion": conclusion, "created_at": created}


@pytest.mark.parametrize(
    ("result", "runs", "expected"),
    [
        ("failure", [run(9, "failure", "2026-11-02T11:41:00Z"), run(8, "success", "2026-11-02T09:41:00Z")], "stopped"),
        ("failure", [run(9, "failure", "2026-11-02T11:41:00Z")], "stopped"),  # first run ever
        ("failure", [run(9, "failure", "2026-11-02T11:41:00Z"), run(8, "failure", "2026-11-02T09:41:00Z")], None),
        # still down, but the earlier failures were yesterday (IST): one reminder today
        ("failure", [run(9, "failure", "2026-11-02T11:41:00Z"), run(8, "failure", "2026-11-01T17:41:00Z")], "still"),
        # 18:15 UTC on 1 Nov is 23:45 IST on 1 Nov, so it counts as yesterday
        ("failure", [run(9, "failure", "2026-11-02T11:41:00Z"), run(8, "timed_out", "2026-11-01T18:15:00Z")], "still"),
        ("success", [run(9, "success", "2026-11-02T11:41:00Z"), run(8, "failure", "2026-11-02T09:41:00Z")], "back"),
        ("success", [run(9, "success", "2026-11-02T11:41:00Z"), run(8, "success", "2026-11-02T09:41:00Z")], None),
        ("success", [], None),
        # a cancelled run in between is ignored: the last real result was a failure
        (
            "success",
            [
                run(9, "success", "2026-11-02T11:41:00Z"),
                run(8, "cancelled", "2026-11-02T10:41:00Z"),
                run(7, "failure", "2026-11-02T09:41:00Z"),
            ],
            "back",
        ),
    ],
)
def test_decide(result, runs, expected):
    assert decide(result, 9, runs, NOW) == expected


def test_failed_conclusions():
    assert {"failure", "timed_out", "startup_failure"} == FAILED


def test_recipients_are_fixers_only():
    roles = yaml.safe_load((ROOT / "config" / "ci.yml").read_text())["watchdog"]
    assert set(roles) == {"developer", "teamlead", "manager", "management"}
    assert "bizmanager" not in roles


@pytest.mark.parametrize("wf", ["alerts", "notices"])
def test_workflow_runs_the_watchdog(wf):
    text = (ROOT / ".github" / "workflows" / f"{wf}.yml").read_text()
    flow = yaml.safe_load(text)
    assert flow["permissions"] == {"contents": "read", "actions": "read"}
    steps = flow["jobs"][wf]["steps"]
    last = steps[-1]
    want = {
        "alerts": "always() && (github.event_name == 'schedule' || inputs.timed)",  # 29a: VPS timer runs count too
        "notices": "always() && github.event_name == 'schedule'",
    }
    assert last["if"] == want[wf]
    assert f"ci/watchdog.py --workflow {wf}" in last["run"]
    assert last["env"]["RESULT"] == "${{ job.status }}"
    assert steps[-2].get("timeout-minutes") == 12  # the main step fails on a hang, the job does not time out
    assert "MAIL_BIZMANAGER" not in last["env"]


class FakeSMTP:
    sent = []

    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def login(self, user, password):
        pass

    def send_message(self, msg):
        FakeSMTP.sent.append(msg)


def main(monkeypatch, result, answer, event="schedule", timed="", dispatched=()):
    """Run the script as GitHub would. answer: the scheduled runs GitHub returns, or an HTTP status code;
    dispatched: the workflow_dispatch runs (29a: timer runs and manual runs)."""
    FakeSMTP.sent = []

    def urlopen(req, timeout):
        if isinstance(answer, int):
            raise urllib.error.HTTPError(req.full_url, answer, "x", {}, None)
        runs = list(dispatched) if "event=workflow_dispatch" in req.full_url else answer
        return io.BytesIO(json.dumps({"workflow_runs": runs}).encode())

    monkeypatch.chdir(ROOT)
    monkeypatch.setattr("urllib.request.urlopen", urlopen)
    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr("smtplib.SMTP_SSL", FakeSMTP)
    env = {
        "GITHUB_EVENT_NAME": event,
        "GITHUB_TOKEN": "t",
        "GITHUB_REPOSITORY": "o/r",
        "GITHUB_RUN_ID": "9",
        "GITHUB_SERVER_URL": "https://github.com",
        "SMTP_USER": "sender" + AT + "x.test",
        "SMTP_APP_PASSWORD": "x",
        "MAIL_DEVELOPER": "dev" + AT + "x.test",
        "MAIL_TEAMLEAD": "tl" + AT + "x.test",
        "MAIL_MANAGER": "mgr" + AT + "x.test",
        "MAIL_MANAGEMENT": "mgmt" + AT + "x.test",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("TIMED", timed)
    monkeypatch.setattr("sys.argv", ["watchdog.py", "--workflow", "alerts", "--env", "prod", "--result", result])
    runpy.run_path(str(ROOT / "ci" / "watchdog.py"), run_name="__main__")
    return FakeSMTP.sent


def test_first_failure_mails_the_fixers(monkeypatch):
    sent = main(monkeypatch, "failure", [{"id": 8, "conclusion": "success", "created_at": "2026-11-02T09:41:00Z"}])
    assert len(sent) == 1
    msg = sent[0]
    assert "Monitoring stopped: alerts could not run" in msg["Subject"]
    assert msg["To"] == ", ".join(r + AT + "x.test" for r in ["dev", "mgmt", "mgr", "tl"])
    body = msg.get_body(("plain",)).get_content()
    assert "Sign in to Databricks" in body and "https://github.com/o/r/actions/runs/9" in body
    html = msg.get_body(("html",)).get_content()
    assert "Monitoring stopped" in html and "#c0392b" in html  # 28d: red HTML mail


def test_github_unreachable_still_mails_a_failure(monkeypatch):
    assert len(main(monkeypatch, "failure", 503)) == 1


def test_github_unreachable_good_run_sends_nothing(monkeypatch):
    assert main(monkeypatch, "success", 503) == []


def test_recovery_mail(monkeypatch):
    sent = main(monkeypatch, "success", [{"id": 8, "conclusion": "failure", "created_at": "2026-11-02T09:41:00Z"}])
    assert len(sent) == 1 and "Monitoring back" in sent[0]["Subject"]
    assert "#1f8a4c" in sent[0].get_body(("html",)).get_content()  # 28d: green


@pytest.mark.parametrize(("event", "result"), [("workflow_dispatch", "failure"), ("schedule", "cancelled")])
def test_manual_or_cancelled_sends_nothing(monkeypatch, event, result):
    assert main(monkeypatch, result, [], event=event) == []


# 29a: the VPS timer lh-tick starts alerts with workflow_dispatch + timed=true (run name "alerts timer <env>")
def timer_run(i, conclusion, created, title="alerts timer prod"):
    return {"id": i, "conclusion": conclusion, "created_at": created, "display_title": title}


def test_timer_run_failure_mails_like_a_scheduled_one(monkeypatch):
    earlier = [timer_run(8, "success", "2026-11-02T09:23:00Z")]
    sent = main(monkeypatch, "failure", [], event="workflow_dispatch", timed="yes", dispatched=earlier)
    assert len(sent) == 1 and "Monitoring stopped" in sent[0]["Subject"]


def test_timer_and_scheduled_runs_form_one_history(monkeypatch):
    # newest earlier run is the failed timer run (09:23), the older scheduled one was fine: this success is "back"
    sent = main(
        monkeypatch,
        "success",
        [run(7, "success", "2026-11-02T07:23:00Z")],
        dispatched=[timer_run(8, "failure", "2026-11-02T09:23:00Z")],
    )
    assert len(sent) == 1 and "Monitoring back" in sent[0]["Subject"]


def test_manual_runs_are_not_part_of_the_history(monkeypatch):
    # a manual dry run that failed (not a timer run) must not make the next good scheduled run send "back"
    manual = [timer_run(8, "failure", "2026-11-02T09:23:00Z", title="alerts")]
    assert main(monkeypatch, "success", [run(7, "success", "2026-11-02T07:23:00Z")], dispatched=manual) == []


def test_timed_flag_only_from_the_timer(monkeypatch):
    # a manual run without TIMED=yes stays silent even when it fails
    assert main(monkeypatch, "failure", [], event="workflow_dispatch", timed="no") == []


def test_alerts_workflow_takes_timer_runs():
    flow = yaml.safe_load((ROOT / ".github" / "workflows" / "alerts.yml").read_text())
    assert flow["run-name"] == "${{ inputs.timed && format('alerts timer {0}', inputs.env) || 'alerts' }}"
    timed = flow[True]["workflow_dispatch"]["inputs"]["timed"]  # yaml reads the key "on" as True
    assert timed["type"] == "boolean" and timed["default"] is False
    env = flow["jobs"]["alerts"]["steps"][-2]["env"]
    assert "!inputs.timed" in env["MODE"] and "inputs.timed" in env["SCHEDULED"]
    assert flow["jobs"]["alerts"]["steps"][-1]["env"]["TIMED"] == "${{ inputs.timed && 'yes' || 'no' }}"


def test_stream_schedule_is_only_a_fallback():
    path = ROOT / ".github" / "workflows" / "stream.yml"
    if not path.exists():
        pytest.skip("stream.yml exists in FQF only")
    flow = yaml.safe_load(path.read_text())
    assert flow["run-name"] == "${{ inputs.timed && 'stream timer' || 'stream' }}"
    assert flow["permissions"] == {"contents": "read", "actions": "read"}
    gate, produce = flow["jobs"]["gate"], flow["jobs"]["produce"]
    assert gate["if"] == "github.event_name == 'schedule'"
    assert '"stream timer"' in gate["steps"][0]["run"] and "125 minutes ago" in gate["steps"][0]["run"]
    assert produce["needs"] == "gate" and "needs.gate.outputs.skip != 'yes'" in produce["if"]
    assert "inputs.timed" in produce["strategy"]["matrix"]["env"]
    step = produce["steps"][-1]["env"]
    assert "!inputs.timed" in step["DRY"] and "inputs.timed" in step["SCHEDULED"]
