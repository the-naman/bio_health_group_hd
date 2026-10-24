"""Chain (same file in both repos): start the daily job again when the source files came after it already ran.

Run by .github/workflows/simulator.yml right after the simulator wrote new files for test or prod. GitHub may start the
scheduled simulator hours late (24 Oct 2026: 03:05 instead of 00:01), after the daily job had already run on an empty
landing folder. Then this script starts that job again, and for FQF also the BHG job (the group view reads FQF Gold).
When the job has not run yet today, nothing is started: its own schedule loads the files. A run that was started after
the files arrived (by the schedule or by the other company's chain) counts as done, so nothing runs twice.
Account: sp_lh_deploy (Can Manage on the test/prod jobs, no data access). Prints job names and results, never ids.
    python ci/chain.py --env prod --since 2026-10-24T21:35:00Z
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

IST = timezone(timedelta(hours=5, minutes=30))
WAIT_MINUTES = 120  # longest a daily run may take before the chain gives up waiting (job timeout is 150)


def ms(t):
    return int(t.timestamp() * 1000)


class Chain:
    def __init__(self, w, env, now=None, sleep=time.sleep, poll=30):
        self.w, self.env, self.sleep, self.poll = w, env, sleep, poll
        self.now = now or (lambda: datetime.now(IST))
        self.ids = {}

    def job(self, code):
        name = f"lh_{code}_{self.env}_daily"
        if name not in self.ids:
            found = [j.job_id for j in self.w.jobs.list(name=name)]
            if len(found) != 1:
                raise SystemExit(f"{name}: expected one job, found {len(found)}")
            self.ids[name] = found[0]
        return name, self.ids[name]

    def runs_since(self, code, since):
        _, jid = self.job(code)
        return list(self.w.jobs.list_runs(job_id=jid, start_time_from=ms(since)))

    def active(self, code):
        _, jid = self.job(code)
        return list(self.w.jobs.list_runs(job_id=jid, active_only=True))

    def wait_idle(self, code):
        name, _ = self.job(code)
        end = self.now() + timedelta(minutes=WAIT_MINUTES)
        while self.active(code):
            if self.now() > end:
                print(f"{name}: still running after {WAIT_MINUTES} min, going on")
                return
            print(f"{name}: running, waiting")
            self.sleep(self.poll)

    def run(self, code, since):
        """Start the job unless a run started at or after `since` already covers it. Returns True if it ran ok."""
        name, jid = self.job(code)
        if any((r.start_time or 0) >= ms(since) for r in self.runs_since(code, since)):
            print(f"{name}: a run started after the new files already covers them, nothing to start")
            return True
        print(f"{name}: starting a run for the new files")
        start = self.now()
        try:
            self.w.jobs.run_now(job_id=jid).result(timeout=timedelta(minutes=WAIT_MINUTES))
        except Exception as e:  # noqa: BLE001 - reported; Databricks mails the fixers of a failed task
            print(f"::error::{name}: the chained run did not succeed ({type(e).__name__})")
            return False
        print(f"{name}: chained run succeeded in {(self.now() - start).seconds // 60} min")
        return True

    def go(self, code, since=None):
        """since = when the simulator started writing the files (default: now)."""
        midnight = self.now().replace(hour=0, minute=0, second=0, microsecond=0)
        since = since or self.now()
        if not self.runs_since(code, midnight):
            print(f"lh_{code}_{self.env}_daily has not run today: its schedule loads the new files")
            return True
        ok = True
        if code == "bhg":
            self.wait_idle("fqf")  # the group view must read finished FQF Gold
        ok = self.run(code, since)
        if code == "fqf" and ok:
            after = self.now()
            if self.runs_since("bhg", midnight):  # BHG already merged without these FQF days: merge again
                self.wait_idle("bhg")
                ok = self.run("bhg", after)
        return ok


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--env", required=True, choices=["test", "prod"])
    p.add_argument("--since", help="UTC time the simulator started writing, e.g. 2026-10-24T03:05:00Z")
    a = p.parse_args()
    since = datetime.fromisoformat(a.since.replace("Z", "+00:00")).astimezone(IST) if a.since else None
    from databricks.sdk import WorkspaceClient

    code = yaml.safe_load(Path("config/company.yml").read_text())["code"]
    sys.exit(0 if Chain(WorkspaceClient(), a.env).go(code, since) else 1)


if __name__ == "__main__":
    main()
