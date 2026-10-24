"""ci/chain.py: the daily job runs again when the source files came after it (GitHub started the simulator late)."""

import sys
import types
from datetime import datetime, timedelta

import pytest

from tests.conftest import ROOT

sys.path.insert(0, str(ROOT / "ci"))
import chain  # noqa: E402

IST = chain.IST
DAY = datetime(2026, 10, 24, tzinfo=IST)


class Clock:
    def __init__(self, t):
        self.t = t

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += timedelta(seconds=s)


class Jobs:
    """Fake Jobs API. runs: {job name: [(start, end or None)]}; a started run lasts `minutes`."""

    def __init__(self, clock, runs, minutes=10, fail=()):
        self.clock, self.runs, self.minutes, self.fail, self.started = clock, runs, minutes, fail, []

    def list(self, name):
        return [types.SimpleNamespace(job_id=name)] if name in self.runs else []

    def list_runs(self, job_id, start_time_from=None, active_only=False):
        out = []
        for start, end in self.runs[job_id]:
            running = (end is None or end > self.clock()) and start <= self.clock()
            recent = start_time_from is None or chain.ms(start) >= start_time_from
            if (active_only and running) or (not active_only and recent):
                out.append(types.SimpleNamespace(start_time=chain.ms(start)))
        return out

    def run_now(self, job_id):
        start = self.clock()
        self.started.append((job_id, start))
        self.runs[job_id].append((start, start + timedelta(minutes=self.minutes)))
        clock, fail, minutes = self.clock, self.fail, self.minutes

        def result(timeout):
            clock.t += timedelta(minutes=minutes)
            if job_id in fail:
                raise RuntimeError("failed")

        return types.SimpleNamespace(result=result)


def at(h, m=0):
    return DAY.replace(hour=h, minute=m)


def make(now, runs, **kw):
    clock = Clock(now)
    jobs = Jobs(clock, runs, **kw)
    return chain.Chain(types.SimpleNamespace(jobs=jobs), "prod", now=clock, sleep=clock.sleep), jobs


def test_files_on_time_nothing_is_started():
    c, jobs = make(at(0, 5), {"lh_fqf_prod_daily": [], "lh_bhg_prod_daily": []})
    assert c.go("fqf", since=at(0, 1)) and jobs.started == []


def test_late_fqf_files_run_fqf_again_then_bhg():  # the night of 24 Oct 2026
    runs = {"lh_fqf_prod_daily": [(at(3), at(3, 7))], "lh_bhg_prod_daily": []}
    c, jobs = make(at(3, 6), runs)
    assert c.go("fqf", since=at(3, 5))
    assert [j for j, _ in jobs.started] == ["lh_fqf_prod_daily"]  # BHG has not run yet: its 04:00 schedule follows


def test_late_fqf_files_after_bhg_also_rerun_bhg_after_fqf():
    runs = {"lh_fqf_prod_daily": [(at(3), at(3, 7))], "lh_bhg_prod_daily": [(at(4), at(4, 17))]}
    c, jobs = make(at(4, 3), runs)
    assert c.go("fqf", since=at(4, 2))
    names = [j for j, _ in jobs.started]
    assert names == ["lh_fqf_prod_daily", "lh_bhg_prod_daily"]
    assert jobs.started[1][1] >= at(4, 17)  # BHG waited for its own running run to finish


def test_a_run_started_after_the_files_covers_them():
    runs = {"lh_fqf_prod_daily": [(at(3), at(3, 7))], "lh_bhg_prod_daily": [(at(1), at(1, 10)), (at(4), at(4, 17))]}
    c, jobs = make(at(4, 20), runs)
    assert c.go("bhg", since=at(3, 59)) and jobs.started == []


def test_late_bhg_files_wait_for_fqf_first():
    runs = {"lh_fqf_prod_daily": [(at(4, 30), at(4, 50))], "lh_bhg_prod_daily": [(at(4), at(4, 17))]}
    c, jobs = make(at(4, 35), runs)
    assert c.go("bhg", since=at(4, 34))
    assert jobs.started[0][0] == "lh_bhg_prod_daily" and jobs.started[0][1] >= at(4, 50)


def test_a_failed_chained_run_fails_the_step():
    runs = {"lh_fqf_prod_daily": [(at(3), at(3, 7))], "lh_bhg_prod_daily": [(at(4), at(4, 17))]}
    c, jobs = make(at(5), runs, fail={"lh_fqf_prod_daily"})
    assert not c.go("fqf", since=at(4, 59))
    assert [j for j, _ in jobs.started] == ["lh_fqf_prod_daily"]  # BHG is not run on top of a failed FQF run


def test_missing_job_stops_with_a_clear_message():
    c, _ = make(at(5), {"lh_fqf_prod_daily": [(at(3), at(3, 7))]})
    with pytest.raises(SystemExit, match="lh_bhg_prod_daily"):
        c.go("bhg", since=at(4))
