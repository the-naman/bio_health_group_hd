"""Write source files for one company and one environment.
 
  python -m simulator.run --company bhg --env dev --out ./out          # local folder (test)
  python -m simulator.run --company bhg --env test --s3                 # S3 (GitHub Actions)
 
Without --upto it writes everything still missing up to yesterday (IST):
first run = full load (monthly files up to 4 Oct 2026, even if run earlier) + catch-up daily files;
later runs = new days only. A file name is never written twice.
The last written business date is kept as an empty marker object under landing/_sim/state/
(the simulator key may only Put and List under landing/, never Get).
"""
import argparse, calendar, os, random
from datetime import date, datetime, timedelta, timezone
 
from simulator import mess
from simulator.formats import render
from simulator.world import World, TABLES, CUTOVER, BHG_START, FQF_START, source_of, ext_of
 
IST = timezone(timedelta(hours=5, minutes=30))
STATE_PREFIX = "landing/_sim/state/"   # empty marker objects: landing/_sim/state/<yyyy-mm-dd>
 
 
class Sink:
    def __init__(self, out=None, bucket=None):
        self.out, self.bucket, self.s3 = out, bucket, None
        if bucket:
            import boto3
            self.s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
 
    def put(self, key, body):
        if self.s3:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=body.encode("utf-8"))
        else:
            path = os.path.join(self.out, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8", newline="") as f:
                f.write(body)
 
    def last_date(self):
        if self.s3:
            resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=STATE_PREFIX)
            names = [o["Key"][len(STATE_PREFIX):] for o in resp.get("Contents", [])]
        else:
            path = os.path.join(self.out, STATE_PREFIX)
            names = os.listdir(path) if os.path.isdir(path) else []
        return max((date.fromisoformat(n) for n in names), default=None)
 
 
def key_for(table, file_date, nn):
    ymd = file_date.strftime("%Y%m%d")
    return f"landing/{source_of(table)}/{table}/{file_date:%Y/%m}/{table}_{ymd}_{nn:02d}.{ext_of(table)}"
 
 
def incidents(env, company, d):
    """test/prod only: per month one unreadable file + one duplicate re-send; per quarter one delayed day."""
    if env == "dev":
        return {}
    r = random.Random(f"inc-{env}-{company}-{d:%Y%m}")
    days = calendar.monthrange(d.year, d.month)[1]
    bad_day, dup_day = r.randint(1, days), r.randint(1, days)
    bad_t, dup_t = r.choice(TABLES[company]), r.choice(TABLES[company])
    out = {}
    if d.day == bad_day:
        out["unreadable"] = bad_t
    if d.day == dup_day:
        out["duplicate"] = dup_t
    return out
 
 
def delayed_day(env, company, d):
    if env == "dev":
        return False
    q = (d.month - 1) // 3
    r = random.Random(f"delay-{env}-{company}-{d.year}-{q}")
    start = date(d.year, q * 3 + 1, 1)
    return d == start + timedelta(days=r.randint(5, 80))
 
 
def write_full_load(world, company, env, sink):
    first = BHG_START if company == "bhg" else FQF_START
    count = 0
    for table in TABLES[company]:
        by_month = {}
        for row in world.rows[table]:
            if row["_emit"] <= CUTOVER:
                by_month.setdefault((row["_emit"].year, row["_emit"].month), []).append(row)
        y, m = first.year, first.month
        while (y, m) <= (CUTOVER.year, CUTOVER.month):
            fd = min(date(y, m, calendar.monthrange(y, m)[1]), CUTOVER)
            rows = mess.apply(env, table, fd, by_month.get((y, m), []))
            sink.put(key_for(table, fd, 1), render(env, table, rows, fd))
            count += 1
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return count
 
 
def write_day(world, company, env, d, sink):
    inc = incidents(env, company, d)
    count = 0
    for table in TABLES[company]:
        rows = mess.apply(env, table, d, [x for x in world.rows[table] if x["_emit"] == d])
        body = render(env, table, rows, d)
        if inc.get("unreadable") == table:
            sink.put(key_for(table, d, 1), render(env, table, rows, d, broken=True))  # broken file
            sink.put(key_for(table, d, 2), body)                                  # corrected re-send
            count += 2
        elif inc.get("duplicate") == table:
            sink.put(key_for(table, d, 1), body)
            sink.put(key_for(table, d, 2), body)                                  # same content again
            count += 2
        else:
            sink.put(key_for(table, d, 1), body)
            count += 1
    return count
 
 
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--company", required=True, choices=["bhg", "fqf"])
    ap.add_argument("--env", required=True, choices=["dev", "test", "prod"])
    ap.add_argument("--out", help="local folder instead of S3")
    ap.add_argument("--s3", action="store_true", help="write to S3_BUCKET")
    ap.add_argument("--upto", help="last business date to write (yyyy-mm-dd); default = yesterday IST")
    a = ap.parse_args()
 
    sink = Sink(out=a.out, bucket=os.environ["S3_BUCKET"] if a.s3 else None)
    yesterday = (datetime.now(IST) - timedelta(days=1)).date()
    upto = date.fromisoformat(a.upto) if a.upto else yesterday
    last = sink.last_date()
 
    if last and last >= upto:
        print(f"{a.company}/{a.env}: nothing to do (last={last}, upto={upto})")
        return
    if delayed_day(a.env, a.company, upto) and upto > CUTOVER:
        upto -= timedelta(days=1)                       # planned missing day: sent with the next run
        print(f"{a.company}/{a.env}: planned delay, holding {upto + timedelta(days=1)}")
 
    upto = max(upto, CUTOVER)                           # the full load always covers history up to 4 Oct 2026
    world = World(a.env, upto).build()
    files = 0
    if last is None:
        files += write_full_load(world, a.company, a.env, sink)
        last = CUTOVER
        print(f"{a.company}/{a.env}: full load written ({files} files)")
    d = last + timedelta(days=1)
    while d <= upto:
        files += write_day(world, a.company, a.env, d, sink)
        d += timedelta(days=1)
    sink.put(STATE_PREFIX + str(upto), "")
    print(f"{a.company}/{a.env}: done, {files} files, last business date {upto}")
 
if __name__ == "__main__":
    main()
 