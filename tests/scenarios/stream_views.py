"""Scenario test, run by tests/test_scenarios.py in its own process: python <this file> <repo copy> <work dir>.
Prints PASS/FAIL per step and a last line "... -> PASS" or "-> FAIL". Synthetic values only; mail addresses are
built at run time (AT) so no address-shaped text is ever committed."""

AT = chr(64)
import sys
import json
import re
import datetime as dt
from decimal import Decimal as D

REPO = sys.argv[1]
sys.path.insert(0, REPO + "/src")
import yaml
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.warehouse.dir", sys.argv[2] + "/wh")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
from lh import bi, privacy

ok = []


def check(n, c, d=""):
    ok.append(bool(c))
    print(("PASS " if c else "FAIL ") + n + ("" if c else f" -> {d}"))


for db in ("silver", "gold"):
    spark.sql(f"DROP DATABASE IF EXISTS {db} CASCADE")
    spark.sql(f"CREATE DATABASE {db}")
cfg = {"code": "fqf", "catalog": "spark_catalog"}
# minimal other sources so every fqf view can be created
for t, ddl in {
    "gold.fact_fqf_event": "date_key int, product_key bigint, branch_key bigint, loan_key bigint, event_group string, event_type string, is_hospital_bill boolean, amount decimal(18,2), principal_part decimal(18,2), interest_part decimal(18,2), _gold_ts timestamp",
    "gold.dim_date": "date_key int, date date, year_month string, fiscal_year string, fiscal_quarter string",
    "gold.dim_product": "product_key bigint, product_name string, loan_type string",
    "gold.dim_branch": "branch_key bigint, branch_name string, city string",
    "gold.dim_loan": "loan_key bigint, status string, dpd_bucket string, score_band string, is_hospital_bill boolean, principal decimal(18,2)",
}.items():
    spark.sql(f"CREATE TABLE {t} ({ddl}) USING parquet")
for v in ("v_fqf_payments_live", "v_fqf_payment_alerts"):
    sql = (
        bi.EMPTY_UNTIL[v][1]
        if not spark.catalog.tableExists("spark_catalog." + bi.EMPTY_UNTIL[v][0])
        else bi.wanted(cfg)[v]
    )
    spark.sql(f"CREATE OR REPLACE VIEW gold.{v} AS {sql.format(cat='spark_catalog')}")
check(
    "empty fallback views build, 0 rows",
    spark.table("gold.v_fqf_payments_live").count() == 0 and spark.table("gold.v_fqf_payment_alerts").count() == 0,
)
empty_cols = {
    v: [(f.name, f.dataType.simpleString()) for f in spark.table(f"gold.{v}").schema]
    for v in ("v_fqf_payments_live", "v_fqf_payment_alerts")
}
# stream tables as stream.py writes them
ts = lambda s: dt.datetime.fromisoformat(s)
spark.createDataFrame(
    [
        ("E1", ts("2026-10-23 14:01:00"), "SC1", "LN1", "failed", "upi", "insufficient_funds", D("100.00")),
        ("E2", ts("2026-10-23 14:02:00"), "SC1", "LN1", "failed", "upi", "insufficient_funds", D("100.00")),
        ("E3", ts("2026-10-23 14:03:00"), "SC1", "LN1", "failed", "card", "bank_declined", D("50.00")),
        ("E4", ts("2026-10-23 18:40:00"), "SC2", "LN2", "success", "netbanking", None, D("500.00")),
    ],
    "event_id string, event_ts timestamp, customer_ref string, loan_ref string, status string, method string, reason string, amount decimal(18,2)",
).write.saveAsTable("silver.payment_events")
spark.createDataFrame(
    [
        (ts("2026-10-23 14:00:00"), ts("2026-10-23 14:10:00"), "SC1", 3, D("250.00"), True),
        (ts("2026-10-23 18:40:00"), ts("2026-10-23 18:50:00"), "SC9", 1, D("10.00"), False),
    ],
    "window_start timestamp, window_end timestamp, customer_ref string, failed_payments int, failed_amount decimal(18,2), is_alert boolean",
).write.saveAsTable("gold.payment_failure_windows")
for v in ("v_fqf_payments_live", "v_fqf_payment_alerts"):
    spark.sql(f"CREATE OR REPLACE VIEW gold.{v} AS {bi.wanted(cfg)[v].format(cat='spark_catalog')}")
live = spark.table("gold.v_fqf_payments_live")
check("live view: events total = silver rows", live.agg({"events": "sum"}).first()[0] == 4)
r = live.filter("status = 'success'").first()
check("live view: 18:40 UTC -> 24 Oct 00 IST", str(r["day"]) == "2026-10-24" and r["hour_ist"] == 0, r)
check(
    "live view: failed 14:0x UTC -> 19 IST",
    live.filter("status='failed' AND hour_ist = 19").agg({"events": "sum"}).first()[0] == 3,
)
check("live view: no customer column", "customer_ref" not in live.columns and "loan_ref" not in live.columns)
al = spark.table("gold.v_fqf_payment_alerts").collect()
check(
    "alerts view: only is_alert rows, IST start",
    len(al) == 1 and al[0]["window_start_ist"] == ts("2026-10-23 19:30:00"),
    al,
)
filled_cols = {v: [(f.name, f.dataType.simpleString()) for f in spark.table(f"gold.{v}").schema] for v in empty_cols}
check(
    "empty fallback has the same column names",
    all([c for c, _ in empty_cols[v]] == [c for c, _ in filled_cols[v]] for v in empty_cols),
    (empty_cols, filled_cols),
)
# dashboard datasets run against the views
d = json.load(open(REPO + "/bi/fqf_lending.lvdash.json"))
names = {"pay_day", "pay_7", "alerts_7"}
spark.sql("CREATE OR REPLACE TEMP VIEW _x AS SELECT 1")
for ds in d["datasets"]:
    if ds["name"] not in names:
        continue
    q = "".join(ds["queryLines"]).replace("fqf_dev.gold.", "gold.").replace("current_date()", "DATE'2026-10-24'")
    rows = spark.sql(q).collect()
    check(f"dataset {ds['name']} runs ({len(rows)} rows)", len(rows) > 0, q)
p7 = {
    r["method"]: r
    for r in spark.sql(
        "".join([l for l in next(x for x in d["datasets"] if x["name"] == "pay_7")["queryLines"]])
        .replace("fqf_dev.gold.", "gold.")
        .replace("current_date()", "DATE'2026-10-24'")
    ).collect()
}
check("pay_7: failed upi = 2, amount 200", p7["upi"]["failed"] == 2 and p7["upi"]["failed_amount"] == D("200.00"), p7)
# every widget field references a column its dataset returns
cols = {
    ds["name"]: set(
        spark.sql(
            "".join(ds["queryLines"]).replace("fqf_dev.gold.", "gold.").replace("current_date()", "DATE'2026-10-24'")
        ).columns
    )
    for ds in d["datasets"]
    if ds["name"] in names
}
bad = []
for w in d["pages"][1]["layout"]:
    for qq in w["widget"].get("queries", []):
        for f in qq["query"]["fields"]:
            for c in re.findall(r"`([^`]+)`", f["expression"]):
                if c not in cols[qq["query"]["datasetName"]]:
                    bad.append((w["widget"]["name"], c))
    enc = w["widget"].get("spec", {}).get("encodings", {})
    fn = {
        qq["query"]["fields"][i]["name"]
        for qq in w["widget"].get("queries", [])
        for i in range(len(qq["query"]["fields"]))
    }
    for k, e in enc.items():
        if isinstance(e, dict) and e.get("fieldName") not in fn:
            bad.append((w["widget"]["name"], k))
check("every widget field exists in its dataset and query", not bad, bad)
# privacy plan
p = yaml.safe_load(open(REPO + "/config/privacy.yml"))
silver_cols = {
    "payment_events": {"event_id": "string", "customer_ref": "string", "loan_ref": "string"},
    "customers": {c: "string" for c in ["aadhaar", "full_name", "phone", "dob", "monthly_income", "id_fingerprint"]},
    "disbursements": {"bank_ref": "string"},
    "bank_settlements": {"bank_ref": "string"},
    "reconciliation": {"ref": "string"},
}
for env in ("dev", "test", "prod"):
    sp = privacy.plan(p, env, "silver", silver_cols, {})
    check(
        f"{env}: silver.payment_events gets no mask (stream source)",
        not [x for x in sp if x[0] == "payment_events"],
        sp,
    )
    bp = privacy.plan(p, env, "bronze", {"payment_events": {"value": "string", "topic": "string"}}, {})
    check(f"{env}: bronze.payment_events gets no mask (stream source)", bp == [], bp)
    gp = privacy.plan(
        p, env, "gold", {"payment_failure_windows": {"customer_ref": "string", "window_start": "timestamp"}}, {}
    )
    check(f"{env}: gold windows customer_ref = token", gp == [("payment_failure_windows", "customer_ref", "token")], gp)
print(f"\nstream BI tests: {sum(ok)} of {len(ok)} ->", "PASS" if all(ok) else "FAIL")
