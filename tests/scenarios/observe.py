"""Scenario test, run by tests/test_scenarios.py in its own process: python <this file> <repo copy> <work dir>.
Prints PASS/FAIL per step and a last line "... -> PASS" or "-> FAIL". Synthetic values only; mail addresses are
built at run time (AT) so no address-shaped text is ever committed."""

AT = chr(64)
import sys
import json
import datetime as dt

REPO = sys.argv[1]
sys.path.insert(0, REPO + "/src")
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.legacy.createHiveTableByDefault", "false")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
from lh import observe, bi

code = (
    "bhg"
    if "bio" in open(REPO + "/config/company.yml").read() or "bhg" in open(REPO + "/config/company.yml").read()
    else "fqf"
)
code = __import__("yaml").safe_load(open(REPO + "/config/company.yml"))["code"]
cat = "spark_catalog"
cfg = {"catalog": cat, "code": code, "env": "dev"}
for s in ["bronze", "silver", "ops", "gold"]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {s}")
now = dt.datetime(2026, 10, 22, 4, 0)
first = "patients" if code == "bhg" else "loans"
planned = "preferred_language" if code == "bhg" else "channel"
rows = [
    ("a", json.dumps({planned: "x", "_file_path": "p"}), now),
    ("b", json.dumps({"blood_group": "O+", "_file_path": "p"}), now + dt.timedelta(hours=1)),
    ("c", None, now),
    ("d", json.dumps({planned: "y"}), now - dt.timedelta(days=2)),
]
spark.createDataFrame(rows, "id string,_rescued_data string,_ingest_ts timestamp").write.mode("overwrite").saveAsTable(
    f"bronze.{first}"
)
other = "doctors" if code == "bhg" else "customers"
spark.createDataFrame([("z", None, now)], "id string,_rescued_data string,_ingest_ts timestamp").write.mode(
    "overwrite"
).saveAsTable(f"bronze.{other}")
if code == "fqf":
    spark.createDataFrame([("p", b"x")], "path string,content binary").write.mode("overwrite").saveAsTable(
        "bronze.bank_settlements"
    )
n = observe.rescued_keys(spark, cfg)
out = {(r.table_name, r.key): (r.rows, r.planned, str(r.first_seen)) for r in spark.table("ops.rescued_keys").collect()}
print(out)
assert (
    n == 2
    and out[(first, planned)][0] == 2
    and out[(first, planned)][1] is True
    and out[(first, "blood_group")][1] is False
)
assert not any(k == "_file_path" for _, k in out)
assert "_rescued_data" not in spark.table("ops.rescued_keys").columns
# rerun = same snapshot (overwrite)
assert observe.rescued_keys(spark, cfg) == 2
# no bronze rescued at all -> empty table, still created
spark.sql(f"DROP TABLE bronze.{first}")
assert observe.rescued_keys(spark, cfg) == 0
# views
spark.createDataFrame(
    [
        ("t1", "k1", "missing_key", None, None, None, dt.datetime(2026, 10, 21, 20, 0), now, "open"),
        ("t1", "k2", "missing_key", None, None, None, dt.datetime(2026, 10, 21, 17, 0), now, "open"),
    ],
    "table_name string,row_key string,rule string,detail string,raw_row string,source_file string,first_seen timestamp,last_seen timestamp,status string",
).write.mode("overwrite").saveAsTable("silver.quarantine")
for v in ["v_health_quarantine_daily", "v_health_rescued"]:
    sql = bi.HEALTH[v].format(cat=cat)
    spark.sql(f"CREATE OR REPLACE VIEW gold.{v} AS {sql}")
d = {str(r.day): r.rows for r in spark.table("gold.v_health_quarantine_daily").collect()}
print(d)
assert d == {"2026-10-22": 1, "2026-10-21": 1}  # 20:00 UTC = 01:30 IST next day
assert spark.table("gold.v_health_rescued").count() == 0
assert set(bi.wanted(cfg)) >= {"v_health_quarantine_daily", "v_health_rescued"}
print(code, "OBSERVE TESTS PASS")
