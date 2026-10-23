"""Scenario test, run by tests/test_scenarios.py in its own process: python <this file> <repo copy> <work dir>.
Data dictionary on local Spark: comments set, rerun changes nothing, missing and undocumented reported, view comment.
Prints PASS/FAIL per step and a last line "... -> PASS" or "-> FAIL". Synthetic values only."""

import sys

import yaml

REPO = sys.argv[1]
sys.path.insert(0, REPO + "/src")
from pyspark.sql import SparkSession  # noqa: E402

spark = (
    SparkSession.builder.master("local[2]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.legacy.createHiveTableByDefault", "false")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
from lh import dictionary  # noqa: E402

results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(("PASS " if ok else "FAIL ") + name + (f" -> {detail}" if not ok else ""))


code = yaml.safe_load(open(REPO + "/config/company.yml"))["code"]
cfg = {"catalog": "spark_catalog", "code": code}
d = dictionary.load()
for s in ("silver", "gold"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {s}")

s_name = next(iter(d["silver"]))
g_name = "dim_date"
spark.sql(f"CREATE TABLE silver.{s_name} (id STRING) USING parquet")
cols = ", ".join(f"`{c}` STRING" for c in d["gold"][g_name]["columns"])
spark.sql(f"CREATE TABLE gold.{g_name} ({cols}, extra_col STRING, _gold_ts TIMESTAMP) USING parquet")
small = {"silver": {s_name: d["silver"][s_name]}, "gold": {g_name: d["gold"][g_name]}, "views": d["views"]}
n_cols = len(d["gold"][g_name]["columns"])

out = dictionary.apply(spark, cfg, small)
print(out)
check("first run sets 2 table and every column description", out["tables"] == 2 and out["columns"] == n_cols)
check(
    "table description readable",
    spark.catalog.getTable(f"spark_catalog.gold.{g_name}").description == d["gold"][g_name]["about"],
)
got = {c.name: c.description for c in spark.catalog.listColumns(f"spark_catalog.gold.{g_name}")}
check("column descriptions readable", all(got[c] == t for c, t in d["gold"][g_name]["columns"].items()))
check(
    "an undocumented Gold column is reported, technical columns are not",
    out["undocumented"] == [f"{g_name}.extra_col"],
    out["undocumented"],
)
check("nothing failed", out["failed"] == [], out["failed"])

again = dictionary.apply(spark, cfg, small)
check(
    "second run changes nothing",
    again["tables"] == 0 and again["columns"] == 0 and again["unchanged"] == 2 + n_cols,
    again,
)

missing = dictionary.apply(spark, cfg, {"gold": {"not_built_yet": {"about": "A table that does not exist yet"}}})
check(
    "a table that is not built yet is reported, not an error",
    missing["missing"] == ["spark_catalog.gold.not_built_yet"],
)

from lh import gold  # noqa: E402

row = spark.createDataFrame([tuple("x" for _ in d["gold"][g_name]["columns"])], cols.replace("`", ""))
gold.save(spark, cfg, g_name, row)  # full overwrite, as every daily run does
got = {c.name: c.description for c in spark.catalog.listColumns(f"spark_catalog.gold.{g_name}")}
check(
    "Gold save writes the column descriptions with the data",
    all(got[c] == t for c, t in d["gold"][g_name]["columns"].items()),
    got,
)
after = dictionary.apply(spark, cfg, small)
check("after a full Gold rebuild no column description needs setting", after["columns"] == 0, after)

view = next(iter(d["views"]))
about = dictionary.view_comment(view, d)
spark.sql(f"CREATE OR REPLACE VIEW gold.{view} COMMENT '{about}' AS SELECT 1 AS x")
check("BI view gets its description", spark.catalog.getTable(f"spark_catalog.gold.{view}").description == about)
check("unknown view has no description", dictionary.view_comment("v_unknown", d) is None)

print(f"dictionary scenario: {sum(results)} of {len(results)} ->", "PASS" if all(results) else "FAIL")
sys.exit(0 if all(results) else 1)
