"""Silver: cleaning rules as Spark column expressions (no Python UDFs, safe in ANSI mode).
 
Every cleaner takes a text column and returns a clean column; a value that cannot be
read becomes NULL, never an error. apply() runs the cleaners named in config/silver.yml
and adds `_bad`: the list of columns that had a value which could not be read.
 
Rules then decide what happens to a row (split_and_log):
  missing_key, unreadable_value (number, date, code), value_not_allowed -> row goes to silver.quarantine
  unreadable phone or national id                                       -> value stays NULL, row is kept (warning)
Every rule's result is written to ops.dq_results.
"""
import os
from functools import reduce
 
import yaml
from pyspark.sql import Window, functions as F
 
from lh import common
 
FAKE_NULLS = ["", "null", "n/a", "na", "none", "-"]
CITY_ALIASES = {
    "bangalore": "Bengaluru", "blr": "Bengaluru", "bengaluru": "Bengaluru",
    "bombay": "Mumbai", "mumbai": "Mumbai",
    "new delhi": "Delhi", "delhi": "Delhi",
    "madras": "Chennai", "chennai": "Chennai",
    "hyd": "Hyderabad", "hyderabad": "Hyderabad",
}
DAY, MON = r"(0[1-9]|[12]\d|3[01])", r"(0[1-9]|1[0-2])"
# (pattern the text must match, format used to read it). The pattern is checked first, so the
# parser only ever sees text of the right shape. Order matters for 03/04/2026: day first (India).
DATE_FORMATS = [(r"^\d{4}-" + MON + "-" + DAY + "$", "yyyy-MM-dd"),
                ("^" + DAY + "/" + MON + r"/\d{4}$", "dd/MM/yyyy"),
                ("^" + DAY + r"-[A-Za-z]{3}-\d{4}$", "dd-MMM-yyyy"),
                ("^" + DAY + "-" + MON + r"-\d{4}$", "dd-MM-yyyy"),
                ("^" + MON + "/" + DAY + r"/\d{4}$", "MM/dd/yyyy")]
CLOCK = r"\d{2}:\d{2}:\d{2}$"
TS_FORMATS = [(r"^\d{4}-\d{2}-\d{2} " + CLOCK, "yyyy-MM-dd HH:mm:ss"),
              (r"^\d{4}-\d{2}-\d{2}T" + CLOCK, "yyyy-MM-dd'T'HH:mm:ss"),
              (r"^\d{2}-\d{2}-\d{4} " + CLOCK, "dd-MM-yyyy HH:mm:ss"),
              (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd")]
META = ["_source_file", "_business_date", "_ingest_ts"]
HARD = {"int", "decimal", "date", "timestamp", "code"}      # an unreadable value of these kinds quarantines the row
 
 
def load_silver():
    with open(os.path.join(common.repo_root(), "config", "silver.yml")) as f:
        return yaml.safe_load(f)["tables"]
 
 
def text(c):
    """Trim, collapse spaces, turn fake nulls (NULL, N/A, -) into real NULL."""
    t = F.regexp_replace(F.trim(c), r"\s+", " ")
    return F.when(F.lower(t).isin(FAKE_NULLS), None).otherwise(t)
 
 
def name(c):
    return F.initcap(text(c))
 
 
def code(c):
    return F.lower(text(c))
 
 
def city(c):
    lookup = F.create_map(*[F.lit(x) for pair in CITY_ALIASES.items() for x in pair])
    t = text(c)
    return F.coalesce(F.try_element_at(lookup, F.lower(t)), F.initcap(t))
 
 
def phone(c):
    """10 digit Indian mobile number; +91, 91 and a leading 0 are removed."""
    d = F.regexp_replace(text(c), r"\D", "")
    d = F.when((F.length(d) == 12) & d.startswith("91"), F.substring(d, 3, 10)) \
         .when((F.length(d) == 11) & d.startswith("0"), F.substring(d, 2, 10)).otherwise(d)
    return F.when(d.rlike(r"^[6-9]\d{9}$"), d)
 
 
def gender(c):
    t = code(c)
    return F.when(t.isin("m", "male"), "M").when(t.isin("f", "female"), "F").when(t.isNotNull(), "O")
 
 
def aadhaar(c):
    """12 digits starting 0 or 1 (the synthetic rule of this project), else NULL."""
    d = F.regexp_replace(text(c), r"\D", "")
    return F.when(d.rlike(r"^[01]\d{11}$"), d)
 
 
def integer(c):
    d = F.regexp_replace(text(c), r"[,\s]", "")
    return F.when(d.rlike(r"^-?\d{1,9}$"), d.cast("int"))
 
 
def decimal(c):
    """Money as decimal(18,2); currency signs and thousands separators are removed."""
    d = F.regexp_replace(text(c), r"[^0-9.\-]", "")
    return F.when(d.rlike(r"^-?\d{1,16}(\.\d+)?$"), d.cast("decimal(18,2)"))
 
 
def _parse(t, formats):
    return F.coalesce(*[F.when(t.rlike(p), F.try_to_timestamp(t, F.lit(f))) for p, f in formats])
 
 
def date(c):
    t = text(c)
    t = F.when(t.rlike(r"^\d{4}-\d{2}-\d{2}[ T]"), F.substring(t, 1, 10)).otherwise(t)   # timestamp sent in a date column
    return _parse(t, DATE_FORMATS).cast("date")
 
 
def timestamp(c):
    return _parse(text(c), TS_FORMATS)
 
 
CLEANERS = {"string": text, "name": name, "code": code, "city": city, "phone": phone, "gender": gender,
            "aadhaar": aadhaar, "int": integer, "decimal": decimal, "date": date, "timestamp": timestamp}
 
 
def apply(df, spec):
    """Bronze rows -> silver columns for one table (config entry `spec`).
 
    Adds: is_deleted (from the source delete flag), _bad (columns whose value could not be read),
    _raw (the source row as JSON), and keeps the bronze metadata columns and id_fingerprint when present.
    """
    cols, bad = [], []
    for target, rule in spec["columns"].items():
        kind, source = (rule, target) if isinstance(rule, str) else rule
        raw = F.col(f"`{source}`")
        cols.append(CLEANERS[kind](raw).alias(target))
        if kind != "string":
            bad.append(F.when(text(raw).isNotNull() & CLEANERS[kind](raw).isNull(), F.lit(target)))
    flag = spec.get("delete")
    deleted = (F.lower(F.trim(F.col(f"`{flag['column']}`"))) == str(flag["value"]).lower()) if flag else F.lit(False)
    cols.append(F.coalesce(deleted, F.lit(False)).alias("is_deleted"))
    cols.append(F.array_compact(F.array(*bad)).alias("_bad") if bad else F.array().cast("array<string>").alias("_bad"))
    sources = [t if isinstance(r, str) else r[1] for t, r in spec["columns"].items()]
    cols.append(F.to_json(F.struct(*[F.col(f"`{c}`") for c in sources])).alias("_raw"))    # the row as the source sent it
    keep = [c for c in META + ["id_fingerprint"] if c in df.columns]
    return df.select(*cols, *keep)
 
 
def _kinds(spec):
    return {t: (r if isinstance(r, str) else r[0]) for t, r in spec["columns"].items()}
 
 
def check(df, spec):
    """Add _rule and _detail to cleaned rows: the first rule a row breaks, NULL for a good row.
    Also adds _soft: columns that were unreadable but do not block the row."""
    kinds = _kinds(spec)
    hard = F.array(*[F.lit(c) for c, k in kinds.items() if k in HARD]).cast("array<string>")
    hard_bad = F.array_intersect("_bad", hard)
    no_key = reduce(lambda a, b: a | b, [F.col(k).isNull() for k in spec["key"]])
    rules = [("missing_key", no_key, F.lit(",".join(spec["key"]))),
             ("unreadable_value", F.size(hard_bad) > 0, F.array_join(hard_bad, ","))]
    for col, values in spec.get("allowed", {}).items():
        bad = F.col(col).isNotNull() & ~F.col(col).isin([str(v) for v in values])
        rules.append(("value_not_allowed", bad, F.concat(F.lit(col + "="), F.col(col))))
    return (df.withColumn("_rule", F.coalesce(*[F.when(cond, F.lit(n)) for n, cond, _ in rules]))
              .withColumn("_detail", F.coalesce(*[F.when(cond, d) for _, cond, d in rules]))
              .withColumn("_soft", F.array_except("_bad", hard)))
 
 
def ensure_tables(spark, cfg):
    cat = cfg["catalog"]
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cat}.silver.quarantine (
        table_name STRING, row_key STRING, rule STRING, detail STRING, raw_row STRING,
        source_file STRING, first_seen TIMESTAMP, last_seen TIMESTAMP, status STRING)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cat}.ops.dq_results (
        run_ts TIMESTAMP, table_name STRING, rule STRING, checked BIGINT, failed BIGINT, status STRING)""")
 
 
def to_quarantine(spark, cfg, table, bad):
    """Upsert bad rows (row_key, rule, detail, raw_row, source_file). A row already there keeps its
    first_seen and only gets a new last_seen, so a rerun never adds duplicates."""
    rows = (bad.dropDuplicates(["row_key", "rule", "source_file"])
               .withColumn("table_name", F.lit(table)).withColumn("seen", F.current_timestamp()))
    rows.createOrReplaceTempView("_quarantine_in")
    spark.sql(f"""
        MERGE INTO {cfg['catalog']}.silver.quarantine AS t
        USING _quarantine_in AS s
          ON t.table_name = s.table_name AND t.row_key = s.row_key AND t.rule = s.rule
         AND t.source_file <=> s.source_file
        WHEN MATCHED THEN UPDATE SET t.last_seen = s.seen, t.detail = s.detail
        WHEN NOT MATCHED THEN INSERT (table_name, row_key, rule, detail, raw_row, source_file, first_seen, last_seen, status)
             VALUES (s.table_name, s.row_key, s.rule, s.detail, s.raw_row, s.source_file, s.seen, s.seen, 'open')""")
 
 
def split_and_log(spark, cfg, table, df, spec):
    """Cleaned rows (from apply) -> good rows. Bad rows go to silver.quarantine, counts to ops.dq_results.
    Returns (good rows, {"rows": rows checked, rule: failed count, ...})."""
    checked = check(df, spec)
    stats = checked.agg(F.count("*").alias("n"),
                        *[F.sum(F.when(F.col("_rule") == r, 1).otherwise(0)).alias(r)
                          for r in ("missing_key", "unreadable_value", "value_not_allowed")],
                        F.sum(F.when(F.size("_soft") > 0, 1).otherwise(0)).alias("unreadable_contact")).first()
    n = stats["n"]
    failed = {r: int(stats[r] or 0) for r in ("missing_key", "unreadable_value", "value_not_allowed", "unreadable_contact")}
    if n and sum(v for r, v in failed.items() if r != "unreadable_contact"):
        bad = checked.filter("_rule IS NOT NULL").select(
            F.coalesce(F.nullif(F.concat_ws("|", *spec["key"]), F.lit("")), F.lit("(no key)")).alias("row_key"),
            F.col("_rule").alias("rule"), F.col("_detail").alias("detail"),
            F.col("_raw").alias("raw_row"), F.col("_source_file").alias("source_file"))
        to_quarantine(spark, cfg, table, bad)
    status = lambda r, v: "pass" if v == 0 else ("warn" if r == "unreadable_contact" else "quarantined")
    spark.createDataFrame([(table, r, n, v, status(r, v)) for r, v in failed.items()],
                          "table_name string, rule string, checked long, failed long, status string") \
         .select(F.current_timestamp().alias("run_ts"), "*") \
         .write.mode("append").saveAsTable(f"{cfg['catalog']}.ops.dq_results")
    good = checked.filter("_rule IS NULL").drop("_rule", "_detail", "_soft", "_bad", "_raw")
    return good, {"rows": n, **failed}
 
 
def bronze_new(spark, cfg, table, target):
    """Bronze rows not yet seen by the silver table: loaded after the newest row silver holds."""
    df = spark.table(f"{cfg['catalog']}.bronze.{table}")
    if spark.catalog.tableExists(target):
        mark = spark.table(target).agg(F.max("_ingest_ts")).first()[0]
        if mark is not None:
            df = df.filter(F.col("_ingest_ts") > F.lit(mark))
    return df
 
 
def latest(df, spec):
    """One row per key: the highest order column, then the latest load."""
    w = Window.partitionBy(*spec["key"]).orderBy(F.col(spec["order"]).desc_nulls_last(), F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
 
 
def load_latest(spark, cfg, table, spec):
    """Bronze -> silver for a table kept as one row per key (mode: latest).
    New bronze rows are cleaned, checked, reduced to the latest row per key and merged.
    An older version that arrives late never overwrites a newer one.
    Returns {"rows", "quarantined", "inserted", "updated"}."""
    target = f"{cfg['catalog']}.silver.{table}"
    new = bronze_new(spark, cfg, table, target)
    if new.limit(1).count() == 0:
        return {"rows": 0, "quarantined": 0, "inserted": 0, "updated": 0}
    good, stats = split_and_log(spark, cfg, table, apply(new, spec), spec)
    rows = latest(good, spec).withColumn("_silver_ts", F.current_timestamp())
    out = {"rows": stats["rows"],
           "quarantined": stats["missing_key"] + stats["unreadable_value"] + stats["value_not_allowed"]}
    if not spark.catalog.tableExists(target):
        rows.write.saveAsTable(target)
        return {**out, "inserted": spark.table(target).count(), "updated": 0}
    rows.createOrReplaceTempView("_silver_in")
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in spec["key"])
    o = f"`{spec['order']}`"
    m = spark.sql(f"""
        MERGE INTO {target} AS t
        USING _silver_in AS s
          ON {on}
        WHEN MATCHED AND (s.{o} > t.{o} OR (s.{o} = t.{o} AND s._ingest_ts > t._ingest_ts)) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""").first()
    return {**out, "inserted": m["num_inserted_rows"], "updated": m["num_updated_rows"]}
 