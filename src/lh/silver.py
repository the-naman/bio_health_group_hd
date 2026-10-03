"""Silver: cleaning rules as Spark column expressions (no Python UDFs, safe in ANSI mode).
 
Every cleaner takes a text column and returns a clean column; a value that cannot be
read becomes NULL, never an error. apply() runs the cleaners named in config/silver.yml
and adds `_bad`: the list of columns that had a value which could not be read.
 
Rules then decide what happens to a row (split_and_log):
  missing_key, unreadable_value (number, date, code), value_not_allowed -> row goes to silver.quarantine
  too_late (daily files only: the change is older than late_days)       -> row goes to silver.quarantine
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
 
 
RULES = ("missing_key", "unreadable_value", "value_not_allowed", "too_late")
 
 
def check(df, spec, cfg=None):
    """Add _rule and _detail to cleaned rows: the first rule a row breaks, NULL for a good row.
    Also adds _soft: columns that were unreadable but do not block the row.
    With cfg, a row in a daily file whose change (order column) is more than late_days older
    than the file's business date is too_late. History files (before daily_start) are exempt."""
    kinds = _kinds(spec)
    hard = F.array(*[F.lit(c) for c, k in kinds.items() if k in HARD]).cast("array<string>")
    hard_bad = F.array_intersect("_bad", hard)
    need = spec["key"] + ([spec["order"]] if spec.get("mode") == "scd2" else [])    # history needs its date
    no_key = reduce(lambda a, b: a | b, [F.col(k).isNull() for k in need])
    rules = [("missing_key", no_key, F.lit(",".join(need))),
             ("unreadable_value", F.size(hard_bad) > 0, F.array_join(hard_bad, ","))]
    for col, values in spec.get("allowed", {}).items():
        bad = F.col(col).isNotNull() & ~F.col(col).isin([str(v) for v in values])
        rules.append(("value_not_allowed", bad, F.concat(F.lit(col + "="), F.col(col))))
    if cfg and "_business_date" in df.columns:
        days = F.datediff("_business_date", F.to_date(spec["order"]))
        late = (F.col("_business_date") >= F.lit(str(cfg["daily_start"])).cast("date")) & (days > int(cfg["late_days"]))
        rules.append(("too_late", late, F.concat(days.cast("string"), F.lit(" days late"))))
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
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cat}.ops.silver_state (
        table_name STRING, last_ingest_ts TIMESTAMP, updated_ts TIMESTAMP)""")
 
 
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
    checked = check(df, spec, cfg)
    stats = checked.agg(F.count("*").alias("n"),
                        *[F.sum(F.when(F.col("_rule") == r, 1).otherwise(0)).alias(r) for r in RULES],
                        F.sum(F.when(F.size("_soft") > 0, 1).otherwise(0)).alias("unreadable_contact")).first()
    n = stats["n"]
    failed = {r: int(stats[r] or 0) for r in RULES + ("unreadable_contact",)}
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
 
 
def bronze_new(spark, cfg, table):
    """Bronze rows silver has not processed yet, and the newest load time among them.
    The last processed load time per table is kept in ops.silver_state (the watermark)."""
    df = spark.table(f"{cfg['catalog']}.bronze.{table}")
    state = spark.table(f"{cfg['catalog']}.ops.silver_state").filter(F.col("table_name") == table).first()
    if state is not None:
        df = df.filter(F.col("_ingest_ts") > F.lit(state["last_ingest_ts"]))
    return df, df.agg(F.max("_ingest_ts")).first()[0]
 
 
def save_state(spark, cfg, table, mark):
    spark.createDataFrame([(table, mark)], "table_name string, last_ingest_ts timestamp") \
         .createOrReplaceTempView("_state_in")
    spark.sql(f"""
        MERGE INTO {cfg['catalog']}.ops.silver_state AS t
        USING _state_in AS s ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET t.last_ingest_ts = s.last_ingest_ts, t.updated_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (table_name, last_ingest_ts, updated_ts)
             VALUES (s.table_name, s.last_ingest_ts, current_timestamp())""")
 
 
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
    new, mark = bronze_new(spark, cfg, table)
    if mark is None:
        return {"rows": 0, "quarantined": 0, "inserted": 0, "updated": 0}
    good, stats = split_and_log(spark, cfg, table, apply(new, spec), spec)
    rows = latest(good, spec).withColumn("_silver_ts", F.current_timestamp())
    out = {"rows": stats["rows"], "quarantined": sum(stats[r] for r in RULES)}
    if not spark.catalog.tableExists(target):
        rows.write.saveAsTable(target)
        out.update(inserted=spark.table(target).count(), updated=0)
    else:
        rows.createOrReplaceTempView("_silver_in")
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in spec["key"])
        o = f"`{spec['order']}`"
        m = spark.sql(f"""
            MERGE INTO {target} AS t
            USING _silver_in AS s
              ON {on}
            WHEN MATCHED AND (s.{o} > t.{o} OR (s.{o} = t.{o} AND s._ingest_ts > t._ingest_ts)) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""").first()
        out.update(inserted=m["num_inserted_rows"], updated=m["num_updated_rows"])
    save_state(spark, cfg, table, mark)                # only after the merge: a failed run is simply repeated
    return out
 
 
def tracked(spec):
    """Columns whose change makes a new history version: everything except the key and the order column."""
    return [c for c in spec["columns"] if c not in spec["key"] and c != spec["order"]] + ["is_deleted"]
 
 
def scd2_chain(existing, incoming, spec):
    """Build the full version chain (SCD Type 2) for every key that has new rows.
 
    existing : the silver table (or None on the first load)
    incoming : good cleaned rows from bronze
    Steps: add a hash of the tracked columns; take the stored versions of the touched keys;
    keep one row per key and valid_from; drop a new version that changes nothing;
    then valid_to = the next version's valid_from, and the last version is current.
    A version that arrives late lands in its right place, because the whole chain is rebuilt.
    """
    key = spec["key"]
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in tracked(spec)]
    inc = (incoming.withColumn("_row_hash", F.sha2(F.concat_ws("||", *parts), 256))
                   .withColumn("valid_from", F.col(spec["order"])).withColumn("_new", F.lit(True)))
    versions = inc
    if existing is not None:
        old = existing.join(inc.select(*key).distinct(), key, "left_semi").withColumn("_new", F.lit(False))
        versions = old.select(*inc.columns).unionByName(inc)
    slot = Window.partitionBy(*key, "valid_from")
    versions = (versions.withColumn("_had", F.min("_new").over(slot) == F.lit(False))     # this version was stored before
                        .withColumn("_rn", F.row_number().over(slot.orderBy(F.col("_ingest_ts").desc(), F.col("_new"))))
                        .filter("_rn = 1"))
    chain = Window.partitionBy(*key).orderBy("valid_from")
    versions = versions.withColumn("_prev", F.lag("_row_hash").over(chain))
    same = F.col("_new") & ~F.col("_had") & F.col("_prev").isNotNull() & (F.col("_prev") == F.col("_row_hash"))
    versions = versions.filter(~same)
    return (versions.withColumn("valid_to", F.lead("valid_from").over(chain))
                    .withColumn("is_current", F.col("valid_to").isNull())
                    .drop("_new", "_had", "_rn", "_prev"))
 
 
def load_scd2(spark, cfg, table, spec):
    """Bronze -> silver for a table that keeps history (mode: scd2).
    Returns {"rows", "quarantined", "inserted", "updated"}."""
    target = f"{cfg['catalog']}.silver.{table}"
    new, mark = bronze_new(spark, cfg, table)
    if mark is None:
        return {"rows": 0, "quarantined": 0, "inserted": 0, "updated": 0}
    good, stats = split_and_log(spark, cfg, table, apply(new, spec), spec)
    exists = spark.catalog.tableExists(target)
    rows = scd2_chain(spark.table(target) if exists else None, good, spec) \
        .withColumn("_silver_ts", F.current_timestamp())
    out = {"rows": stats["rows"], "quarantined": sum(stats[r] for r in RULES)}
    if not exists:
        rows.write.saveAsTable(target)
        out.update(inserted=spark.table(target).count(), updated=0)
    else:
        rows.createOrReplaceTempView("_silver_in")
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in spec["key"])
        m = spark.sql(f"""
            MERGE INTO {target} AS t
            USING _silver_in AS s
              ON {on} AND t.valid_from = s.valid_from
            WHEN MATCHED AND (NOT (t.valid_to <=> s.valid_to) OR t._row_hash <> s._row_hash) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *""").first()
        out.update(inserted=m["num_inserted_rows"], updated=m["num_updated_rows"])
    save_state(spark, cfg, table, mark)
    return out
 
 
def load_table(spark, cfg, table, spec):
    return (load_scd2 if spec.get("mode") == "scd2" else load_latest)(spark, cfg, table, spec)
 
 