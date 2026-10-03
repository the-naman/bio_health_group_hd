"""Silver: cleaning rules as Spark column expressions (no Python UDFs, safe in ANSI mode).

Every cleaner takes a text column and returns a clean column; a value that cannot be
read becomes NULL, never an error. apply() runs the cleaners named in config/silver.yml
and adds `_bad`: the list of columns that had a value which could not be read.

Rules then decide what happens to a row (split_and_log):
  not_prepared (a prepare step could not make sense of the row or file)  -> row goes to silver.quarantine
  missing_key, unreadable_value (number, date, code), value_not_allowed -> row goes to silver.quarantine
  date_order (a date that must not be before another one is)            -> row goes to silver.quarantine
  too_late (daily files only: the change is older than late_days)       -> row goes to silver.quarantine
  orphan (the parent row is missing)                                    -> row waits in silver.quarantine and is
        tried again on every run; after orphan_retry_days (by business date) it is accepted with
        orphan_of naming the missing parent, so no money event is lost and gold can show "unknown"
  unreadable phone or national id                                       -> value stays NULL, row is kept (warning)
Every rule's result is written to ops.dq_results.
"""
import os
from datetime import timedelta
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


def load_id_maps():
    """Optional `id_maps` section of config/silver.yml (see build_id_map)."""
    with open(os.path.join(common.repo_root(), "config", "silver.yml")) as f:
        return yaml.safe_load(f).get("id_maps") or {}


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
    keep = [c for c in META + ["id_fingerprint", "_reject"] if c in df.columns]
    return df.select(*cols, *keep)


def _kinds(spec):
    return {t: (r if isinstance(r, str) else r[0]) for t, r in spec["columns"].items()}


RULES = ("not_prepared", "missing_key", "unreadable_value", "value_not_allowed", "date_order", "too_late", "orphan")


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
    rules = [("not_prepared", F.col("_reject").isNotNull(), F.col("_reject"))] if "_reject" in df.columns else []
    rules += [("missing_key", no_key, F.lit(",".join(need))),
             ("unreadable_value", F.size(hard_bad) > 0, F.array_join(hard_bad, ","))]
    for col, values in spec.get("allowed", {}).items():
        bad = F.col(col).isNotNull() & ~F.col(col).isin([str(v) for v in values])
        rules.append(("value_not_allowed", bad, F.concat(F.lit(col + "="), F.col(col))))
    for later, earlier in spec.get("not_before", {}).items():        # e.g. effective_to must not be before effective_from
        rules.append(("date_order", F.col(later) < F.col(earlier), F.lit(f"{later} is before {earlier}")))
    if cfg and "_business_date" in df.columns:
        days = F.datediff("_business_date", F.to_date(spec["order"]))
        late = (F.col("_business_date") >= F.lit(str(cfg["daily_start"])).cast("date")) & (days > int(cfg["late_days"]))
        rules.append(("too_late", late, F.concat(days.cast("string"), F.lit(" days late"))))
    if "_wait" in df.columns:
        rules.append(("orphan", F.col("_wait"), F.col("orphan_of")))
    return (df.withColumn("_rule", F.coalesce(*[F.when(cond, F.lit(n)) for n, cond, _ in rules]))
              .withColumn("_detail", F.coalesce(*[F.when(cond, d) for _, cond, d in rules]))
              .withColumn("_soft", F.array_except("_bad", hard)))


def mark_orphans(spark, cfg, df, spec, asof):
    """Add orphan_of (the columns whose parent row is missing, NULL when all parents exist) and
    _wait (the row is still inside the retry window, counted in business days up to `asof`).
    A parent is looked up in the silver table named in `parents`, by the same column name."""
    names = list(spec.get("parents", {}))
    if not names or asof is None:
        return df.withColumn("orphan_of", F.lit(None).cast("string")).withColumn("_wait", F.lit(False))
    missing = []
    for col in names:
        keys = (spark.table(f"{cfg['catalog']}.silver.{spec['parents'][col]}")
                     .select(F.col(col).alias(f"_p_{col}")).distinct())
        df = df.join(F.broadcast(keys), F.col(col) == F.col(f"_p_{col}"), "left")
        missing.append(F.when(F.col(col).isNotNull() & F.col(f"_p_{col}").isNull(), F.lit(col)))
    df = (df.withColumn("orphan_of", F.nullif(F.array_join(F.array_compact(F.array(*missing)), ","), F.lit("")))
            .drop(*[f"_p_{c}" for c in names]))
    age = F.datediff(F.lit(asof), F.col("_business_date"))
    return df.withColumn("_wait", F.col("orphan_of").isNotNull() & (age <= int(cfg["orphan_retry_days"])))


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


def split_and_log(spark, cfg, table, df, spec, asof=None):
    """Cleaned rows (from apply) -> good rows. Bad rows go to silver.quarantine, counts to ops.dq_results.
    `asof` (newest business date of the table) switches on the orphan check.
    Returns (good rows, stats): rows checked, failed count per rule, and "hold" = the oldest load
    time among waiting orphans (the watermark must stay before it so they are tried again)."""
    checked = check(mark_orphans(spark, cfg, df, spec, asof), spec, cfg)
    warn = {"unreadable_contact": F.size("_soft") > 0,
            "orphan_accepted": F.col("_rule").isNull() & F.col("orphan_of").isNotNull()}
    hold = F.min(F.when(F.col("_rule") == "orphan", F.col("_ingest_ts"))) if "_ingest_ts" in df.columns else F.lit(None)
    stats = checked.agg(F.count("*").alias("n"),
                        *[F.sum(F.when(F.col("_rule") == r, 1).otherwise(0)).alias(r) for r in RULES],
                        *[F.sum(F.when(cond, 1).otherwise(0)).alias(r) for r, cond in warn.items()],
                        hold.alias("hold")).first()
    n = stats["n"]
    failed = {r: int(stats[r] or 0) for r in RULES + tuple(warn)}
    if n and sum(failed[r] for r in RULES):
        bad = checked.filter("_rule IS NOT NULL").select(
            F.coalesce(F.nullif(F.concat_ws("|", *spec["key"]), F.lit("")), F.lit("(no key)")).alias("row_key"),
            F.col("_rule").alias("rule"), F.col("_detail").alias("detail"),
            F.col("_raw").alias("raw_row"), F.col("_source_file").alias("source_file"))
        to_quarantine(spark, cfg, table, bad)
    status = lambda r, v: "pass" if v == 0 else ("warn" if r in warn else "quarantined")
    spark.createDataFrame([(table, r, n, v, status(r, v)) for r, v in failed.items()],
                          "table_name string, rule string, checked long, failed long, status string") \
         .select(F.current_timestamp().alias("run_ts"), "*") \
         .write.mode("append").saveAsTable(f"{cfg['catalog']}.ops.dq_results")
    good = checked.filter("_rule IS NULL").drop("_rule", "_detail", "_soft", "_bad", "_raw", "_wait", "_reject")
    return good, {"rows": n, **failed, "hold": stats["hold"]}


def close_orphans(spark, cfg, table, spec):
    """Orphans of this table that have reached silver are closed in the quarantine:
    resolved = the parent arrived, accepted = the retry window ended and the row was let in."""
    key = ", ".join(f"`{k}`" for k in spec["key"])
    spark.sql(f"""
        MERGE INTO {cfg['catalog']}.silver.quarantine AS q
        USING (SELECT concat_ws('|', {key}) AS row_key, max(orphan_of) AS orphan_of
               FROM {cfg['catalog']}.silver.{table} GROUP BY 1) AS s
          ON q.table_name = '{table}' AND q.rule = 'orphan' AND q.status = 'open' AND q.row_key = s.row_key
        WHEN MATCHED THEN UPDATE SET
          q.status = CASE WHEN s.orphan_of IS NULL THEN 'resolved' ELSE 'accepted' END,
          q.last_seen = current_timestamp()""")


def _finish(spark, cfg, table, spec, mark, stats):
    """After a successful merge: close orphans that got in, then move the watermark.
    It stays just before the oldest waiting orphan, so that row is read again next run."""
    if spec.get("parents"):
        close_orphans(spark, cfg, table, spec)
    if stats["hold"] is not None:
        mark = min(mark, stats["hold"] - timedelta(microseconds=1))
    save_state(spark, cfg, table, mark)


def _asof(new):
    return new.agg(F.max("_business_date")).first()[0] if "_business_date" in new.columns else None


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


def prepared(spark, df, spec):
    """A table whose bronze shape is not flat text (JSON, drifting headers, documents) names a
    function in lh/prepare.py that turns its bronze rows into flat text columns first.
    It may add `_reject` (a reason text): such a row goes to quarantine as not_prepared."""
    if not spec.get("prepare"):
        return df
    from lh import prepare
    return getattr(prepare, spec["prepare"])(spark, df, spec)


def latest(df, spec):
    """One row per key: the highest order column, then the latest load."""
    w = Window.partitionBy(*spec["key"]).orderBy(F.col(spec["order"]).desc_nulls_last(), F.col("_ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def load_latest(spark, cfg, table, spec):
    """Bronze -> silver for a table kept as one row per key (mode: latest).
    New bronze rows are cleaned, checked, reduced to the latest row per key and merged.
    An older version that arrives late never overwrites a newer one.
    Returns {"rows", "quarantined", "orphan_wait", "orphan_accepted", "inserted", "updated"}."""
    target = f"{cfg['catalog']}.silver.{table}"
    new, mark = bronze_new(spark, cfg, table)
    if mark is None:
        return {"rows": 0, "quarantined": 0, "orphan_wait": 0, "orphan_accepted": 0, "inserted": 0, "updated": 0}
    new = prepared(spark, new, spec)
    good, stats = split_and_log(spark, cfg, table, apply(new, spec), spec, _asof(new))
    rows = latest(good, spec).withColumn("_silver_ts", F.current_timestamp())
    out = {"rows": stats["rows"], "quarantined": sum(stats[r] for r in RULES),
           "orphan_wait": stats["orphan"], "orphan_accepted": stats["orphan_accepted"]}
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
    _finish(spark, cfg, table, spec, mark, stats)      # only after the merge: a failed run is simply repeated
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
    Returns {"rows", "quarantined", "orphan_wait", "orphan_accepted", "inserted", "updated"}."""
    target = f"{cfg['catalog']}.silver.{table}"
    new, mark = bronze_new(spark, cfg, table)
    if mark is None:
        return {"rows": 0, "quarantined": 0, "orphan_wait": 0, "orphan_accepted": 0, "inserted": 0, "updated": 0}
    new = prepared(spark, new, spec)
    good, stats = split_and_log(spark, cfg, table, apply(new, spec), spec, _asof(new))
    exists = spark.catalog.tableExists(target)
    rows = scd2_chain(spark.table(target) if exists else None, good, spec) \
        .withColumn("_silver_ts", F.current_timestamp())
    out = {"rows": stats["rows"], "quarantined": sum(stats[r] for r in RULES),
           "orphan_wait": stats["orphan"], "orphan_accepted": stats["orphan_accepted"]}
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
    _finish(spark, cfg, table, spec, mark, stats)
    return out


def load_table(spark, cfg, table, spec):
    return (load_scd2 if spec.get("mode") == "scd2" else load_latest)(spark, cfg, table, spec)


def build_id_map(spark, cfg, name, m):
    """Find the same real-world entity stored under several ids and map every id to one survivor.

    m = {table, key, match, first}: rows of `table` are grouped by the `match` column (for customers
    the keyed fingerprint of the national id); the id that appeared first (`first`, then the id
    itself) is the survivor. An id without a match value maps to itself.
    The map <catalog>.silver.<name> is rebuilt in full on every run (small, and so rerun-safe):
    key, survivor_id, is_survivor, group_size. Other tables are joined through it; nothing is rewritten.
    Returns {"ids", "duplicates"}."""
    key = m["key"]
    per_id = (spark.table(f"{cfg['catalog']}.silver.{m['table']}").groupBy(key)
                   .agg(F.max(m["match"]).alias("_match"), F.min(m["first"]).alias("_first")))
    group = Window.partitionBy("_match")
    known = F.col("_match").isNotNull()
    out = (per_id.withColumn("survivor_id", F.when(known, F.first(key).over(group.orderBy("_first", key)))
                                             .otherwise(F.col(key)))
                 .withColumn("group_size", F.when(known, F.count("*").over(group)).otherwise(F.lit(1)).cast("int"))
                 .select(key, "survivor_id", (F.col(key) == F.col("survivor_id")).alias("is_survivor"),
                         "group_size", F.current_timestamp().alias("_silver_ts")))
    out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{cfg['catalog']}.silver.{name}")
    r = spark.table(f"{cfg['catalog']}.silver.{name}").agg(
        F.count("*").alias("ids"), F.sum((~F.col("is_survivor")).cast("int")).alias("duplicates")).first()
    return {"ids": r["ids"], "duplicates": int(r["duplicates"] or 0)}


def checkpoint(spark, cfg, tables, max_quarantine_pct=5.0, small_table_rows=2):
    """The gate between silver and gold. For every table:
      rows     : the silver table is not empty
      unique   : one row per key (for history tables: one current row per key)
      rejected : rows in quarantine for a broken rule (waiting orphans are not counted) stay within
                 max_quarantine_pct of the table, with small_table_rows always allowed so that one
                 bad row in a tiny table does not block the run
    Prints one line per table, writes the result to ops.dq_results and returns True when all pass.
    The runner stops with an error on False, so gold never reads a silver layer that is not sound."""
    cat = cfg["catalog"]
    rejected = {r["table_name"]: r["count"] for r in spark.table(f"{cat}.silver.quarantine")
                .filter("status = 'open' AND rule <> 'orphan'").groupBy("table_name").count().collect()}
    out, ok = [], True
    print(f"{'table':20} {'rows':>7} {'keys':>7} {'rejected':>8} {'allowed':>7}  result")
    for table, spec in tables.items():
        df = spark.table(f"{cat}.silver.{table}")
        if spec.get("mode") == "scd2":
            df = df.filter("is_current")
        r = df.agg(F.count("*").alias("n"), F.countDistinct(*spec["key"]).alias("k")).first()
        bad = rejected.get(table, 0)
        allowed = max(small_table_rows, int((r["n"] + bad) * max_quarantine_pct / 100))
        failed = [name for name, wrong in (("rows", r["n"] == 0), ("unique", r["n"] != r["k"]), ("rejected", bad > allowed)) if wrong]
        ok = ok and not failed
        print(f"{table:20} {r['n']:>7} {r['k']:>7} {bad:>8} {allowed:>7}  {'FAIL ' + ','.join(failed) if failed else 'pass'}")
        out.append((table, "checkpoint", r["n"], len(failed), "fail" if failed else "pass"))
    spark.createDataFrame(out, "table_name string, rule string, checked long, failed long, status string") \
         .select(F.current_timestamp().alias("run_ts"), "*") \
         .write.mode("append").saveAsTable(f"{cat}.ops.dq_results")
    print("SILVER CHECKPOINT", "PASS" if ok else "FAIL")
    return ok

    