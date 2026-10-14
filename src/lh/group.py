"""Group consolidation: BHG prod Gold + FQF prod Gold = one group view (BHG repo only).

BHG reads the FQF Gold of the SAME environment, read-only (config/group.yml), so FQF dev or test data never
reaches BHG prod. Since 5.5s the group tables are SQL: one SELECT per table in gold/sql/group/<name>.sql, run in
the order of config/group.yml (sql_build) by lh/gold_sql.group_tables(). All of them live in <bhg catalog>.gold
and are rebuilt in full on every run:

  dim_company          one row per company (parent and child)
  map_group_customer   one row per company member (BHG patient, FQF master customer) -> its person
  dim_group_customer   one row per real person across both companies
  group_match_review   pairs a person must look at: same phone + date of birth but a different Aadhaar,
                       or a hospital bill paid by a loan of a different Aadhaar
  group_intercompany   one row per BHG invoice paid by an FQF loan: BHG payment vs FQF payout
  fact_group_daily     one row per date, company and measure: amount, intercompany part, group amount
  group_exposure       one row per person who owes the group money, against their credit-policy limit
  fact_group_kpi       one row per headline number, as of the run date

This module keeps the parts that are not a SELECT:
  source()          which FQF catalog this BHG environment may read (never another environment's)
  decisions()       ops.group_match_decisions: review decisions by the Manager, created empty, never rebuilt
  merge_control()   ops.merge_log (one row per group run) and ops.merge_backlog (business days BHG has and FQF
                    has not delivered yet; BHG never waits, the days are merged oldest first when FQF catches up)

How people are matched (only keyed hashes are read, never names, phones or dates of birth):
  1. same id_fingerprint (keyed hash of Aadhaar) = same person
  2. a hospital-bill loan links the BHG patient of the invoice and the FQF customer of the loan;
     if only one side has an Aadhaar fingerprint, the other side joins that person
  3. a decision recorded in ops.group_match_decisions ("same") merges a reviewed pair
Look-alikes are never merged automatically.
"""
import os
from datetime import date, datetime, timedelta

import yaml
from pyspark.sql import functions as F

from lh import common


def load_group():
    with open(os.path.join(common.repo_root(), "config", "group.yml")) as f:
        return yaml.safe_load(f)


def source(cfg, env):
    """The FQF catalog this BHG environment reads. Group code runs only in the parent."""
    if cfg["code"] != "bhg":
        raise RuntimeError("group consolidation runs only in the BHG (parent) repo")
    src = load_group()["source"][env]
    if src != f"fqf_{env}":
        raise RuntimeError(f"{env} must read fqf_{env}, config says {src}")
    return src


DECISIONS = "ops.group_match_decisions"


def decisions(spark, cat):
    """Review decisions made by a person; created empty on first use, never rebuilt.
    The Manager records them, so lh_manager may read and write this one table in every environment
    (granted here on each run, rerun-safe; in prod the Manager is otherwise read-only)."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cat}.{DECISIONS} (
        bhg_patient_id STRING, fqf_customer_id STRING, decision STRING, decided_by STRING, decided_at TIMESTAMP)""")
    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {cat}.{DECISIONS} TO `lh_manager`")
    return spark.table(f"{cat}.{DECISIONS}").filter(F.col("decision").isin("same", "different"))


def data_to(fact, exclude=()):
    """Last business date a company fact holds, up to today. Planned rows (instalments due) are left
    out, and so is any row dated in the future (a bad source date must not fake a later delivery)."""
    today = int(date.today().strftime("%Y%m%d"))
    k = (fact.filter(~F.col("event_type").isin(*exclude) if exclude else F.lit(True))
             .filter(F.col("date_key") <= today).agg(F.max("date_key")).first()[0])
    return datetime.strptime(str(k), "%Y%m%d").date() if k else None


def merge_control(spark, cfg, env, built):
    """Record this run in ops.merge_log and keep ops.merge_backlog up to date. Returns the log row."""
    cat, src = cfg["catalog"], source(cfg, env)
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cat}.ops.merge_log (
        run_ts TIMESTAMP, env STRING, bhg_data_to DATE, fqf_data_to DATE, fqf_gold_ts TIMESTAMP,
        new_fqf_days INT, backlog_days INT, status STRING, tables STRING)""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cat}.ops.merge_backlog (
        business_date DATE, first_seen TIMESTAMP, status STRING, merged_at TIMESTAMP)""")
    ffact = spark.table(f"{src}.gold.fact_fqf_event")
    b_to = data_to(spark.table(f"{cat}.gold.fact_bhg_event"))
    f_to = data_to(ffact, exclude=("emi_due",))
    f_ts = ffact.agg(F.max("_gold_ts")).first()[0]
    prev = spark.sql(f"SELECT max_by(fqf_data_to, run_ts) FROM {cat}.ops.merge_log WHERE env = '{env}'").first()[0]

    missing = [(b_to - timedelta(days=i)).isoformat() for i in range((b_to - f_to).days)] if b_to and f_to and b_to > f_to else []
    if missing:
        spark.createDataFrame([(d,) for d in missing], "d string").createOrReplaceTempView("_missing")
        spark.sql(f"""MERGE INTO {cat}.ops.merge_backlog t
                      USING (SELECT CAST(d AS DATE) AS business_date FROM _missing) s
                      ON t.business_date = s.business_date
                      WHEN NOT MATCHED THEN INSERT (business_date, first_seen, status, merged_at)
                                            VALUES (s.business_date, current_timestamp(), 'waiting', NULL)""")
    spark.sql(f"""UPDATE {cat}.ops.merge_backlog SET status = 'merged', merged_at = current_timestamp()
                  WHERE status = 'waiting' AND business_date <= DATE'{f_to.isoformat()}'""")
    waiting = spark.sql(f"SELECT count(*) FROM {cat}.ops.merge_backlog WHERE status = 'waiting'").first()[0]

    row = {"env": env, "bhg_data_to": b_to, "fqf_data_to": f_to, "fqf_gold_ts": f_ts,
           "new_fqf_days": (f_to - prev).days if prev and f_to else None, "backlog_days": waiting,
           "status": "complete" if waiting == 0 else "fqf_behind", "tables": str(built)}
    (spark.createDataFrame([row], "env string, bhg_data_to date, fqf_data_to date, fqf_gold_ts timestamp, "
                                  "new_fqf_days int, backlog_days int, status string, tables string")
          .withColumn("run_ts", F.current_timestamp())
          .select("run_ts", "env", "bhg_data_to", "fqf_data_to", "fqf_gold_ts", "new_fqf_days", "backlog_days",
                  "status", "tables")
          .write.mode("append").saveAsTable(f"{cat}.ops.merge_log"))
    return row
