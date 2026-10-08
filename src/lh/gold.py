"""Gold: the star schema that business users and BI read.

  dim_date            one row per calendar day, same in every catalog (date_key = yyyymmdd)
  dim_<entity>        one row per entity (or per version, for entities with history)
  fact_<company>_event  one row per money event (next steps)

Keys: a surrogate key is a 64-bit hash of the natural key (plus valid_from for a version), so it is
the same on every rebuild. Key -1 is the "unknown" member that a fact points to when the parent is
missing (an orphan that Silver accepted). The first version of an entity is open from 1900-01-01 and
the last one to 9999-12-31, so every event finds exactly one version.

Every Gold table is rebuilt in full from Silver on each run. Silver holds the history, so Gold
needs none of its own, and a rerun always gives the same result.
"""
import os

import yaml
from pyspark.sql import Window, functions as F

from lh import common


def load_gold():
    with open(os.path.join(common.repo_root(), "config", "gold.yml")) as f:
        return yaml.safe_load(f)


def save(spark, cfg, name, df):
    """Replace <catalog>.gold.<name> with df and return its row count."""
    target = f"{cfg['catalog']}.gold.{name}"
    (df.withColumn("_gold_ts", F.current_timestamp())
       .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target))
    return spark.table(target).count()


FAR_PAST, FAR_FUTURE = "1900-01-01 00:00:00", "9999-12-31 23:59:59"


def skey(*cols):
    """Surrogate key: stable 64-bit hash of the given columns."""
    return F.xxhash64(*[F.col(c).cast("string") for c in cols])


def history(df, id_col):
    """Silver SCD2 rows ready for Gold: first version open from the far past, last one to the far future."""
    first = F.row_number().over(Window.partitionBy(id_col).orderBy("valid_from")) == 1
    return (df.withColumn("valid_from", F.when(first, F.lit(FAR_PAST).cast("timestamp")).otherwise(F.col("valid_from")))
              .withColumn("valid_to", F.coalesce(F.col("valid_to"), F.lit(FAR_FUTURE).cast("timestamp"))))


def with_unknown(spark, df, key_col, values):
    """Add the unknown member (key -1): the given values, everything else empty, open for all time."""
    fill = {key_col: -1, "valid_from": FAR_PAST, "valid_to": FAR_FUTURE, "is_current": True, **values}
    row = spark.range(1).select(*[F.lit(fill.get(c)).cast(t).alias(c) for c, t in df.dtypes])
    return df.unionByName(row)


def dim_date(spark, start, end, fiscal_start=4):
    """Calendar from start to end. Financial year FY2026-27 = April 2026 to March 2027."""
    d = F.col("date")
    fy = F.when(F.month(d) >= fiscal_start, F.year(d)).otherwise(F.year(d) - 1)
    return (spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
            .select(F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
                    d,
                    F.year(d).alias("year"),
                    F.quarter(d).alias("quarter"),
                    F.month(d).alias("month"),
                    F.date_format(d, "MMMM").alias("month_name"),
                    F.date_format(d, "yyyy-MM").alias("year_month"),
                    F.dayofmonth(d).alias("day"),
                    (F.weekday(d) + 1).alias("day_of_week"),                      # 1 = Monday ... 7 = Sunday
                    F.date_format(d, "EEEE").alias("day_name"),
                    F.weekofyear(d).alias("week_of_year"),
                    (F.weekday(d) >= 5).alias("is_weekend"),
                    (d == F.last_day(d)).alias("is_month_end"),
                    F.concat(F.lit("FY"), fy.cast("string"), F.lit("-"),
                             F.lpad(((fy + 1) % 100).cast("string"), 2, "0")).alias("fiscal_year"),
                    (F.floor(((F.month(d) - fiscal_start + 12) % 12) / 3) + 1).cast("int").alias("fiscal_quarter")))


def build_dim_date(spark, cfg):
    g = load_gold()["dim_date"]
    return save(spark, cfg, "dim_date", dim_date(spark, g["start"], g["end"], int(g["fiscal_year_start_month"])))


def dim_hospital(spark, cat):
    h = spark.table(f"{cat}.silver.hospitals")
    df = h.select(skey("hospital_id").alias("hospital_key"), "hospital_id", "hospital_name", "city", "state",
                  "beds", "opened_date", (~F.col("is_deleted")).alias("is_active"))
    return with_unknown(spark, df, "hospital_key", {"hospital_id": "unknown", "hospital_name": "unknown"})


def dim_doctor(spark, cat):
    """History of each doctor: specialty, status and department (no name or phone in Gold)."""
    d = history(spark.table(f"{cat}.silver.doctors"), "doctor_id")
    dept = spark.table(f"{cat}.silver.departments").select("dept_id", F.col("dept_name").alias("department"), "hospital_id")
    df = (d.join(dept, "dept_id", "left")
           .select(skey("doctor_id", "valid_from").alias("doctor_key"), "doctor_id", "specialty", "status",
                   "dept_id", "department", "hospital_id", "valid_from", "valid_to", "is_current"))
    return with_unknown(spark, df, "doctor_key", {"doctor_id": "unknown", "specialty": "unknown", "department": "unknown"})


def dim_patient(spark, cat):
    """History of each patient without name, phone, national id or date of birth.
    Age is not here: the fact carries the age band at the time of each event."""
    p = history(spark.table(f"{cat}.silver.patients"), "patient_id")
    df = p.select(skey("patient_id", "valid_from").alias("patient_key"), "patient_id", "gender", "city",
                  "insurance_provider", "id_fingerprint", "valid_from", "valid_to", "is_current")
    return with_unknown(spark, df, "patient_key", {"patient_id": "unknown", "gender": "unknown", "city": "unknown"})


DIMENSIONS = {"bhg": {"dim_hospital": dim_hospital, "dim_doctor": dim_doctor, "dim_patient": dim_patient}}


def build_dimensions(spark, cfg):
    """Build this company's dimensions. Returns {table: rows}."""
    return {name: save(spark, cfg, name, fn(spark, cfg["catalog"]))
            for name, fn in DIMENSIONS.get(cfg["code"], {}).items()}

