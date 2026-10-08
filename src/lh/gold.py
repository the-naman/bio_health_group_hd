"""Gold: the star schema that business users and BI read.

  dim_date            one row per calendar day, same in every catalog (date_key = yyyymmdd)
  dimensions, facts   added in the next steps

Every Gold table is rebuilt in full from Silver on each run. Silver holds the history, so Gold
needs none of its own, and a rerun always gives the same result.
"""
import os

import yaml
from pyspark.sql import functions as F

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
    