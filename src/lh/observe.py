"""5.12 observability helper (same file in both repos), run by bi/01_create_views in every daily job.

rescued_keys()  Auto Loader keeps a source column it does not know in Bronze `_rescued_data` (Phase 4: a new column
                lands there and reaches Silver only through a reviewed PR). This writes a snapshot of the column NAMES
                found there to ops.rescued_keys, one row per table and name, with planned = listed under
                `from_rescued` in config/silver.yml. Values are never copied. ci/alerts.py mails a new, unplanned
                name once (view gold.v_health_rescued).
"""

import os
import yaml
from pyspark.sql import functions as F
from lh import common

SCHEMA = "table_name string, key string, rows long, first_seen timestamp, last_seen timestamp, planned boolean"


def _yml(name):
    with open(os.path.join(common.repo_root(), "config", name)) as f:
        return yaml.safe_load(f)


def rescued_keys(spark, cfg):
    """Rebuild ops.rescued_keys from every Bronze table that has `_rescued_data`. Returns the rows written."""
    cat = cfg["catalog"]
    planned = {t: set(s.get("from_rescued", [])) for t, s in _yml("silver.yml")["tables"].items()}
    parts = []
    for table in _yml("tables.yml")["tables"]:
        full = f"{cat}.bronze.{table}"
        if not spark.catalog.tableExists(full):
            continue
        df = spark.table(full)
        if "_rescued_data" not in df.columns:  # pdf / image tables, or a table read without rescue
            continue
        parts.append(
            df.where("_rescued_data IS NOT NULL").select(
                F.lit(table).alias("table_name"),
                F.explode(F.expr("json_object_keys(_rescued_data)")).alias("key"),
                F.col("_ingest_ts"),
            )
        )
    out = spark.createDataFrame([], SCHEMA)
    if parts:
        rows = parts[0]
        for p in parts[1:]:
            rows = rows.unionByName(p)
        known = (
            F.create_map(*[F.lit(x) for t, keys in planned.items() for k in keys for x in (f"{t}.{k}", True)])
            if any(planned.values())
            else None
        )
        out = (
            rows.where("key <> '_file_path'")  # Auto Loader adds the file path itself
            .groupBy("table_name", "key")
            .agg(
                F.count("*").alias("rows"),
                F.min("_ingest_ts").alias("first_seen"),
                F.max("_ingest_ts").alias("last_seen"),
            )
            .withColumn(
                "planned",
                F.coalesce(known[F.concat_ws(".", "table_name", "key")], F.lit(False))
                if known is not None
                else F.lit(False),
            )
            .select(*[c.split()[0] for c in SCHEMA.split(", ")])
        )
    out.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{cat}.ops.rescued_keys")
    return spark.table(f"{cat}.ops.rescued_keys").count()
