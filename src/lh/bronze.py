"""Bronze: load new landing files into <catalog>.bronze.<table>, kept exactly as the source sends them.

All source columns stay text. Added columns: _source_file, _business_date, _ingest_ts,
_rescued_data (anything that does not fit, e.g. a new column) and id_fingerprint where configured.
Auto Loader keeps one checkpoint per table, so a file is never loaded twice.
"""
import os

import yaml
from pyspark import cloudpickle
from pyspark.sql import functions as F

from lh import common

cloudpickle.register_pickle_by_value(common)      # lets common.fingerprint run on the Spark workers


def load_tables():
    with open(os.path.join(common.repo_root(), "config", "tables.yml")) as f:
        return yaml.safe_load(f)["tables"]


def _reader(spark, spec, schema_dir):
    fmt = spec["format"]
    r = spark.readStream.format("cloudFiles")
    if fmt == "binary":                            # pdf, image: bytes + path, length, modificationTime
        return r.option("cloudFiles.format", "binaryFile")
    r = (r.option("cloudFiles.schemaLocation", schema_dir)
          .option("cloudFiles.inferColumnTypes", "false")          # everything as text
          .option("cloudFiles.schemaEvolutionMode", "rescue"))     # new columns go to _rescued_data
    if fmt == "json":
        return r.option("cloudFiles.format", "json")
    # drifting headers: read without a header, the header line stays as a data row for silver
    r = r.option("cloudFiles.format", "csv").option("header", "false" if spec.get("drift") else "true")
    if fmt == "pipe":
        return r.option("sep", "|").option("quote", "")
    return r.option("escape", '"')


def load_table(spark, cfg, table, spec, key):
    """Load every new file of one table. Returns the number of rows added."""
    catalog = cfg["catalog"]
    source = f"/Volumes/{catalog}/bronze/landing/{spec['source']}/{table}/"
    state = f"/Volumes/{catalog}/ops/checkpoints/bronze/{table}"
    target = f"{catalog}.bronze.{table}"
    before = spark.table(target).count() if spark.catalog.tableExists(target) else 0

    df = _reader(spark, spec, state + "/schema").load(source)
    df = (df.withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_business_date",
                        F.to_date(F.regexp_extract("_source_file", r"_(\d{8})_\d{2}\.\w+$", 1), "yyyyMMdd"))
            .withColumn("_ingest_ts", F.current_timestamp()))
    if spec.get("fingerprint"):
        make = common.fingerprint
        fp = F.udf(lambda v: make(v, key) if v else None, "string")
        df = df.withColumn("id_fingerprint", fp(F.col(spec["fingerprint"])))

    query = (df.writeStream.option("checkpointLocation", state + "/checkpoint")
               .trigger(availableNow=True).toTable(target))
    query.awaitTermination()
    return spark.table(target).count() - before