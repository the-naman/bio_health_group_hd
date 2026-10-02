"""Bronze: check, load and file away new landing files, kept exactly as the source sends them.

For each table: a bad file goes to rejected/ with its reason in ops.audit_runs; good files are
loaded into <catalog>.bronze.<table> and then moved to processed/.
All source columns stay text. Added columns: _source_file, _business_date, _ingest_ts,
_rescued_data (anything that does not fit, e.g. a new column) and id_fingerprint where configured.
Auto Loader keeps one checkpoint per table, so a file is never loaded twice.
Files are listed and read by one Spark job and moved in parallel, never one by one through the
volume folder path (that is very slow).
"""
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import yaml
from pyspark import cloudpickle
from pyspark.sql import functions as F

from lh import common

cloudpickle.register_pickle_by_value(common)      # lets common.fingerprint run on the Spark workers

EXT = {"csv": ("csv",), "pipe": ("txt",), "json": ("json",), "binary": ("pdf", "png")}


def load_tables():
    with open(os.path.join(common.repo_root(), "config", "tables.yml")) as f:
        return yaml.safe_load(f)["tables"]


def list_landing(spark, cfg):
    """One Spark job: every landing file as {(source, table): [(path, bytes), ...]}."""
    root = f"/Volumes/{cfg['catalog']}/bronze/landing/"
    try:
        rows = (spark.read.format("binaryFile").option("recursiveFileLookup", "true").load(root)
                .select("path", "content").collect())
    except Exception as e:
        if "PATH_NOT_FOUND" in str(e):
            return {}
        raise
    out = {}
    for r in rows:
        path = r["path"].replace("dbfs:", "", 1)
        parts = path.split("/landing/", 1)[1].split("/")
        if len(parts) >= 3 and not parts[0].startswith("_"):      # skip the simulator's _sim folder
            out.setdefault((parts[0], parts[1]), []).append((path, bytes(r["content"])))
    return out


def check_file(name, data, table, spec, daily_start, daily_loaded):
    """Return None for a good file, else the reason it is rejected."""
    fmt = spec["format"]
    m = re.fullmatch(rf"{table}_(\d{{8}})_\d{{2}}\.(\w+)", name)
    if not m or m.group(2) not in EXT[fmt]:
        return "file name is not <table>_<yyyymmdd>_<nn>.<ext>"
    try:
        day = datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return "file name has an invalid date"
    if daily_loaded and str(day) < daily_start:
        return f"dated before {daily_start} after daily loads began"
    if fmt == "binary":
        if m.group(2) == "pdf":
            ok = data.startswith(b"%PDF-") and data.rstrip().endswith(b"%%EOF")
        else:
            ok = data.startswith(b"\x89PNG\r\n\x1a\n") and data.endswith(b"IEND\xaeB`\x82")
        return None if ok else f"cut off or not a real {m.group(2)} file"
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return "not UTF-8 text"
    if fmt == "json":
        for n, line in enumerate(text.splitlines(), 1):
            if line.strip():
                try:
                    json.loads(line)
                except ValueError:
                    return f"line {n} is not valid JSON"
        return None
    sep = "|" if fmt == "pipe" else ","
    if len(text.split("\n", 1)[0].split(sep)) < 2:
        return f"header is not '{sep}' delimited"
    return None


def _move_all(paths, base, stage):
    from databricks.sdk.runtime import dbutils

    def move(path):
        dbutils.fs.mv(path, path.replace(f"{base}/landing/", f"{base}/{stage}/", 1))

    with ThreadPoolExecutor(8) as pool:
        list(pool.map(move, paths))


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


def load_table(spark, cfg, table, spec, key, landing):
    """Check, load and file away every new file of one table (landing = result of list_landing).
    Returns (rows added, files loaded, files rejected)."""
    files = landing.get((spec["source"], table), [])
    if not files:
        return 0, 0, 0
    catalog = cfg["catalog"]
    base = f"/Volumes/{catalog}/bronze"
    source = f"{base}/landing/{spec['source']}/{table}/"
    state = f"/Volumes/{catalog}/ops/checkpoints/bronze/{table}"
    target = f"{catalog}.bronze.{table}"
    job = f"bronze.{table}"

    before, last = 0, None
    if spark.catalog.tableExists(target):
        before, last = spark.table(target).agg(F.count("*"), F.max("_business_date")).first()
    daily_start = str(cfg["daily_start"])
    daily_loaded = last is not None and str(last) >= daily_start     # full-load phase is over

    good, bad = [], []
    for path, data in files:
        reason = check_file(os.path.basename(path), data, table, spec, daily_start, daily_loaded)
        if reason:
            bad.append(path)
            common.log_run(spark, cfg, job, "rejected", 0, f"{os.path.basename(path)}: {reason}")
        else:
            good.append(path)
    _move_all(bad, base, "rejected")               # out of landing before the load starts
    if not good:
        return 0, 0, len(bad)

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
    rows = spark.table(target).count() - before

    common.log_run(spark, cfg, job, "success", rows, f"{len(good)} files")
    _move_all(good, base, "processed")             # only after the load and the audit row
    return rows, len(good), len(bad)