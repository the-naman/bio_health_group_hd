"""Small-file compaction (same file in both repos). Tables written in many small appends (run logs, the payment
stream) end up as many tiny files, which slows every read. The BI step calls compact() once a day; it runs OPTIMIZE
only on the tables of config/performance.yml that pass the limits, so a normal day costs one DESCRIBE per table.

    compact(spark, cfg) -> ([(table, files before, files after)], [(table, error)])
A table that cannot be compacted (for example a rule of a masked table) never stops the job; it is reported.
"""

import os

import yaml

from lh import common


def settings():
    with open(os.path.join(common.repo_root(), "config", "performance.yml")) as f:
        return yaml.safe_load(f)["compact"]


def needs(files, size_bytes, s):
    """True when a table has enough files and they are small on average."""
    return files >= int(s["min_files"]) and size_bytes / max(files, 1) < float(s["max_avg_mb"]) * 1048576


def compact(spark, cfg, s=None):
    s = s or settings()
    out, failed = [], []
    for name in s["tables"]:
        full = f"{cfg['catalog']}.{name}"
        if not spark.catalog.tableExists(full):
            continue
        d = spark.sql(f"DESCRIBE DETAIL {full}").first()
        files, size = int(d["numFiles"] or 0), int(d["sizeInBytes"] or 0)
        if needs(files, size, s):
            try:
                spark.sql(f"OPTIMIZE {full}")
            except Exception as e:  # noqa: BLE001 - compaction is housekeeping; the job must not fail for it
                failed.append((name, type(e).__name__))
                continue
            after = int(spark.sql(f"DESCRIBE DETAIL {full}").first()["numFiles"] or 0)
            out.append((name, files, after))
    return out, failed
