"""Data dictionary (same file in both repos): table, column and view descriptions in Unity Catalog.

The text lives in config/dictionary.yml (silver: table about; gold: about + every column; views: about). It shows in
Catalog Explorer, helps Genie and Power BI users, and travels with the data. Gold column descriptions are written
with the data by gold.save; apply() runs in the BI step of the daily job and sets only what still differs (Silver and
table descriptions, the stream table, anything new), so a normal day changes nothing. BI views get
their description when they are created (src/lh/bi.py, view_comment). A comment that cannot be set never stops the
job: it is reported in "failed" and by checks/t33_dictionary.

    apply(spark, cfg) -> {"tables", "columns", "unchanged", "missing", "undocumented", "failed"}
"""

import os
import re

import yaml

from lh import common

SAFE = re.compile(r"^[^'\\\n]{3,300}$")  # one line, no quote or backslash: safe inside a SQL string literal
LAYERS = ("silver", "gold")


def load(path=None):
    """config/dictionary.yml, every description checked against SAFE."""
    with open(path or os.path.join(common.repo_root(), "config", "dictionary.yml")) as f:
        d = yaml.safe_load(f)
    for layer, tables in d.items():
        for t, spec in tables.items():
            for text in [spec.get("about"), *spec.get("columns", {}).values()]:
                if not isinstance(text, str) or not SAFE.match(text):
                    raise ValueError(
                        f"dictionary.yml {layer}.{t}: description must be one line, 3-300 chars, no quotes"
                    )
    return d


def view_comment(name, d):
    """Description of a BI view, or None."""
    spec = d.get("views", {}).get(name)
    return spec["about"] if spec else None


def _run(spark, sql, out, what):
    try:
        spark.sql(sql)
        return True
    except Exception as e:  # noqa: BLE001 - a missing comment must not stop the daily job; t33 reports it
        out["failed"].append(f"{what}: {type(e).__name__}")
        return False


def apply(spark, cfg, d=None):
    """Set table and column comments of Silver and Gold where they differ. Returns counts and lists."""
    d = d or load()
    cat = cfg["catalog"]
    out = {"tables": 0, "columns": 0, "unchanged": 0, "missing": [], "undocumented": [], "failed": []}
    for layer in LAYERS:
        for table, spec in d.get(layer, {}).items():
            full = f"{cat}.{layer}.{table}"
            if not spark.catalog.tableExists(full):
                out["missing"].append(full)  # e.g. the stream tables before the first stream run
                continue
            if (spark.catalog.getTable(full).description or "") != spec["about"]:
                out["tables"] += _run(spark, f"COMMENT ON TABLE {full} IS '{spec['about']}'", out, full)
            else:
                out["unchanged"] += 1
            now = {c.name: c.description or "" for c in spark.catalog.listColumns(full)}
            cols = spec.get("columns", {})
            for col, text in cols.items():
                if col not in now:
                    out["missing"].append(f"{full}.{col}")
                elif now[col] != text:
                    out["columns"] += _run(
                        spark, f"ALTER TABLE {full} ALTER COLUMN `{col}` COMMENT '{text}'", out, f"{full}.{col}"
                    )
                else:
                    out["unchanged"] += 1
            if layer == "gold":
                out["undocumented"] += [f"{table}.{c}" for c in now if c not in cols and not c.startswith("_")]
    return out
