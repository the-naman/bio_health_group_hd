"""Group consolidation: BHG prod Gold + FQF prod Gold = one group view (BHG repo only).

BHG reads the FQF Gold of the SAME environment, read-only (config/group.yml), so FQF dev or test
data never reaches BHG prod. All group tables live in <bhg catalog>.gold and are rebuilt in full
on every run, like the company Gold tables.

  dim_company          one row per company (parent and child)
"""
import os

import yaml
from pyspark.sql import functions as F

from lh import common, gold


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


def dim_company(spark, grp):
    rows = [(c["code"], c["name"], c["role"], c["industry"]) for c in grp["companies"]]
    df = spark.createDataFrame(rows, "company_code string, company_name string, role string, industry string")
    return df.select(gold.skey("company_code").alias("company_key"), "*")


def build(spark, cfg, env):
    """Build every group table. Returns {table: rows}."""
    grp, src = load_group(), source(cfg, env)
    out = {}
    out["dim_company"] = gold.save(spark, cfg, "dim_company", dim_company(spark, grp))
    return out

