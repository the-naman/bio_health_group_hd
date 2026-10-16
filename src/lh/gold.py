"""Gold: the star schema that business users and BI read.

Since 5.5s the logic of every Gold table is SQL: one SELECT per table in gold/sql/<name>.sql (company
tables) and gold/sql/group/<name>.sql (group tables, BHG repo only), run in the order of config/gold.yml
and config/group.yml by lh/gold_sql.py. This module keeps what those files need from Python:

  load_gold()                 the Gold settings (config/gold.yml)
  save()                      full rebuild of one Gold table; column masks and row filters stay attached
  contact_fingerprint_udf()   keyed hash of phone + date of birth, registered for SQL as contact_fingerprint()
                              (it needs the group match key from the secret scope, so it cannot be plain SQL)

Keys: a surrogate key is a 64-bit hash (xxhash64) of the natural key (plus valid_from for a version), so it
is the same on every rebuild. Key -1 is the "unknown" member that a fact points to when the parent is missing.
Every Gold table is rebuilt in full from Silver on each run: Silver holds the history, so a rerun always
gives the same result.
"""
import hashlib
import hmac
import os
import re

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


def contact_fingerprint_udf(cat):
    """Keyed hash of phone (last 10 digits) + date of birth, made with the same group match key as
    id_fingerprint. Two companies can then spot the same contact details without either one
    showing a phone number or a date of birth. Registered for SQL as contact_fingerprint(phone, dob)."""
    key = common.match_key(cat.rsplit("_", 1)[1])          # catalog bhg_dev -> scope lh_dev

    def fp(phone, dob):
        digits = re.sub(r"\D", "", str(phone or ""))[-10:]
        if len(digits) != 10 or dob is None:
            return None
        return hmac.new(key.encode(), f"{digits}|{dob.isoformat()}".encode(), hashlib.sha256).hexdigest()
    return F.udf(fp, "string")
