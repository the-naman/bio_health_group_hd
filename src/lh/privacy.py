"""Personal data protection: Unity Catalog column masks driven by config/privacy.yml.

A mask is a SQL function attached to a column. Whoever reads the column gets the function's
result: the real value for the groups listed under `clear`, the masked value for everyone else.
The data in the table is never changed, so the pipeline keeps working on real values.

  last4   -> XXXXXXXX9012          initial -> A***
  token   -> PAY#7F3A21C4 (same input, same token: joins and counts still work)
  hide    -> NULL (one function per data type, because a mask must return the column's type)
"""
import os
import re

import yaml

from lh import common

MASKED = {      # what a reader outside the clear groups sees; v is the column value (text)
    "last4": "CASE WHEN length(v) > 4 THEN concat(repeat('X', length(v) - 4), right(v, 4)) ELSE repeat('X', length(v)) END",
    "initial": "concat(left(trim(v), 1), '***')",
    "token": "concat(regexp_extract(v, '^[A-Z]{0,4}', 0), '#', upper(left(sha2(v, 256), 8)))",
}
UUID = r"^[0-9a-f]{8}-([0-9a-f]{4}-){3}[0-9a-f]{12}$"


def load():
    with open(os.path.join(common.repo_root(), "config", "privacy.yml")) as f:
        return yaml.safe_load(f)


def clear_sql(spark, env, clear=None):
    """SQL condition that is true for a reader who may see real values."""
    p = load()
    groups = p["clear"][env] if clear is None else clear
    for g in groups:
        if not re.match(r"^[a-z_]+$", g):
            raise ValueError(f"group name not allowed: {g}")
    parts = [f"is_account_group_member('{g}')" for g in groups]
    if clear is None and env in p.get("pipeline", {}):          # test, prod: the job account
        user = spark.sql("SELECT session_user()").first()[0]
        if not re.match(UUID, user):
            raise RuntimeError(f"in {env} the masks must be applied by the job account {p['pipeline'][env]}")
        parts.append(f"session_user() = '{user}'")
    return " OR ".join(parts)


def function_name(cfg, kind, dtype="string", prefix="mask_"):
    if kind == "hide":
        return f"{cfg['catalog']}.ops.{prefix}hide_" + re.sub(r"[^a-z0-9]+", "_", dtype.lower()).strip("_")
    return f"{cfg['catalog']}.ops.{prefix}{kind}"


def create_function(spark, cfg, env, kind, dtype="string", prefix="mask_", clear=None):
    """Create (or replace) one mask function and return its full name."""
    if kind == "hide":
        masked = f"CAST(NULL AS {dtype})"
    elif dtype.lower() != "string":
        raise ValueError(f"mask type {kind} works on text only, not {dtype}")
    else:
        masked = MASKED[kind]
    name = function_name(cfg, kind, dtype, prefix)
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {name}(v {dtype}) RETURNS {dtype}
        RETURN CASE WHEN {clear_sql(spark, env, clear)} THEN v ELSE {masked} END""")
    return name


def create_functions(spark, cfg, env, prefix="mask_", clear=None):
    """The standard set. Other data types for `hide` are added when a table needs them."""
    made = [create_function(spark, cfg, env, k, "string", prefix, clear) for k in MASKED]
    made += [create_function(spark, cfg, env, "hide", t, prefix, clear) for t in ("string", "date")]
    return made
    