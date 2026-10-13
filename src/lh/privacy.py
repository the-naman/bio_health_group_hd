"""Personal data protection: Unity Catalog column masks and row filters driven by config/privacy.yml.

A mask is a SQL function attached to a column. Whoever reads the column gets the function's
result: the real value for the groups listed under `clear`, the masked value for everyone else.
The data in the table is never changed, so the pipeline keeps working on real values.

  last4   -> XXXXXXXX9012          initial -> A***
  token   -> PAY#7F3A21C4 (same input, same token: joins and counts still work)
  hide    -> NULL (one function per data type, because a mask must return the column's type)

A row filter is a SQL function attached to a table. It decides per row whether the reader sees it:
a limited group sees only the rows of its listed ids (e.g. its hospitals), everyone else every row.
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


def bronze_names():
    """{table: {silver column: bronze column}}, taken from the renames in config/silver.yml."""
    with open(os.path.join(common.repo_root(), "config", "silver.yml")) as f:
        tables = yaml.safe_load(f)["tables"]
    return {t: {c: (r[1] if isinstance(r, list) and len(r) > 1 else c) for c, r in spec["columns"].items()}
            for t, spec in tables.items()}


def plan(p, env, layer, columns, renames):
    """Which columns of one layer get which mask: [(table, column, mask type)].

    columns = {table: {column: data type}} as the tables are today. Order of the rules: ids
    (only in the listed environments), then personal columns, then technical columns.
    A personal column that is missing in silver is an error; in bronze it is skipped
    (a PDF or image table has no such column, its bytes are covered by `content`)."""
    ids = p.get("ids", {})
    id_columns = ids.get("columns", []) if env in ids.get("environments", []) else []
    if layer == "silver":
        for table, wanted in p["personal"].items():
            if table.startswith(("dim_", "fact_")): continue          # gold tables are checked in gold
            lost = [c for c in wanted if c not in columns.get(table, {})]
            if lost:
                raise ValueError(f"privacy.yml: silver.{table} has no column {lost}")
    out = []
    for table in sorted(columns):
        bronze = renames.get(table, {}) if layer == "bronze" else {}
        rules = {bronze.get(c, c): "token" for c in id_columns}
        rules.update({bronze.get(c, c): kind for c, kind in p["personal"].get(table, {}).items()})
        rules.update({c: "hide" for c in p.get("technical", [])})
        out += [(table, c, kind) for c, kind in rules.items() if c in columns[table]]
    return out


def columns_of(spark, cat, layer):
    rows = spark.sql(f"""
        SELECT c.table_name, c.column_name, c.full_data_type
        FROM {cat}.information_schema.columns c
        JOIN {cat}.information_schema.tables t
          ON c.table_schema = t.table_schema AND c.table_name = t.table_name
        WHERE c.table_schema = '{layer}' AND t.table_type IN ('MANAGED', 'EXTERNAL')""").collect()
    out = {}
    for r in rows:
        out.setdefault(r[0], {})[r[1]] = r[2]
    return out


def masks_now(spark, cat):
    """{(schema, table, column): mask function name} for every mask in the catalog."""
    rows = spark.sql(f"""SELECT table_schema, table_name, column_name, mask_name
                         FROM {cat}.information_schema.column_masks""").collect()
    return {(r[0], r[1], r[2]): r[3].split(".")[-1] for r in rows}


def status(spark, cfg, env):
    """One row per column that must be masked: the function wanted and the function attached now."""
    p, cat = load(), cfg["catalog"]
    renames, have = bronze_names(), masks_now(spark, cat)
    rows = []
    for layer in p["layers"]:
        columns = columns_of(spark, cat, layer)
        for table, column, kind in plan(p, env, layer, columns, renames):
            dtype = columns[table][column]
            rows.append({"layer": layer, "table": table, "column": column, "kind": kind, "dtype": dtype,
                         "wanted": function_name(cfg, kind, dtype).split(".")[-1],
                         "actual": have.get((layer, table, column))})
    return rows


def apply(spark, cfg, env):
    """Attach the masks. Safe to run again: a column that already has the right mask is left alone."""
    cat = cfg["catalog"]
    ready = {r[0] for r in spark.sql(
        f"SELECT routine_name FROM {cat}.information_schema.routines WHERE routine_schema = 'ops'").collect()}
    done = {"set": 0, "kept": 0}
    for r in status(spark, cfg, env):
        if r["actual"] == r["wanted"]:
            done["kept"] += 1
            continue
        if r["wanted"] not in ready:
            create_function(spark, cfg, env, r["kind"], r["dtype"])
            ready.add(r["wanted"])
        target = f"{cat}.{r['layer']}.{r['table']}"
        if r["actual"]:
            spark.sql(f"ALTER TABLE {target} ALTER COLUMN `{r['column']}` DROP MASK")
        spark.sql(f"ALTER TABLE {target} ALTER COLUMN `{r['column']}` SET MASK {cat}.ops.{r['wanted']}")
        done["set"] += 1
    done.update({f"rows_{k}": v for k, v in apply_row_filters(spark, cfg, env).items()})
    return done


def create_row_filter(spark, cfg, env, table, spec, prefix="rf_", clear=None):
    """Create (or replace) the row filter function of one table and return its full name.
    Clear groups and the job account see every row; a limited group sees only the rows whose key
    is the gold key (xxhash64) of one of its ids; any other reader sees every row."""
    rules = []
    for group, ids in spec["limited"].items():
        if not re.match(r"^[a-z_]+$", group):
            raise ValueError(f"group name not allowed: {group}")
        bad = [i for i in ids if not re.match(r"^[A-Za-z0-9_-]+$", i)]
        if bad:
            raise ValueError(f"id not allowed: {bad}")
        keys = ", ".join(f"xxhash64('{i}')" for i in ids)
        rules.append(f"WHEN is_account_group_member('{group}') THEN " + (f"k IN ({keys})" if ids else "FALSE"))
    name = f"{cfg['catalog']}.ops.{prefix}{table}"
    spark.sql(f"""
        CREATE OR REPLACE FUNCTION {name}(k BIGINT) RETURNS BOOLEAN
        RETURN CASE WHEN {clear_sql(spark, env, clear)} THEN TRUE {' '.join(rules)} ELSE TRUE END""")
    return name


def row_filters_now(spark, cat):
    """{table: filter function name} for every row filter on a gold table."""
    rows = spark.sql(f"""SELECT table_name, filter_name FROM {cat}.information_schema.row_filters
                         WHERE table_schema = 'gold'""").collect()
    return {r[0]: r[1].split(".")[-1] for r in rows}


def apply_row_filters(spark, cfg, env):
    """Attach the row filters of privacy.yml to the gold tables that exist. Safe to run again.
    The function is replaced on every run, so a change in privacy.yml takes effect at once."""
    p, cat = load(), cfg["catalog"]
    tables = {r[0] for r in spark.sql(f"""SELECT table_name FROM {cat}.information_schema.tables
                                          WHERE table_schema = 'gold'""").collect()}
    have = row_filters_now(spark, cat)
    done = {"set": 0, "kept": 0}
    for table, spec in p.get("row_filters", {}).items():
        if table not in tables:
            continue
        name = create_row_filter(spark, cfg, env, table, spec)
        if have.get(table) == name.split(".")[-1]:
            done["kept"] += 1
            continue
        target = f"{cat}.gold.{table}"
        if table in have:
            spark.sql(f"ALTER TABLE {target} DROP ROW FILTER")
        spark.sql(f"ALTER TABLE {target} SET ROW FILTER {name} ON (`{spec['column']}`)")
        done["set"] += 1
    return done

