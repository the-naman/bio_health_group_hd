"""Gold in SQL (5.5s). The business logic of every Gold table is one plain SELECT in gold/sql/<name>.sql,
written so a manager or analyst can read it. This module only runs those files, in the order of the config:

  - the current catalog is set first, so a file says silver.patients, not bhg_dev.silver.patients
  - named parameters (:start_date, :fqf_catalog, ...) are passed safely as values, never pasted into the text
  - a file whose name starts with "_" is a helper step: its result becomes a temporary view that later files
    read (for example _bhg_events); it is never saved
  - every other file is saved as <catalog>.gold.<table> by gold.save (full rebuild; masks and row filters stay)
Same file in both repos."""
import os
import re

from lh import common, gold

PARAM = re.compile(r"(?<![:\w]):([A-Za-z_]\w*)")
COMMENT = re.compile(r"--[^\n]*")
STRING = re.compile(r"'(?:[^'\\]|\\.)*'")


def path(name):
    return os.path.join(common.repo_root(), "gold", "sql", f"{name}.sql")


def text(name):
    with open(path(name)) as f:
        return f.read()


def params_used(sql):
    """Named parameters in a SQL text (comments and quoted text ignored)."""
    return set(PARAM.findall(STRING.sub("''", COMMENT.sub("", sql))))


def query(spark, cfg, name, params=None):
    """The DataFrame of gold/sql/<name>.sql, run in this environment's catalog."""
    sql, params = text(name), params or {}
    used = params_used(sql)
    missing = sorted(used - set(params))
    if missing:
        raise ValueError(f"{name}.sql needs parameter(s) {missing}")
    spark.catalog.setCurrentCatalog(cfg["catalog"])
    return spark.sql(sql, args={k: v for k, v in params.items() if k in used} or None)


def run(spark, cfg, names, params=None):
    """Run the files in order. Returns {table: rows} for the saved tables."""
    out = {}
    for name in names:
        table = os.path.basename(name)
        df = query(spark, cfg, name, params)
        if table.startswith("_"):
            df.createOrReplaceTempView(table)
        else:
            out[table] = gold.save(spark, cfg, table, df)
    return out


def company(spark, cfg):
    """This company's Gold: dim_date, dimensions, facts (config/gold.yml, sql_build)."""
    g = gold.load_gold()
    spark.udf.register("contact_fingerprint", gold.contact_fingerprint_udf(cfg["catalog"]))
    d = g["dim_date"]
    params = {"start_date": str(d["start"]), "end_date": str(d["end"]),
              "fiscal_start_month": int(d["fiscal_year_start_month"])}
    return run(spark, cfg, g["sql_build"][cfg["code"]], params)


def group_tables(spark, cfg, env):
    """The group Gold of the parent (config/group.yml, sql_build). Reads the FQF Gold of the same environment."""
    from lh import group
    grp, src = group.load_group(), group.source(cfg, env)
    group.decisions(spark, cfg["catalog"])                    # review decisions table + Manager grant (rerun-safe)
    rows = [(c["code"], c["name"], c["role"], c["industry"]) for c in grp["companies"]]
    (spark.createDataFrame(rows, "company_code string, company_name string, role string, industry string")
          .createOrReplaceTempView("_cfg_companies"))
    params = {"fqf_catalog": src, "ic_payer_type": grp["intercompany"]["bhg_payer_type"]}
    return run(spark, cfg, grp["sql_build"], params)
