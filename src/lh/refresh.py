"""Controlled refresh (5.11): a safe copy of real-looking data from a higher environment to a lower one
(prod -> test, test -> dev), driven by config/refresh.yml. Same file in both repos.

It works on Silver (clean names, no source drift), in two halves that run as two different job accounts:
  out (source job account): picks a sample of people, follows the parent links of config/silver.yml so every
      row of a picked person comes along (visits, invoices, loans, repayments ...) plus all reference rows
      (hospitals, branches, products), fakes the personal columns, re-keys the id columns of config/privacy.yml
      and writes the result as files into the TARGET's ops.refresh_in volume. Real values never leave the source.
  in  (target job account): adds those rows to the target's Silver tables (the target's own rows stay and keep
      linking; re-keyed ids never clash with them) and remakes id_fingerprint with the target's own key; the
      target's normal run then rebuilds id maps, reconciliation and Gold.
People are picked by a keyed hash of their national id, so the same person is picked in BHG and in FQF, and
ids are re-keyed with one shared key, so links between the two companies (bhg_invoice_id) still match.
Amounts are kept. The key comes from the secret scope lh_refresh (loaded by the refresh workflow), never Git.
"""

import json
import os

import yaml
from pyspark import cloudpickle
from pyspark.sql import functions as F

from lh import common

cloudpickle.register_pickle_by_value(common)
TECHNICAL = ("raw_row", "_rescued_data", "content")


def _yaml(name):
    with open(os.path.join(common.repo_root(), "config", name)) as f:
        return yaml.safe_load(f)


def load():
    """(refresh settings, {table: (parents, key)} from silver.yml, id columns from privacy.yml)."""
    s = _yaml("silver.yml")["tables"]
    return (
        _yaml("refresh.yml"),
        {t: (v.get("parents") or {}, v["key"]) for t, v in s.items()},
        _yaml("privacy.yml")["ids"]["columns"],
    )


def flow(rcfg, target):
    """(source env, sample percent) for a target env; only the flows in config/refresh.yml are allowed."""
    f = rcfg["flows"].get(target)
    if not f:
        raise ValueError(f"no refresh into {target} (allowed: {sorted(rcfg['flows'])})")
    return f["from"], int(f["pct"])


def _h(key, kind, col):
    return F.sha2(F.concat_ws("|", F.lit(key), F.lit(kind), col.cast("string")), 256)


def _num(key, kind, col, n):
    """n digits taken from a keyed hash of col."""
    return F.lpad(
        (F.conv(F.substring(_h(key, kind, col), 1, 15), 16, 10).cast("long") % (10**n)).cast("string"), n, "0"
    )


def token(key, col):
    """Same input + same key -> same new id (letters prefix kept, then R + 10 hex), so joins still work."""
    v = col.cast("string")
    return F.when(v.isNull(), F.lit(None).cast("string")).otherwise(
        F.concat(F.regexp_extract(v, r"^[A-Za-z_]*", 0), F.lit("R"), F.upper(F.substring(_h(key, "id", v), 1, 10)))
    )


def fake(key, kind, col, dtype):
    """Personal value -> believable fake. Same real value -> same fake (in both companies)."""
    digits = F.regexp_replace(col.cast("string"), r"\D", "")
    if kind == "name":
        out = F.concat(F.lit("Person "), F.upper(F.substring(_h(key, "name", col), 1, 6)))
    elif kind == "aadhaar":  # starts with 0: never a real Aadhaar
        out = F.concat(F.lit("0"), _num(key, "aadhaar", digits, 11))
    elif kind == "phone":  # starts with 0: never a real mobile number
        out = F.concat(F.lit("0"), _num(key, "phone", F.substring(digits, -10, 10), 9))
    elif kind == "date":  # moved by up to +-6 months
        shift = (F.conv(F.substring(_h(key, "date", col), 1, 6), 16, 10).cast("int") % 365) - 182
        out = F.date_add(col.cast("date"), shift)
    elif kind == "round":  # nearest 5,000
        out = F.round(col.cast("double") / 5000) * 5000
    elif kind == "token":
        out = token(key, col)
    else:
        raise ValueError(f"unknown fake kind {kind}")
    return F.when(col.isNull(), None).otherwise(out).cast(dtype)


def order(parents):
    """Tables with their parents first."""
    done, out = set(), []
    while len(out) < len(parents):
        ready = [t for t, (p, _) in parents.items() if t not in done and set(p.values()) - {t} <= done]
        if not ready:
            raise ValueError("loop in silver.yml parents")
        out += sorted(ready)
        done |= set(ready)
    return out


def linked(parents, person):
    """Tables whose rows belong to a person (directly or through their parents)."""
    out, grew = {person}, True
    while grew:
        grew = False
        for t, (p, _) in parents.items():
            if t not in out and set(p.values()) & out:
                out.add(t)
                grew = True
    return out


def transform(df, table, key, rcfg, ids):
    fakes = (rcfg.get("fake") or {}).get(table, {})
    cols = []
    for c, dtype in df.dtypes:
        if c in TECHNICAL or c == "id_fingerprint":  # fingerprint is remade in the target with its own key
            cols.append(F.lit(None).cast(dtype).alias(c))
        elif c in fakes:
            cols.append(fake(key, fakes[c], F.col(c), dtype).alias(c))
        elif c in ids:
            cols.append(token(key, F.col(c)).alias(c))
        else:
            cols.append(F.col(c))
    return df.select(cols)


def out(spark, source_catalog, dest, key, pct, rcfg, parents, ids, read=None):
    """Write the sanitised sample of every Silver table to dest/<table> (parquet). Returns {table: rows}."""
    read = read or (lambda t: spark.table(f"{source_catalog}.silver.{t}"))
    person = rcfg["person"]
    pt, pkey, by = person["table"], person["key"], person["sample_by"]
    mine = linked(parents, pt)
    counts = {}
    for t in order(parents):
        p, tkey = parents[t]
        df = read(t)
        if t == pt:
            per = df.groupBy(pkey).agg(F.max(F.regexp_replace(F.col(by).cast("string"), r"\D", "")).alias("_by"))
            pick = per.select(
                pkey,
                (
                    F.conv(
                        F.substring(
                            _h(
                                key,
                                "sample",
                                F.coalesce(F.when(F.col("_by") != "", F.col("_by")), F.col(pkey).cast("string")),
                            ),
                            1,
                            8,
                        ),
                        16,
                        10,
                    ).cast("long")
                    % 100
                ).alias("_b"),
            )
            df = df.join(pick.filter(F.col("_b") < pct).select(pkey), pkey, "left_semi")
        elif t in mine:
            links = [(c, pt_) for c, pt_ in p.items() if pt_ in mine]
            ok = []
            for i, (c, parent) in enumerate(links):
                pk = parents[parent][1][0]
                kept = spark.read.parquet(f"{dest}/{parent}").select(F.col(pk).alias(f"_k{i}")).distinct()
                df = df.join(kept, token(key, F.col(c)) == F.col(f"_k{i}"), "left")
                ok.append(F.col(c).isNull() | F.col(f"_k{i}").isNotNull())
            any_link = (
                F.greatest(*[F.col(c).isNotNull() for c, _ in links])
                if len(links) > 1
                else F.col(links[0][0]).isNotNull()
            )
            cond = any_link
            for o in ok:
                cond = cond & o
            df = df.filter(cond).drop(*[f"_k{i}" for i in range(len(links))])
        transform(df, t, key, rcfg, ids).write.mode("overwrite").parquet(f"{dest}/{t}")
        counts[t] = spark.read.parquet(f"{dest}/{t}").count()
    return counts


def load_in(spark, target_catalog, src, fp_key, rcfg, tables, keys, write=None):
    """Add the refreshed rows in src/<table> to the target's Silver tables; remake id_fingerprint with the target key.
    Rows whose key is already there are skipped (reference rows such as hospitals keep their ids), so the target's own
    data stays and goes on linking, and a second load of the same run adds nothing. Columns are matched by name.
    Returns {table: (rows in the files, rows added)}."""
    make = common.fingerprint
    fp = F.udf(lambda v: make(v, fp_key) if v else None, "string")
    person = rcfg["person"]
    counts = {}
    for t in tables:
        df = spark.read.parquet(f"{src}/{t}")
        read = df.count()
        if t == person["table"] and "id_fingerprint" in df.columns:
            df = df.withColumn("id_fingerprint", fp(F.col(person["sample_by"])))
        target = f"{target_catalog}.silver.{t}"
        have = spark.table(target)
        df = df.select([(F.col(c) if c in df.columns else F.lit(None)).cast(d).alias(c) for c, d in have.dtypes])
        df = df.join(have.select(*keys[t]).distinct(), keys[t], "left_anti").select(have.columns)
        added = df.count()
        (write or (lambda d, name: d.write.insertInto(name)))(df, target)
        counts[t] = (read, added)
    return counts


def manifest(source, target, pct, counts):
    return json.dumps({"from": source, "to": target, "pct": pct, "rows": counts}, sort_keys=True)
