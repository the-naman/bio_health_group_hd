"""Silver: cleaning rules as Spark column expressions (no Python UDFs, safe in ANSI mode).

Every cleaner takes a text column and returns a clean column; a value that cannot be
read becomes NULL, never an error. apply() runs the cleaners named in config/silver.yml
and adds `_bad`: the list of columns that had a value which could not be read.
"""
import os

import yaml
from pyspark.sql import functions as F

from lh import common

FAKE_NULLS = ["", "null", "n/a", "na", "none", "-"]
CITY_ALIASES = {
    "bangalore": "Bengaluru", "blr": "Bengaluru", "bengaluru": "Bengaluru",
    "bombay": "Mumbai", "mumbai": "Mumbai",
    "new delhi": "Delhi", "delhi": "Delhi",
    "madras": "Chennai", "chennai": "Chennai",
    "hyd": "Hyderabad", "hyderabad": "Hyderabad",
}
DAY, MON = r"(0[1-9]|[12]\d|3[01])", r"(0[1-9]|1[0-2])"
# (pattern the text must match, format used to read it). The pattern is checked first, so the
# parser only ever sees text of the right shape. Order matters for 03/04/2026: day first (India).
DATE_FORMATS = [(r"^\d{4}-" + MON + "-" + DAY + "$", "yyyy-MM-dd"),
                ("^" + DAY + "/" + MON + r"/\d{4}$", "dd/MM/yyyy"),
                ("^" + DAY + r"-[A-Za-z]{3}-\d{4}$", "dd-MMM-yyyy"),
                ("^" + DAY + "-" + MON + r"-\d{4}$", "dd-MM-yyyy"),
                ("^" + MON + "/" + DAY + r"/\d{4}$", "MM/dd/yyyy")]
CLOCK = r"\d{2}:\d{2}:\d{2}$"
TS_FORMATS = [(r"^\d{4}-\d{2}-\d{2} " + CLOCK, "yyyy-MM-dd HH:mm:ss"),
              (r"^\d{4}-\d{2}-\d{2}T" + CLOCK, "yyyy-MM-dd'T'HH:mm:ss"),
              (r"^\d{2}-\d{2}-\d{4} " + CLOCK, "dd-MM-yyyy HH:mm:ss"),
              (r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd")]
META = ["_source_file", "_business_date", "_ingest_ts"]


def load_silver():
    with open(os.path.join(common.repo_root(), "config", "silver.yml")) as f:
        return yaml.safe_load(f)["tables"]


def text(c):
    """Trim, collapse spaces, turn fake nulls (NULL, N/A, -) into real NULL."""
    t = F.regexp_replace(F.trim(c), r"\s+", " ")
    return F.when(F.lower(t).isin(FAKE_NULLS), None).otherwise(t)


def name(c):
    return F.initcap(text(c))


def code(c):
    return F.lower(text(c))


def city(c):
    lookup = F.create_map(*[F.lit(x) for pair in CITY_ALIASES.items() for x in pair])
    t = text(c)
    return F.coalesce(F.try_element_at(lookup, F.lower(t)), F.initcap(t))


def phone(c):
    """10 digit Indian mobile number; +91, 91 and a leading 0 are removed."""
    d = F.regexp_replace(text(c), r"\D", "")
    d = F.when((F.length(d) == 12) & d.startswith("91"), F.substring(d, 3, 10)) \
         .when((F.length(d) == 11) & d.startswith("0"), F.substring(d, 2, 10)).otherwise(d)
    return F.when(d.rlike(r"^[6-9]\d{9}$"), d)


def gender(c):
    t = code(c)
    return F.when(t.isin("m", "male"), "M").when(t.isin("f", "female"), "F").when(t.isNotNull(), "O")


def aadhaar(c):
    """12 digits starting 0 or 1 (the synthetic rule of this project), else NULL."""
    d = F.regexp_replace(text(c), r"\D", "")
    return F.when(d.rlike(r"^[01]\d{11}$"), d)


def integer(c):
    d = F.regexp_replace(text(c), r"[,\s]", "")
    return F.when(d.rlike(r"^-?\d{1,9}$"), d.cast("int"))


def decimal(c):
    """Money as decimal(18,2); currency signs and thousands separators are removed."""
    d = F.regexp_replace(text(c), r"[^0-9.\-]", "")
    return F.when(d.rlike(r"^-?\d{1,16}(\.\d+)?$"), d.cast("decimal(18,2)"))


def _parse(t, formats):
    return F.coalesce(*[F.when(t.rlike(p), F.try_to_timestamp(t, F.lit(f))) for p, f in formats])


def date(c):
    t = text(c)
    t = F.when(t.rlike(r"^\d{4}-\d{2}-\d{2}[ T]"), F.substring(t, 1, 10)).otherwise(t)   # timestamp sent in a date column
    return _parse(t, DATE_FORMATS).cast("date")


def timestamp(c):
    return _parse(text(c), TS_FORMATS)


CLEANERS = {"string": text, "name": name, "code": code, "city": city, "phone": phone, "gender": gender,
            "aadhaar": aadhaar, "int": integer, "decimal": decimal, "date": date, "timestamp": timestamp}


def apply(df, spec):
    """Bronze rows -> silver columns for one table (config entry `spec`).

    Adds: is_deleted (from the source delete flag), _bad (columns whose value could not be read),
    and keeps the bronze metadata columns and id_fingerprint when present.
    """
    cols, bad = [], []
    for target, rule in spec["columns"].items():
        kind, source = (rule, target) if isinstance(rule, str) else rule
        raw = F.col(f"`{source}`")
        cols.append(CLEANERS[kind](raw).alias(target))
        if kind != "string":
            bad.append(F.when(text(raw).isNotNull() & CLEANERS[kind](raw).isNull(), F.lit(target)))
    flag = spec.get("delete")
    deleted = (F.lower(F.trim(F.col(f"`{flag['column']}`"))) == str(flag["value"]).lower()) if flag else F.lit(False)
    cols.append(F.coalesce(deleted, F.lit(False)).alias("is_deleted"))
    cols.append(F.array_compact(F.array(*bad)).alias("_bad") if bad else F.array().cast("array<string>").alias("_bad"))
    keep = [c for c in META + ["id_fingerprint"] if c in df.columns]
    return df.select(*cols, *keep)