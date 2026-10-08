"""Gold: the star schema that business users and BI read.

  dim_date            one row per calendar day, same in every catalog (date_key = yyyymmdd)
  dim_<entity>        one row per entity (or per version, for entities with history)
  fact_<company>_event  one row per money event. Charges, adjustments and payments of an invoice
                        add up to its net amount, so Gold reconciles to the source.

Keys: a surrogate key is a 64-bit hash of the natural key (plus valid_from for a version), so it is
the same on every rebuild. Key -1 is the "unknown" member that a fact points to when the parent is
missing (an orphan that Silver accepted). The first version of an entity is open from 1900-01-01 and
the last one to 9999-12-31, so every event finds exactly one version.

Every Gold table is rebuilt in full from Silver on each run. Silver holds the history, so Gold
needs none of its own, and a rerun always gives the same result.
"""
import os
from functools import reduce

import yaml
from pyspark.sql import Window, functions as F

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


FAR_PAST, FAR_FUTURE = "1900-01-01 00:00:00", "9999-12-31 23:59:59"


def skey(*cols):
    """Surrogate key: stable 64-bit hash of the given columns."""
    return F.xxhash64(*[F.col(c).cast("string") for c in cols])


def history(df, id_col):
    """Silver SCD2 rows ready for Gold: first version open from the far past, last one to the far future."""
    first = F.row_number().over(Window.partitionBy(id_col).orderBy("valid_from")) == 1
    return (df.withColumn("valid_from", F.when(first, F.lit(FAR_PAST).cast("timestamp")).otherwise(F.col("valid_from")))
              .withColumn("valid_to", F.coalesce(F.col("valid_to"), F.lit(FAR_FUTURE).cast("timestamp"))))


def with_unknown(spark, df, key_col, values):
    """Add the unknown member (key -1): the given values, everything else empty, open for all time."""
    fill = {key_col: -1, "valid_from": FAR_PAST, "valid_to": FAR_FUTURE, "is_current": True, **values}
    row = spark.range(1).select(*[F.lit(fill.get(c)).cast(t).alias(c) for c, t in df.dtypes])
    return df.unionByName(row)


def dim_date(spark, start, end, fiscal_start=4):
    """Calendar from start to end. Financial year FY2026-27 = April 2026 to March 2027."""
    d = F.col("date")
    fy = F.when(F.month(d) >= fiscal_start, F.year(d)).otherwise(F.year(d) - 1)
    return (spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
            .select(F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
                    d,
                    F.year(d).alias("year"),
                    F.quarter(d).alias("quarter"),
                    F.month(d).alias("month"),
                    F.date_format(d, "MMMM").alias("month_name"),
                    F.date_format(d, "yyyy-MM").alias("year_month"),
                    F.dayofmonth(d).alias("day"),
                    (F.weekday(d) + 1).alias("day_of_week"),                      # 1 = Monday ... 7 = Sunday
                    F.date_format(d, "EEEE").alias("day_name"),
                    F.weekofyear(d).alias("week_of_year"),
                    (F.weekday(d) >= 5).alias("is_weekend"),
                    (d == F.last_day(d)).alias("is_month_end"),
                    F.concat(F.lit("FY"), fy.cast("string"), F.lit("-"),
                             F.lpad(((fy + 1) % 100).cast("string"), 2, "0")).alias("fiscal_year"),
                    (F.floor(((F.month(d) - fiscal_start + 12) % 12) / 3) + 1).cast("int").alias("fiscal_quarter")))


def build_dim_date(spark, cfg):
    g = load_gold()["dim_date"]
    return save(spark, cfg, "dim_date", dim_date(spark, g["start"], g["end"], int(g["fiscal_year_start_month"])))


def dim_hospital(spark, cat):
    h = spark.table(f"{cat}.silver.hospitals")
    df = h.select(skey("hospital_id").alias("hospital_key"), "hospital_id", "hospital_name", "city", "state",
                  "beds", "opened_date", (~F.col("is_deleted")).alias("is_active"))
    return with_unknown(spark, df, "hospital_key", {"hospital_id": "unknown", "hospital_name": "unknown"})


def dim_doctor(spark, cat):
    """History of each doctor: specialty, status and department (no name or phone in Gold)."""
    d = history(spark.table(f"{cat}.silver.doctors"), "doctor_id")
    dept = spark.table(f"{cat}.silver.departments").select("dept_id", F.col("dept_name").alias("department"), "hospital_id")
    df = (d.join(dept, "dept_id", "left")
           .select(skey("doctor_id", "valid_from").alias("doctor_key"), "doctor_id", "specialty", "status",
                   "dept_id", "department", "hospital_id", "valid_from", "valid_to", "is_current"))
    return with_unknown(spark, df, "doctor_key", {"doctor_id": "unknown", "specialty": "unknown", "department": "unknown"})


def dim_patient(spark, cat):
    """History of each patient without name, phone, national id or date of birth.
    Age is not here: the fact carries the age band at the time of each event."""
    p = history(spark.table(f"{cat}.silver.patients"), "patient_id")
    df = p.select(skey("patient_id", "valid_from").alias("patient_key"), "patient_id", "gender", "city",
                  "insurance_provider", "id_fingerprint", "valid_from", "valid_to", "is_current")
    return with_unknown(spark, df, "patient_key", {"patient_id": "unknown", "gender": "unknown", "city": "unknown"})


def versions(spark, cat, table, id_col, key_col):
    """Silver history with the same surrogate key as the dimension, for point-in-time lookups."""
    return history(spark.table(f"{cat}.silver.{table}"), id_col).withColumn(key_col, skey(id_col, "valid_from"))


def as_of(events, hist, id_col, key_col, keep=()):
    """Attach the version of id_col that was valid at event_ts. No matching version (missing parent)
    gives key -1; no id at all (not applicable, e.g. the doctor of a pharmacy sale) gives NULL."""
    h = hist.select(F.col(id_col).alias("_id"), F.col("valid_from").alias("_vf"), F.col("valid_to").alias("_vt"),
                    F.col(key_col).alias("_key"), *keep)
    cond = (F.col(id_col) == F.col("_id")) & (F.col("event_ts") >= F.col("_vf")) & (F.col("event_ts") < F.col("_vt"))
    key = F.when(F.col(id_col).isNull(), F.lit(None)).otherwise(F.coalesce(F.col("_key"), F.lit(-1)))
    return events.join(h, cond, "left").withColumn(key_col, key.cast("bigint")).drop("_id", "_vf", "_vt", "_key")


def age_band(age):
    out = F.when(age.isNull(), "unknown")
    for upper, label in [(18, "0-17"), (30, "18-29"), (45, "30-44"), (60, "45-59")]:
        out = out.when(age < upper, label)
    return out.otherwise("60+")


IDS = ["patient_id", "doctor_id", "hospital_id", "invoice_id", "admission_id", "payer_type", "insurer", "pay_mode"]


def events(df, etype, group, eid, ts, amount, qty=None, **ids):
    """One source shaped into the common event layout. ids maps a layout column to a column of df."""
    return df.select(F.lit(etype).alias("event_type"), F.lit(group).alias("event_group"),
                     F.col(eid).alias("event_id"), F.col(ts).cast("timestamp").alias("event_ts"),
                     amount.cast("decimal(18,2)").alias("amount"),
                     (qty if qty is not None else F.lit(None)).cast("int").alias("quantity"),
                     *[(F.col(ids[c]) if c in ids else F.lit(None)).cast("string").alias(c) for c in IDS])


def fact_bhg_event(spark, cat):
    """One row per money event at BHG. Returns (fact, {"dim_payer": payer dimension}).

    charge      consult (completed appointment), treatment, room (admission invoice minus its
                treatments: ward and stay), pharmacy (sale)
    adjustment  discount (negative), tax
    collection  payment
    For every invoice: charges + discount + tax = net amount, and payments = net amount when paid."""
    t = lambda name: spark.table(f"{cat}.silver.{name}").filter(~F.col("is_deleted"))
    adm = t("admissions").select("admission_id", F.col("patient_id").alias("adm_patient"),
                                 F.col("doctor_id").alias("adm_doctor"), F.col("hospital_id").alias("adm_hospital"))
    inv = (t("invoices").join(t("insurance_claims").select("invoice_id", "insurer"), "invoice_id", "left")
           .join(adm, "admission_id", "left")
           .join(t("appointments").select("appt_id", F.col("doctor_id").alias("appt_doctor")), "appt_id", "left")
           .withColumn("doc", F.coalesce("adm_doctor", "appt_doctor")))
    adm_inv = inv.filter("admission_id IS NOT NULL").select("admission_id", "invoice_id", "payer_type", "insurer")
    treated = t("treatments").groupBy("admission_id").agg(F.sum("cost").alias("treated"))
    on_inv = dict(patient_id="patient_id", doctor_id="doc", hospital_id="adm_hospital", invoice_id="invoice_id",
                  admission_id="admission_id", payer_type="payer_type", insurer="insurer")
    one = F.lit(1)

    ev = reduce(lambda x, y: x.unionByName(y), [
        events(t("appointments").filter("status = 'completed'")
                 .join(t("invoices").select("appt_id", "invoice_id", "payer_type"), "appt_id", "left"),
               "consult", "charge", "appt_id", "appt_ts", F.col("consult_fee"), one,
               patient_id="patient_id", doctor_id="doctor_id", invoice_id="invoice_id", payer_type="payer_type"),
        events(t("treatments").join(adm, "admission_id", "left").join(adm_inv, "admission_id", "left"),
               "treatment", "charge", "treatment_id", "treatment_date", F.col("cost"), one,
               patient_id="adm_patient", doctor_id="doctor_id", hospital_id="adm_hospital", invoice_id="invoice_id",
               admission_id="admission_id", payer_type="payer_type", insurer="insurer"),
        events(inv.filter("admission_id IS NOT NULL").join(treated, "admission_id", "left"),
               "room", "charge", "invoice_id", "invoice_date", F.col("gross_amt") - F.coalesce("treated", F.lit(0)), one,
               **on_inv),
        events(t("pharmacy_sales").withColumn("_self", F.lit("self")),
               "pharmacy", "charge", "sale_id", "sale_ts", F.col("qty") * F.col("unit_price"), F.col("qty"),
               patient_id="patient_id", hospital_id="hospital_id", payer_type="_self"),
        events(inv.filter("discount_amt <> 0"), "discount", "adjustment", "invoice_id", "invoice_date",
               -F.col("discount_amt"), **on_inv),
        events(inv.filter("tax_amt <> 0"), "tax", "adjustment", "invoice_id", "invoice_date", F.col("tax_amt"), **on_inv),
        events(t("payments").join(inv.select("invoice_id", "patient_id", "admission_id", "doc", "adm_hospital",
                                             "payer_type", "insurer"), "invoice_id", "left"),
               "payment", "collection", "payment_id", "pay_date", F.col("amount"), pay_mode="mode", **on_inv)])

    pat = versions(spark, cat, "patients", "patient_id", "patient_key")
    dept = spark.table(f"{cat}.silver.departments").select("dept_id", F.col("hospital_id").alias("doc_hospital"))
    doc = versions(spark, cat, "doctors", "doctor_id", "doctor_key").join(dept, "dept_id", "left")
    hosp = spark.table(f"{cat}.silver.hospitals").select(F.col("hospital_id").alias("_h"))
    ev = as_of(ev, pat, "patient_id", "patient_key", keep=["dob"])
    ev = as_of(ev, doc, "doctor_id", "doctor_key", keep=["doc_hospital"])
    ev = (ev.withColumn("hospital_id", F.coalesce("hospital_id", "doc_hospital"))
            .join(hosp, F.col("hospital_id") == F.col("_h"), "left")
            .withColumn("hospital_key", F.when(F.col("hospital_id").isNotNull() & F.col("_h").isNotNull(), skey("hospital_id"))
                                         .when(F.col("hospital_id").isNotNull() | (F.col("doctor_key") == -1), F.lit(-1))
                                         .cast("bigint"))       # unknown doctor -> unknown hospital
            .withColumn("payer_type", F.coalesce("payer_type", F.lit("not invoiced"))))
    payer_cols = [F.coalesce(F.col(c), F.lit("~")) for c in ("payer_type", "insurer", "pay_mode")]
    ev = ev.withColumn("payer_key", F.xxhash64(*payer_cols))
    age = F.floor(F.months_between(F.to_date("event_ts"), F.col("dob")) / 12).cast("int")

    fact = ev.select(skey("event_type", "event_id").alias("event_key"), "event_id", "event_type", "event_group",
                     F.date_format("event_ts", "yyyyMMdd").cast("int").alias("date_key"),
                     "patient_key", "doctor_key", "hospital_key", "payer_key", "invoice_id", "admission_id",
                     "amount", "quantity", (F.col("payer_type") == "fqf_loan").alias("is_fqf_loan"),
                     age.alias("age_at_event"), age_band(age).alias("age_band"))
    payer = ev.select("payer_key", "payer_type", "insurer", "pay_mode").distinct()
    return fact, {"dim_payer": payer}


FACTS = {"bhg": {"fact_bhg_event": fact_bhg_event}}


def build_facts(spark, cfg):
    """Build this company's facts and the dimensions that come out of them. Returns {table: rows}."""
    out = {}
    for name, fn in FACTS.get(cfg["code"], {}).items():
        fact, extra = fn(spark, cfg["catalog"])
        for dim, df in extra.items():
            out[dim] = save(spark, cfg, dim, df)
        out[name] = save(spark, cfg, name, fact)
    return out


DIMENSIONS = {"bhg": {"dim_hospital": dim_hospital, "dim_doctor": dim_doctor, "dim_patient": dim_patient}}


def build_dimensions(spark, cfg):
    """Build this company's dimensions. Returns {table: rows}."""
    return {name: save(spark, cfg, name, fn(spark, cfg["catalog"]))
            for name, fn in DIMENSIONS.get(cfg["code"], {}).items()}
    
