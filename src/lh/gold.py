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
import hashlib
import hmac
import os
import re
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


def contact_fingerprint(cat):
    """Keyed hash of phone (last 10 digits) + date of birth, made with the same group match key as
    id_fingerprint. Two companies can then spot the same contact details without either one
    showing a phone number or a date of birth."""
    key = common.match_key(cat.rsplit("_", 1)[1])          # catalog bhg_dev -> scope lh_dev

    def fp(phone, dob):
        digits = re.sub(r"\D", "", str(phone or ""))[-10:]
        if len(digits) != 10 or dob is None:
            return None
        return hmac.new(key.encode(), f"{digits}|{dob.isoformat()}".encode(), hashlib.sha256).hexdigest()
    return F.udf(fp, "string")(F.col("phone"), F.col("dob"))


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
    Age is not here: the fact carries the age band at the time of each event.
    contact_fingerprint lets the group spot look-alikes across companies (see group.py)."""
    p = history(spark.table(f"{cat}.silver.patients"), "patient_id")
    df = p.select(skey("patient_id", "valid_from").alias("patient_key"), "patient_id", "gender", "city",
                  "insurance_provider", "id_fingerprint", contact_fingerprint(cat).alias("contact_fingerprint"),
                  "valid_from", "valid_to", "is_current")
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


def dim_branch(spark, cat):
    b = spark.table(f"{cat}.silver.branches")
    df = b.select(skey("branch_id").alias("branch_key"), "branch_id", "branch_name", "city", "state", "opened_date",
                  (~F.col("is_deleted")).alias("is_active"))
    return with_unknown(spark, df, "branch_key", {"branch_id": "unknown", "branch_name": "unknown"})


def dim_product(spark, cat):
    """History of each loan product: type, rate, limits."""
    p = history(spark.table(f"{cat}.silver.loan_products"), "product_id")
    df = p.select(skey("product_id", "valid_from").alias("product_key"), "product_id", "product_name", "loan_type",
                  "rate_pct", "min_amt", "max_amt", "max_tenure_m", "valid_from", "valid_to", "is_current")
    return with_unknown(spark, df, "product_key", {"product_id": "unknown", "product_name": "unknown", "loan_type": "unknown"})


def income_band(income):
    out = F.when(income.isNull(), "unknown")
    for upper, label in [(25000, "<25k"), (50000, "25k-50k"), (100000, "50k-1L")]:
        out = out.when(income < upper, label)
    return out.otherwise("1L+")


def dim_customer(spark, cat):
    """History of each customer id without name, phone, national id, date of birth or exact income.
    master_customer_id is the surviving id of the same person (duplicates merged by silver.customer_id_map)."""
    c = history(spark.table(f"{cat}.silver.customers"), "customer_id")
    m = spark.table(f"{cat}.silver.customer_id_map").select("customer_id", F.col("survivor_id").alias("master_customer_id"))
    df = (c.join(m, "customer_id", "left")
           .select(skey("customer_id", "valid_from").alias("customer_key"), "customer_id",
                   F.coalesce("master_customer_id", "customer_id").alias("master_customer_id"), "gender", "city",
                   "branch_id", income_band(F.col("monthly_income")).alias("income_band"),
                   F.coalesce("employment", F.lit("unknown")).alias("employment"), "id_fingerprint",
                   contact_fingerprint(cat).alias("contact_fingerprint"), "valid_from", "valid_to", "is_current"))
    return with_unknown(spark, df, "customer_key", {"customer_id": "unknown", "master_customer_id": "unknown",
                                                    "gender": "unknown", "city": "unknown", "income_band": "unknown",
                                                    "employment": "unknown"})


def dim_loan(spark, cat):
    """One row per loan: terms, the credit check, the credit policy in force on the application date,
    whether it paid a BHG hospital bill, and the latest collections bucket."""
    t = lambda name: spark.table(f"{cat}.silver.{name}").filter(~F.col("is_deleted"))
    app = t("loan_applications").select("application_id", "applied_date", "bhg_invoice_id")
    chk = (t("credit_checks").withColumn("_rn", F.row_number().over(
               Window.partitionBy("application_id").orderBy(F.col("check_ts").desc())))
           .filter("_rn = 1").select("application_id", "bureau_score", "score_band", "dti_pct"))
    pol = t("credit_policy").select("policy_id", F.col("product_id").alias("_pp"), "effective_from",
                                    F.col("max_dti").alias("policy_max_dti"),
                                    F.col("min_bureau_score").alias("policy_min_bureau_score"),
                                    F.col("max_exposure").alias("policy_max_exposure"))
    col = (t("collections").withColumn("_rn", F.row_number().over(
               Window.partitionBy("loan_id").orderBy(F.col("last_modified").desc())))
           .filter("_rn = 1").select("loan_id", F.col("bucket").alias("dpd_bucket")))
    l = t("loans").join(app, "application_id", "left").join(chk, "application_id", "left")
    l = l.join(pol, (F.col("product_id") == F.col("_pp")) & (F.col("effective_from") <= F.col("applied_date")), "left")
    l = (l.withColumn("_rn", F.row_number().over(Window.partitionBy("loan_id").orderBy(F.col("effective_from").desc_nulls_last())))
          .filter("_rn = 1").join(col, "loan_id", "left"))
    df = l.select(skey("loan_id").alias("loan_key"), "loan_id", "application_id", "principal", "rate_pct", "tenure_m",
                  "start_date", "status", "bureau_score", "score_band", "dti_pct", "policy_id", "policy_max_dti",
                  "policy_min_bureau_score", "policy_max_exposure",
                  F.col("bhg_invoice_id").isNotNull().alias("is_hospital_bill"),
                  F.coalesce("dpd_bucket", F.lit("current")).alias("dpd_bucket"))
    return with_unknown(spark, df, "loan_key", {"loan_id": "unknown", "status": "unknown", "dpd_bucket": "unknown"})


def fact_fqf_event(spark, cat):
    """One row per money event on an FQF loan. Returns (fact, {}).

    lending     disbursement (money paid out: to the customer or to a BHG hospital)
    due         emi_due (each instalment that has fallen due up to today, split into principal and interest)
    collection  repayment (instalment paid; split like the instalment it pays)
    fee         late_fee
    A loan that is not in silver (orphan repayment) gets key -1 for loan, customer, product and branch."""
    t = lambda name: spark.table(f"{cat}.silver.{name}").filter(~F.col("is_deleted"))
    emi = t("emi_schedule").select("loan_id", "emi_no", "due_date", "principal_due", "interest_due", "emi_amount")
    rep = t("repayments").join(emi, ["loan_id", "emi_no"], "left")
    late = F.greatest(F.datediff("paid_date", "due_date"), F.lit(0))
    nul = F.lit(None)

    def shape(df, etype, group, eid, ts, amount, principal=nul, interest=nul, dpd=nul, mode=nul, emi_no=nul, inv=nul):
        return df.select(F.lit(etype).alias("event_type"), F.lit(group).alias("event_group"),
                         F.col(eid).alias("event_id"), F.col(ts).cast("timestamp").alias("event_ts"), "loan_id",
                         amount.cast("decimal(18,2)").alias("amount"),
                         principal.cast("decimal(18,2)").alias("principal_part"),
                         interest.cast("decimal(18,2)").alias("interest_part"),
                         dpd.cast("int").alias("dpd_at_event"), mode.cast("string").alias("pay_mode"),
                         emi_no.cast("int").alias("emi_no"), inv.cast("string").alias("bhg_invoice_id"))

    ev = reduce(lambda x, y: x.unionByName(y), [
        shape(t("disbursements"), "disbursement", "lending", "disb_id", "disb_date", F.col("amount"),
              principal=F.col("amount"), inv=F.col("bhg_invoice_id")),
        shape(emi.filter(F.col("due_date") <= F.current_date())
                 .withColumn("_id", F.concat_ws("-", "loan_id", F.lpad(F.col("emi_no").cast("string"), 3, "0"))),
              "emi_due", "due", "_id", "due_date", F.col("emi_amount"), F.col("principal_due"), F.col("interest_due"),
              emi_no=F.col("emi_no")),
        shape(rep, "repayment", "collection", "repay_id", "paid_date", F.col("amount"),
              F.col("principal_due"), F.col("interest_due"), late, F.col("mode"), F.col("emi_no")),
        shape(rep.filter("late_fee > 0"), "late_fee", "fee", "repay_id", "paid_date", F.col("late_fee"),
              dpd=late, mode=F.col("mode"), emi_no=F.col("emi_no"))])

    loans = spark.table(f"{cat}.silver.loans").filter(~F.col("is_deleted")).select(
        F.col("loan_id").alias("_loan"), "customer_id", "product_id", "branch_id")
    ev = ev.join(loans, F.col("loan_id") == F.col("_loan"), "left")
    ev = as_of(ev, versions(spark, cat, "customers", "customer_id", "customer_key"), "customer_id", "customer_key")
    ev = as_of(ev, versions(spark, cat, "loan_products", "product_id", "product_key"), "product_id", "product_key")
    branches = spark.table(f"{cat}.silver.branches").select(F.col("branch_id").alias("_b"))
    ev = ev.join(branches, F.col("branch_id") == F.col("_b"), "left")
    found = F.col("_loan").isNotNull()
    known = lambda c: F.when(found, F.col(c)).otherwise(F.lit(-1)).cast("bigint")

    fact = ev.select(skey("event_type", "event_id").alias("event_key"), "event_id", "event_type", "event_group",
                     F.date_format("event_ts", "yyyyMMdd").cast("int").alias("date_key"),
                     F.when(found, skey("loan_id")).otherwise(F.lit(-1)).cast("bigint").alias("loan_key"),
                     known("customer_key").alias("customer_key"), known("product_key").alias("product_key"),
                     F.when(F.col("_b").isNotNull(), skey("branch_id")).otherwise(F.lit(-1)).cast("bigint").alias("branch_key"),
                     "loan_id", "emi_no", "amount", "principal_part", "interest_part", "dpd_at_event", "pay_mode",
                     "bhg_invoice_id", F.col("bhg_invoice_id").isNotNull().alias("is_hospital_bill"))
    return fact, {}


DIMENSIONS = {"bhg": {"dim_hospital": dim_hospital, "dim_doctor": dim_doctor, "dim_patient": dim_patient},
              "fqf": {"dim_branch": dim_branch, "dim_product": dim_product, "dim_customer": dim_customer,
                      "dim_loan": dim_loan}}


def build_dimensions(spark, cfg):
    """Build this company's dimensions. Returns {table: rows}."""
    return {name: save(spark, cfg, name, fn(spark, cfg["catalog"]))
            for name, fn in DIMENSIONS.get(cfg["code"], {}).items()}


FACTS = {"bhg": {"fact_bhg_event": fact_bhg_event}, "fqf": {"fact_fqf_event": fact_fqf_event}}


def build_facts(spark, cfg):
    """Build this company's facts and the dimensions that come out of them. Returns {table: rows}."""
    out = {}
    for name, fn in FACTS.get(cfg["code"], {}).items():
        fact, extra = fn(spark, cfg["catalog"])
        for dim, df in extra.items():
            out[dim] = save(spark, cfg, dim, df)
        out[name] = save(spark, cfg, name, fact)
    return out

    