"""Group consolidation: BHG prod Gold + FQF prod Gold = one group view (BHG repo only).

BHG reads the FQF Gold of the SAME environment, read-only (config/group.yml), so FQF dev or test
data never reaches BHG prod. All group tables live in <bhg catalog>.gold and are rebuilt in full
on every run, like the company Gold tables.

  dim_company          one row per company (parent and child)
  map_group_customer   one row per company member (BHG patient, FQF master customer) -> its person
  dim_group_customer   one row per real person across both companies
  group_match_review   pairs a person must look at: same phone + date of birth but a different
                       Aadhaar, or a hospital bill paid by a loan of a different Aadhaar

How people are matched (no names, phones or dates of birth are read, only keyed hashes):
  1. same id_fingerprint (keyed hash of Aadhaar) = same person
  2. a hospital-bill loan links the BHG patient of the invoice and the FQF customer of the loan;
     if only one side has an Aadhaar fingerprint, the other side joins that person
  3. a decision recorded in ops.group_match_decisions ("same") merges a reviewed pair
Look-alikes are never merged automatically.
"""
import os
from functools import reduce

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


DECISIONS = "ops.group_match_decisions"


def decisions(spark, cat):
    """Review decisions made by a person; created empty on first use, never rebuilt."""
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cat}.{DECISIONS} (
        bhg_patient_id STRING, fqf_customer_id STRING, decision STRING, decided_by STRING, decided_at TIMESTAMP)""")
    return spark.table(f"{cat}.{DECISIONS}").filter(F.col("decision").isin("same", "different"))


def members(bhg, fqf):
    """Current BHG patients and current FQF master customers with their match hashes."""
    cols = ["company_code", "member_id", "id_fingerprint", "contact_fingerprint", "city", "gender"]
    p = (bhg("dim_patient").filter("is_current AND patient_key <> -1")
         .select(F.lit("bhg").alias("company_code"), F.col("patient_id").alias("member_id"), *cols[2:]))
    c = (fqf("dim_customer").filter("is_current AND customer_key <> -1 AND customer_id = master_customer_id")
         .select(F.lit("fqf").alias("company_code"), F.col("master_customer_id").alias("member_id"), *cols[2:]))
    return p.unionByName(c)


def invoice_links(bhg, fqf):
    """(BHG patient, FQF master customer) pairs where the customer's loan paid the patient's invoice."""
    b = (bhg("fact_bhg_event").filter("invoice_id IS NOT NULL AND patient_key IS NOT NULL AND patient_key <> -1")
         .select("invoice_id", "patient_key").distinct()
         .join(bhg("dim_patient").select("patient_key", "patient_id"), "patient_key")
         .select("invoice_id", "patient_id"))
    f = (fqf("fact_fqf_event").filter("event_type = 'disbursement' AND bhg_invoice_id IS NOT NULL AND customer_key <> -1")
         .select(F.col("bhg_invoice_id").alias("invoice_id"), "customer_key")
         .join(fqf("dim_customer").select("customer_key", "master_customer_id"), "customer_key")
         .select("invoice_id", F.col("master_customer_id").alias("customer_id")))
    return b.join(f, "invoice_id").distinct()


def match(m, links, decided):
    """Give every member its person anchor and say how it was matched. Returns (members, review)."""
    m = m.withColumn("anchor", F.when(F.col("id_fingerprint").isNotNull(), F.concat(F.lit("id:"), "id_fingerprint"))
                                .otherwise(F.concat_ws(":", "company_code", "member_id")))
    pb = m.filter("company_code = 'bhg'").select(F.col("member_id").alias("patient_id"), F.col("anchor").alias("pa"),
                                                 F.col("id_fingerprint").alias("pfp"))
    cf = m.filter("company_code = 'fqf'").select(F.col("member_id").alias("customer_id"), F.col("anchor").alias("ca"),
                                                 F.col("id_fingerprint").alias("cfp"))
    l = links.join(pb, "patient_id").join(cf, "customer_id").filter("pa <> ca")
    same = decided.filter("decision = 'same'").select(F.col("bhg_patient_id").alias("patient_id"),
                                                      F.col("fqf_customer_id").alias("customer_id"))
    pairs = same.join(pb, "patient_id").join(cf, "customer_id").filter("pa <> ca")
    moves = reduce(lambda x, y: x.unionByName(y), [
        # invoice link: the side without an Aadhaar fingerprint joins the other side's person
        l.filter("pfp IS NULL AND cfp IS NOT NULL").select(F.lit("bhg").alias("company_code"),
            F.col("patient_id").alias("member_id"), F.col("ca").alias("new_anchor"), F.lit("invoice_link").alias("how")),
        l.filter("cfp IS NULL").select(F.lit("fqf").alias("company_code"), F.col("customer_id").alias("member_id"),
            F.col("pa").alias("new_anchor"), F.lit("invoice_link").alias("how")),
        # a reviewed pair decided "same": the FQF member joins the BHG person
        pairs.select(F.lit("fqf").alias("company_code"), F.col("customer_id").alias("member_id"),
                     F.col("pa").alias("new_anchor"), F.lit("review").alias("how"))])
    moves = moves.groupBy("company_code", "member_id").agg(F.min("new_anchor").alias("new_anchor"), F.min("how").alias("how"))
    m = (m.join(moves, ["company_code", "member_id"], "left")
          .withColumn("anchor", F.coalesce("new_anchor", "anchor")).drop("new_anchor"))

    pm = m.filter("company_code = 'bhg'").select(F.col("member_id").alias("bhg_patient_id"), F.col("anchor").alias("pa"),
                                                 F.col("contact_fingerprint").alias("k"))
    cm = m.filter("company_code = 'fqf'").select(F.col("member_id").alias("fqf_customer_id"), F.col("anchor").alias("ca"),
                                                 F.col("contact_fingerprint").alias("k"))
    review = reduce(lambda x, y: x.unionByName(y), [
        pm.join(cm, "k").filter("pa <> ca").select("bhg_patient_id", "fqf_customer_id",
            F.lit("same phone and date of birth, different Aadhaar").alias("reason")),
        l.filter("pfp IS NOT NULL AND cfp IS NOT NULL").select(F.col("patient_id").alias("bhg_patient_id"),
            F.col("customer_id").alias("fqf_customer_id"), F.lit("hospital bill paid by a loan of a different Aadhaar").alias("reason"))])
    d = decided.select(F.col("bhg_patient_id"), F.col("fqf_customer_id"), "decision")
    review = (review.join(d, ["bhg_patient_id", "fqf_customer_id"], "left")
                    .withColumn("status", F.when(F.col("decision").isNull(), "open").otherwise("decided"))
                    .select(gold.skey("bhg_patient_id", "fqf_customer_id", "reason").alias("review_key"),
                            "bhg_patient_id", "fqf_customer_id", "reason", "status", "decision"))
    return m, review


def group_customers(spark, m):
    """(map_group_customer, dim_group_customer) from matched members."""
    m = m.withColumn("group_customer_key", F.xxhash64("anchor"))
    per = m.groupBy("group_customer_key").agg(
        F.max(F.col("company_code") == "bhg").alias("is_bhg_patient"),
        F.max(F.col("company_code") == "fqf").alias("is_fqf_customer"),
        F.max("how").alias("how"),
        F.max("id_fingerprint").alias("id_fingerprint"),
        F.max(F.when(F.col("company_code") == "bhg", F.col("city"))).alias("bhg_city"),
        F.max(F.when(F.col("company_code") == "fqf", F.col("city"))).alias("fqf_city"),
        F.max("gender").alias("gender"))
    method = (F.when(F.col("how").isNotNull(), F.col("how"))
               .when(F.col("is_bhg_patient") & F.col("is_fqf_customer"), F.lit("aadhaar"))
               .otherwise(F.lit("one company")))
    dim = per.select("group_customer_key", method.alias("match_method"), "is_bhg_patient", "is_fqf_customer",
                     (F.col("is_bhg_patient") & F.col("is_fqf_customer")).alias("is_shared"),
                     F.coalesce("bhg_city", "fqf_city").alias("city"), "gender", "id_fingerprint")
    unknown = spark.createDataFrame([(-1, "unknown", False, False, False, "unknown", "unknown", None)], dim.schema)
    mp = m.select("company_code", "member_id", "group_customer_key")
    return mp, dim.unionByName(unknown)


def build(spark, cfg, env):
    """Build every group table. Returns {table: rows}."""
    grp, src = load_group(), source(cfg, env)
    out = {}
    out["dim_company"] = gold.save(spark, cfg, "dim_company", dim_company(spark, grp))

    cat = cfg["catalog"]
    bhg = lambda t: spark.table(f"{cat}.gold.{t}")
    fqf = lambda t: spark.table(f"{src}.gold.{t}")
    m, review = match(members(bhg, fqf), invoice_links(bhg, fqf), decisions(spark, cat))
    mp, dim = group_customers(spark, m)
    out["map_group_customer"] = gold.save(spark, cfg, "map_group_customer", mp)
    out["dim_group_customer"] = gold.save(spark, cfg, "dim_group_customer", dim)
    out["group_match_review"] = gold.save(spark, cfg, "group_match_review", review)
    return out

    