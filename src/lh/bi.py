"""BI layer: views on Gold for dashboards and Power BI (same file in both repos).

A view joins a fact to its dimensions and uses readable names, so a dashboard never needs a join.
Views hold no personal columns, and the masks and row filters of the tables underneath still apply
to whoever reads the view (Unity Catalog checks the reader, not the view). They live in
<catalog>.gold with the prefix v_ and are recreated on every run.

  BHG    v_bhg_revenue_daily     day x hospital x event type x payer x age band
  FQF    v_fqf_portfolio_daily   day x product x branch x event type
         v_fqf_loan_book         loans today by status, overdue bucket, product, branch
  group  v_group_daily           day x company x measure, intercompany removed (BHG only)
         v_group_kpi             headline numbers
         v_group_exposure        people owing the group, by city and flags
         v_group_customers       people by match method, shared or not, city
"""
VIEWS = {
    "bhg": {
        "v_bhg_revenue_daily": """
            SELECT d.date, d.year_month, d.fiscal_year, d.fiscal_quarter,
                   coalesce(h.hospital_name, 'unknown') AS hospital, coalesce(h.city, 'unknown') AS city,
                   f.event_group, f.event_type, coalesce(p.payer_type, 'unknown') AS payer_type, f.age_band,
                   count(*) AS events, sum(f.amount) AS amount
            FROM {cat}.gold.fact_bhg_event f
            JOIN {cat}.gold.dim_date d USING (date_key)
            LEFT JOIN {cat}.gold.dim_hospital h USING (hospital_key)
            LEFT JOIN {cat}.gold.dim_payer p USING (payer_key)
            GROUP BY ALL""",
    },
    "fqf": {
        "v_fqf_portfolio_daily": """
            SELECT d.date, d.year_month, d.fiscal_year, d.fiscal_quarter,
                   coalesce(p.product_name, 'unknown') AS product, coalesce(p.loan_type, 'unknown') AS loan_type,
                   coalesce(b.branch_name, 'unknown') AS branch, coalesce(b.city, 'unknown') AS city,
                   f.event_group, f.event_type, f.is_hospital_bill,
                   count(*) AS events, sum(f.amount) AS amount,
                   sum(f.principal_part) AS principal, sum(f.interest_part) AS interest
            FROM {cat}.gold.fact_fqf_event f
            JOIN {cat}.gold.dim_date d USING (date_key)
            LEFT JOIN {cat}.gold.dim_product p USING (product_key)
            LEFT JOIN {cat}.gold.dim_branch b USING (branch_key)
            GROUP BY ALL""",
        "v_fqf_loan_book": """
            WITH repaid AS (SELECT loan_key, sum(principal_part) AS repaid
                            FROM {cat}.gold.fact_fqf_event WHERE event_type = 'repayment' GROUP BY loan_key),
                 owner AS (SELECT DISTINCT loan_key, product_key, branch_key
                           FROM {cat}.gold.fact_fqf_event WHERE event_type = 'disbursement')
            SELECT l.status, l.dpd_bucket, l.score_band, l.is_hospital_bill,
                   coalesce(p.product_name, 'unknown') AS product, coalesce(b.branch_name, 'unknown') AS branch,
                   count(*) AS loans, sum(l.principal) AS principal,
                   sum(CASE WHEN l.status = 'active'
                            THEN greatest(l.principal - coalesce(r.repaid, 0), 0) ELSE 0 END) AS outstanding
            FROM {cat}.gold.dim_loan l
            LEFT JOIN repaid r USING (loan_key)
            LEFT JOIN owner o USING (loan_key)
            LEFT JOIN {cat}.gold.dim_product p USING (product_key)
            LEFT JOIN {cat}.gold.dim_branch b USING (branch_key)
            WHERE l.loan_key <> -1
            GROUP BY ALL""",
    },
    "group": {
        "v_group_daily": """
            SELECT d.date, d.year_month, d.fiscal_year, d.fiscal_quarter, c.company_name, g.company_code,
                   g.measure, g.events, g.amount, g.intercompany, g.group_amount
            FROM {cat}.gold.fact_group_daily g
            JOIN {cat}.gold.dim_date d USING (date_key)
            JOIN {cat}.gold.dim_company c USING (company_key)""",
        "v_group_kpi": """
            SELECT kpi, value, as_of_date FROM {cat}.gold.fact_group_kpi""",
        "v_group_exposure": """
            SELECT c.city, c.is_shared, e.over_limit, e.is_late,
                   count(*) AS people, sum(e.open_loans) AS open_loans,
                   sum(e.fqf_outstanding) AS fqf_outstanding, sum(e.bhg_unpaid) AS bhg_unpaid,
                   sum(e.total_exposure) AS total_exposure, max(e.as_of_date) AS as_of_date
            FROM {cat}.gold.group_exposure e
            JOIN {cat}.gold.dim_group_customer c USING (group_customer_key)
            GROUP BY ALL""",
        "v_group_customers": """
            SELECT match_method, is_bhg_patient, is_fqf_customer, is_shared, city, count(*) AS people
            FROM {cat}.gold.dim_group_customer WHERE group_customer_key <> -1
            GROUP BY ALL""",
    },
}


def wanted(cfg):
    """The views this catalog gets: its own company's, plus the group views in the parent."""
    out = dict(VIEWS.get(cfg["code"], {}))
    if cfg["code"] == "bhg":
        out.update(VIEWS["group"])
    return out


def build(spark, cfg):
    """Create or replace every view of this catalog. Returns {view: rows}."""
    cat, out = cfg["catalog"], {}
    for name, sql in wanted(cfg).items():
        spark.sql(f"CREATE OR REPLACE VIEW {cat}.gold.{name} AS {sql.format(cat=cat)}")
        out[name] = spark.table(f"{cat}.gold.{name}").count()
    return out

