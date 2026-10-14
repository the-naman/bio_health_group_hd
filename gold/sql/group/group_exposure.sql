-- group_exposure: what each person owes the group today, and whether it is above their credit limit.
--   fqf_outstanding = principal still outstanding on their active FQF loans
--   bhg_unpaid      = BHG bills not yet paid
--   policy_limit    = the strictest credit-policy limit (max_exposure) of their open loans
-- Only people who owe something are listed. Parameter :fqf_catalog = FQF catalog of the same environment.
WITH
person AS (SELECT company_code, member_id, group_customer_key FROM gold.map_group_customer),
owner AS (               -- whose loan is it (as a group person)
  SELECT d.loan_key, pp.group_customer_key
  FROM (SELECT DISTINCT loan_key, customer_key FROM IDENTIFIER(:fqf_catalog || '.gold.fact_fqf_event')
        WHERE event_type = 'disbursement' AND loan_key <> -1) d
  JOIN IDENTIFIER(:fqf_catalog || '.gold.dim_customer') c ON d.customer_key = c.customer_key
  JOIN person pp ON pp.company_code = 'fqf' AND pp.member_id = c.master_customer_id
),
repaid AS (
  SELECT loan_key, sum(principal_part) AS repaid
  FROM IDENTIFIER(:fqf_catalog || '.gold.fact_fqf_event') WHERE event_type = 'repayment'
  GROUP BY loan_key
),
open_loans AS (
  SELECT o.group_customer_key, l.loan_id, l.policy_max_exposure, l.dpd_bucket,
         greatest(l.principal - coalesce(r.repaid, 0), 0) AS outstanding
  FROM IDENTIFIER(:fqf_catalog || '.gold.dim_loan') l
  LEFT JOIN repaid r ON l.loan_key = r.loan_key
  JOIN owner o       ON l.loan_key = o.loan_key
  WHERE l.loan_key <> -1 AND l.status = 'active'
),
fqf AS (
  SELECT group_customer_key, count(*) AS open_loans, sum(outstanding) AS fqf_outstanding,
         min(policy_max_exposure) AS lim,
         max(CASE WHEN dpd_bucket <> 'current' THEN 1 ELSE 0 END) AS late
  FROM open_loans GROUP BY group_customer_key
),
bhg_per_patient AS (     -- charges minus payments per patient
  SELECT p.patient_id AS member_id,
         sum(CASE WHEN e.event_group = 'collection' THEN -e.amount ELSE e.amount END) AS unpaid
  FROM gold.fact_bhg_event e
  JOIN gold.dim_patient p ON e.patient_key = p.patient_key
  WHERE e.invoice_id IS NOT NULL AND e.patient_key IS NOT NULL AND e.patient_key <> -1
  GROUP BY p.patient_id
),
bhg AS (
  SELECT pp.group_customer_key, sum(greatest(b.unpaid, 0)) AS bhg_unpaid
  FROM bhg_per_patient b
  JOIN person pp ON pp.company_code = 'bhg' AND pp.member_id = b.member_id
  GROUP BY pp.group_customer_key
),
total AS (
  SELECT coalesce(f.group_customer_key, b.group_customer_key) AS group_customer_key,
         f.open_loans, f.fqf_outstanding, f.lim, f.late, b.bhg_unpaid,
         coalesce(f.fqf_outstanding, CAST(0 AS DECIMAL(18,2))) + coalesce(b.bhg_unpaid, CAST(0 AS DECIMAL(18,2))) AS total
  FROM fqf f
  FULL OUTER JOIN bhg b ON f.group_customer_key = b.group_customer_key
)
SELECT * FROM (
  SELECT
    group_customer_key,
    current_date()                                                              AS as_of_date,
    coalesce(open_loans, 0)                                                     AS open_loans,
    CAST(coalesce(fqf_outstanding, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2))  AS fqf_outstanding,
    CAST(coalesce(bhg_unpaid, CAST(0 AS DECIMAL(18,2))) AS DECIMAL(18,2))       AS bhg_unpaid,
    CAST(total AS DECIMAL(18,2))                                                AS total_exposure,
    CAST(lim AS DECIMAL(18,2))                                                  AS policy_limit,
    lim IS NOT NULL AND total > lim                                             AS over_limit,
    coalesce(late, 0) = 1                                                       AS is_late
  FROM total
)
WHERE total_exposure > 0
