-- fact_group_daily: group money per day, company and measure.
--   BHG measures: billed (charges and discounts), tax, collected
--   FQF measures: disbursed, interest_due, collected, fees
-- intercompany = money moving between the two companies (an FQF loan paying a BHG invoice). It is real for
-- each company, but inside the group it is only a transfer, so group_amount = amount - intercompany.
-- Parameters: :fqf_catalog (FQF catalog of the same environment), :ic_payer_type (config/group.yml: fqf_loan)
WITH movements AS (
  SELECT e.date_key, 'bhg' AS company_code,
         CASE WHEN e.event_group = 'charge'   THEN 'billed'
              WHEN e.event_type  = 'discount' THEN 'billed'
              WHEN e.event_type  = 'tax'      THEN 'tax'
              WHEN e.event_type  = 'payment'  THEN 'collected' END                 AS measure,
         e.amount,
         CASE WHEN e.event_type = 'payment' AND p.payer_type = :ic_payer_type
              THEN e.amount ELSE CAST(0 AS DECIMAL(18,2)) END                       AS intercompany
  FROM gold.fact_bhg_event e
  LEFT JOIN (SELECT payer_key, payer_type FROM gold.dim_payer) p ON e.payer_key = p.payer_key

  UNION ALL
  SELECT date_key, 'fqf',
         CASE WHEN event_type = 'disbursement' THEN 'disbursed'
              WHEN event_type = 'emi_due'      THEN 'interest_due'
              WHEN event_type = 'repayment'    THEN 'collected'
              WHEN event_type = 'late_fee'     THEN 'fees' END,
         CASE WHEN event_type = 'emi_due' THEN coalesce(interest_part, CAST(0 AS DECIMAL(18,2))) ELSE amount END,
         CASE WHEN event_type = 'disbursement' AND is_hospital_bill
              THEN amount ELSE CAST(0 AS DECIMAL(18,2)) END
  FROM IDENTIFIER(:fqf_catalog || '.gold.fact_fqf_event')
)
SELECT
  date_key,
  xxhash64(CAST(company_code AS STRING))   AS company_key,
  company_code,
  measure,
  events,
  amount,
  intercompany,
  amount - intercompany                     AS group_amount
FROM (
  SELECT date_key, company_code, measure, count(*) AS events,
         CAST(sum(amount) AS DECIMAL(18,2))       AS amount,
         CAST(sum(intercompany) AS DECIMAL(18,2)) AS intercompany
  FROM movements
  GROUP BY date_key, company_code, measure
)
