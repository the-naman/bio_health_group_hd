-- fact_group_kpi: the headline numbers of the group, one row per number, as of today.
-- A dashboard tile is then one simple filter: kpi = 'people_shared'.
SELECT kpi, CAST(coalesce(value, 0) AS DECIMAL(20,2)) AS value, current_date() AS as_of_date
FROM (
  SELECT 'people'              AS kpi, count(*)            AS value FROM gold.dim_group_customer WHERE group_customer_key <> -1
  UNION ALL SELECT 'people_shared',       count(*)          FROM gold.dim_group_customer WHERE is_shared
  UNION ALL SELECT 'bhg_patients',        count(*)          FROM gold.dim_group_customer WHERE is_bhg_patient
  UNION ALL SELECT 'fqf_customers',       count(*)          FROM gold.dim_group_customer WHERE is_fqf_customer
  UNION ALL SELECT 'reviews_open',        count(*)          FROM gold.group_match_review WHERE status = 'open'
  UNION ALL SELECT 'intercompany_breaks', count(*)          FROM gold.group_intercompany WHERE status <> 'matched'
  UNION ALL SELECT 'people_over_limit',   count(*)          FROM gold.group_exposure     WHERE over_limit
  UNION ALL SELECT 'group_exposure',      sum(total_exposure) FROM gold.group_exposure
  UNION ALL SELECT 'group_billed',        sum(group_amount) FROM gold.fact_group_daily   WHERE company_code = 'bhg' AND measure = 'billed'
  UNION ALL SELECT 'group_collected',     sum(group_amount) FROM gold.fact_group_daily   WHERE measure = 'collected'
)
