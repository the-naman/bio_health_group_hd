-- dim_company: one row per company of the group (parent BHG and child FQF).
-- The rows come from config/group.yml (companies); the Gold runner hands them over as the view _cfg_companies.
SELECT
  xxhash64(CAST(company_code AS STRING)) AS company_key,
  company_code,
  company_name,
  role,
  industry
FROM _cfg_companies
