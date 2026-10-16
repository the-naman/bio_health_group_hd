-- dim_group_customer: one row per real person across both companies, plus "unknown" (key -1).
-- match_method: aadhaar (same fingerprint in both companies), invoice_link, review, or one company.
-- is_shared = the person is both a BHG patient and an FQF customer.
WITH per_person AS (
  SELECT
    xxhash64(anchor)                                         AS group_customer_key,
    max(company_code = 'bhg')                                AS is_bhg_patient,
    max(company_code = 'fqf')                                AS is_fqf_customer,
    max(how)                                                 AS how,
    max(id_fingerprint)                                      AS id_fingerprint,
    max(CASE WHEN company_code = 'bhg' THEN city END)        AS bhg_city,
    max(CASE WHEN company_code = 'fqf' THEN city END)        AS fqf_city,
    max(gender)                                              AS gender
  FROM _grp_matched
  GROUP BY xxhash64(anchor)
)
SELECT
  group_customer_key,
  CASE WHEN how IS NOT NULL THEN how
       WHEN is_bhg_patient AND is_fqf_customer THEN 'aadhaar'
       ELSE 'one company' END                                AS match_method,
  is_bhg_patient,
  is_fqf_customer,
  is_bhg_patient AND is_fqf_customer                         AS is_shared,
  coalesce(bhg_city, fqf_city)                               AS city,     -- BHG's city first
  gender,
  id_fingerprint
FROM per_person

UNION ALL
--     key  match_method is_bhg is_fqf is_shared city       gender     id_fingerprint
SELECT -1,  'unknown',   FALSE,  FALSE,  FALSE,   'unknown', 'unknown', NULL
