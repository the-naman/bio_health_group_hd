-- group_match_review: pairs a person must look at (the Manager records a decision in ops.group_match_decisions).
--   same phone and date of birth, different Aadhaar     -> maybe one person with a wrong Aadhaar, maybe not
--   hospital bill paid by a loan of a different Aadhaar -> a loan paid someone else's bill
-- status: open (no decision yet) or decided (decision = same / different).
WITH
candidates AS (
  SELECT p.member_id AS bhg_patient_id, c.member_id AS fqf_customer_id,
         'same phone and date of birth, different Aadhaar' AS reason
  FROM _grp_matched p
  JOIN _grp_matched c ON p.contact_fingerprint = c.contact_fingerprint
  WHERE p.company_code = 'bhg' AND c.company_code = 'fqf' AND p.anchor <> c.anchor

  UNION ALL
  SELECT patient_id, customer_id, 'hospital bill paid by a loan of a different Aadhaar'
  FROM _grp_links WHERE pfp IS NOT NULL AND cfp IS NOT NULL
),
decisions AS (
  SELECT bhg_patient_id, fqf_customer_id, decision
  FROM ops.group_match_decisions WHERE decision IN ('same', 'different')
)
SELECT
  xxhash64(CAST(r.bhg_patient_id AS STRING), CAST(r.fqf_customer_id AS STRING), CAST(r.reason AS STRING)) AS review_key,
  r.bhg_patient_id,
  r.fqf_customer_id,
  r.reason,
  CASE WHEN d.decision IS NULL THEN 'open' ELSE 'decided' END AS status,
  d.decision
FROM candidates r
LEFT JOIN decisions d ON r.bhg_patient_id = d.bhg_patient_id AND r.fqf_customer_id = d.fqf_customer_id
