-- _grp_decisions (helper step, not saved): every review decision as a real pair (bhg_patient_id, fqf_customer_id).
-- The Manager records a decision by review_key, the number shown in group_match_review. In test and prod the two
-- ids are shown as tokens, so a decision typed by id would never match; review_key is not personal data and is
-- not masked. Rows that name the ids directly (older dev rows) still count. One decision per pair: the latest wins.
-- The candidate pairs and keys are built exactly like group_match_review, but from the members before any
-- decision moves them, so a pair keeps resolving after it was decided 'same'.
WITH
candidates AS (
  SELECT p.member_id AS bhg_patient_id, c.member_id AS fqf_customer_id,
         'same phone and date of birth, different Aadhaar' AS reason
  FROM _grp_members p
  JOIN _grp_members c ON p.contact_fingerprint = c.contact_fingerprint
  WHERE p.company_code = 'bhg' AND c.company_code = 'fqf' AND p.anchor <> c.anchor

  UNION ALL
  SELECT patient_id, customer_id, 'hospital bill paid by a loan of a different Aadhaar'
  FROM _grp_links WHERE pfp IS NOT NULL AND cfp IS NOT NULL
),
keyed AS (
  SELECT DISTINCT
         xxhash64(CAST(bhg_patient_id AS STRING), CAST(fqf_customer_id AS STRING), CAST(reason AS STRING)) AS review_key,
         bhg_patient_id, fqf_customer_id
  FROM candidates
),
resolved AS (
  SELECT coalesce(k.bhg_patient_id, d.bhg_patient_id)   AS bhg_patient_id,
         coalesce(k.fqf_customer_id, d.fqf_customer_id) AS fqf_customer_id,
         d.decision,
         coalesce(d.decided_at, TIMESTAMP'1970-01-01')   AS decided_at
  FROM ops.group_match_decisions d
  LEFT JOIN keyed k ON d.review_key = k.review_key
  WHERE d.decision IN ('same', 'different')
)
SELECT bhg_patient_id, fqf_customer_id, max_by(decision, decided_at) AS decision
FROM resolved
WHERE bhg_patient_id IS NOT NULL AND fqf_customer_id IS NOT NULL
GROUP BY bhg_patient_id, fqf_customer_id
