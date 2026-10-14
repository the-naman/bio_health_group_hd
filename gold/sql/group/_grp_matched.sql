-- _grp_matched (helper step, not saved): every member with its final person (anchor) and how it was linked.
-- How people are matched:
--   1. same Aadhaar fingerprint = same person (already in _grp_members)
--   2. invoice_link: a hospital-bill loan links a BHG patient and an FQF customer; if only one side has an
--      Aadhaar fingerprint, the other side joins that person (two different fingerprints are never merged)
--   3. review: a reviewed pair decided 'same' in ops.group_match_decisions; the FQF member joins the BHG person
-- Look-alikes (same phone + date of birth) are never merged automatically; they go to group_match_review.
WITH
decided_same AS (
  SELECT bhg_patient_id AS patient_id, fqf_customer_id AS customer_id
  FROM ops.group_match_decisions WHERE decision = 'same'
),
moves AS (
  -- invoice link, patient has no fingerprint, customer has one: the patient joins the customer's person
  SELECT 'bhg' AS company_code, patient_id AS member_id, ca AS new_anchor, 'invoice_link' AS how
  FROM _grp_links WHERE pfp IS NULL AND cfp IS NOT NULL
  UNION ALL
  -- invoice link, customer has no fingerprint: the customer joins the patient's person
  SELECT 'fqf', customer_id, pa, 'invoice_link'
  FROM _grp_links WHERE cfp IS NULL
  UNION ALL
  -- a reviewed pair decided 'same': the customer joins the patient's person
  SELECT 'fqf', d.customer_id, p.anchor, 'review'
  FROM decided_same d
  JOIN _grp_members p ON p.company_code = 'bhg' AND p.member_id = d.patient_id
  JOIN _grp_members c ON c.company_code = 'fqf' AND c.member_id = d.customer_id
  WHERE p.anchor <> c.anchor
),
one_move AS (            -- if a member has several moves, take one fixed choice
  SELECT company_code, member_id, min(new_anchor) AS new_anchor, min(how) AS how
  FROM moves GROUP BY company_code, member_id
)
SELECT m.company_code, m.member_id, m.id_fingerprint, m.contact_fingerprint, m.city, m.gender,
       coalesce(o.new_anchor, m.anchor) AS anchor,
       o.how
FROM _grp_members m
LEFT JOIN one_move o ON m.company_code = o.company_code AND m.member_id = o.member_id
