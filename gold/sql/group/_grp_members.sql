-- _grp_members (helper step, not saved): every current BHG patient and every current FQF master customer,
-- with the hashes used to match people. No names, phones or dates of birth are read.
-- anchor = the person a member belongs to before any linking: same Aadhaar fingerprint = same person;
--          a member without a fingerprint starts as a person of its own.
-- Parameter :fqf_catalog = the FQF catalog of the SAME environment (config/group.yml), read-only.
WITH members AS (
  SELECT 'bhg' AS company_code, patient_id AS member_id, id_fingerprint, contact_fingerprint, city, gender
  FROM gold.dim_patient
  WHERE is_current AND patient_key <> -1

  UNION ALL
  SELECT 'fqf', master_customer_id, id_fingerprint, contact_fingerprint, city, gender
  FROM IDENTIFIER(:fqf_catalog || '.gold.dim_customer')
  WHERE is_current AND customer_key <> -1 AND customer_id = master_customer_id
)
SELECT *,
       CASE WHEN id_fingerprint IS NOT NULL THEN concat('id:', id_fingerprint)
            ELSE concat_ws(':', company_code, member_id) END AS anchor
FROM members
