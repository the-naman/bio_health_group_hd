-- dim_hospital: one row per hospital, plus the "unknown" hospital (key -1) for events whose hospital is missing.
-- hospital_key is a hash of hospital_id, so it stays the same on every rebuild.
SELECT
  xxhash64(CAST(hospital_id AS STRING)) AS hospital_key,
  hospital_id,
  hospital_name,
  city,
  state,
  beds,
  opened_date,
  NOT is_deleted                        AS is_active
FROM silver.hospitals

UNION ALL
--     key  hospital_id hospital_name city  state beds  opened_date is_active
SELECT -1,  'unknown',  'unknown',    NULL, NULL, NULL, NULL,       NULL
