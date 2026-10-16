-- dim_doctor: every version of every doctor (specialty, status, department), no name or phone.
-- Silver keeps the history (SCD2). Here the first version is opened from 1900-01-01 and the last one
-- runs to 9999-12-31, so every event finds exactly one version. Key -1 = unknown doctor.
WITH doctors AS (
  SELECT
    doctor_id, specialty, status, dept_id, is_current,
    CASE WHEN row_number() OVER (PARTITION BY doctor_id ORDER BY valid_from) = 1
         THEN TIMESTAMP'1900-01-01 00:00:00' ELSE valid_from END               AS valid_from,
    coalesce(valid_to, TIMESTAMP'9999-12-31 23:59:59')                         AS valid_to
  FROM silver.doctors
)
SELECT
  xxhash64(CAST(d.doctor_id AS STRING), CAST(d.valid_from AS STRING)) AS doctor_key,   -- one key per version
  d.doctor_id,
  d.specialty,
  d.status,
  d.dept_id,
  dp.dept_name                                                         AS department,
  dp.hospital_id,
  d.valid_from,
  d.valid_to,
  d.is_current
FROM doctors d
LEFT JOIN silver.departments dp ON d.dept_id = dp.dept_id

UNION ALL
--     key  doctor_id  specialty  status dept_id department hospital_id valid_from                      valid_to                        is_current
SELECT -1,  'unknown', 'unknown', NULL,  NULL,   'unknown', NULL,       TIMESTAMP'1900-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59', TRUE
