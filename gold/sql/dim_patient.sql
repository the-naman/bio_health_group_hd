-- dim_patient: every version of every patient WITHOUT name, phone, Aadhaar number or date of birth.
-- Age is not stored here: the fact carries the age band at the time of each event.
-- id_fingerprint      = keyed hash of the Aadhaar number (made in Bronze)
-- contact_fingerprint = keyed hash of phone + date of birth; lets the group spot look-alikes across both
--                       companies without anyone seeing a phone number. contact_fingerprint() is a function
--                       the Gold runner registers (it needs the group match key from the secret scope).
WITH patients AS (
  SELECT
    patient_id, gender, city, insurance_provider, id_fingerprint, phone, dob, is_current,
    CASE WHEN row_number() OVER (PARTITION BY patient_id ORDER BY valid_from) = 1
         THEN TIMESTAMP'1900-01-01 00:00:00' ELSE valid_from END               AS valid_from,
    coalesce(valid_to, TIMESTAMP'9999-12-31 23:59:59')                         AS valid_to
  FROM silver.patients
)
SELECT
  xxhash64(CAST(patient_id AS STRING), CAST(valid_from AS STRING)) AS patient_key,
  patient_id,
  gender,
  city,
  insurance_provider,
  id_fingerprint,
  contact_fingerprint(phone, dob)                                   AS contact_fingerprint,
  valid_from,
  valid_to,
  is_current
FROM patients

UNION ALL
--     key  patient_id gender     city       insurance id_fp contact_fp valid_from                      valid_to                        is_current
SELECT -1,  'unknown', 'unknown', 'unknown', NULL,     NULL, NULL,      TIMESTAMP'1900-01-01 00:00:00', TIMESTAMP'9999-12-31 23:59:59', TRUE
