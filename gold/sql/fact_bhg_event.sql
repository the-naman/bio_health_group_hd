-- fact_bhg_event: one row per money event at BHG (from the helper step _bhg_events).
-- Read it as: on date_key, event_type of amount happened for patient_key at hospital_key, paid by payer_key.
-- age_at_event / age_band replace the date of birth, so Gold holds no birth dates.
SELECT
  xxhash64(CAST(event_type AS STRING), CAST(event_id AS STRING)) AS event_key,
  event_id,
  event_type,
  event_group,
  CAST(date_format(event_ts, 'yyyyMMdd') AS INT)                 AS date_key,
  patient_key,
  doctor_key,
  hospital_key,
  payer_key,
  invoice_id,
  admission_id,
  amount,
  quantity,
  payer_type = 'fqf_loan'                                        AS is_fqf_loan,   -- paid by a Flexi Quant Finance loan
  age                                                            AS age_at_event,
  CASE WHEN age IS NULL THEN 'unknown'
       WHEN age < 18 THEN '0-17'
       WHEN age < 30 THEN '18-29'
       WHEN age < 45 THEN '30-44'
       WHEN age < 60 THEN '45-59'
       ELSE '60+' END                                            AS age_band
FROM (SELECT *, CAST(floor(months_between(to_date(event_ts), dob) / 12) AS INT) AS age FROM _bhg_events)
