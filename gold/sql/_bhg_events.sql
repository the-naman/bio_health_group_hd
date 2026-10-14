-- _bhg_events (helper step, not saved): every BHG money event in one shape, with the keys of the
-- patient, doctor, hospital and payer attached. fact_bhg_event and dim_payer are both read from it.
--
--   charge      consult (completed appointment), treatment, room (admission invoice minus its treatments:
--               ward and stay), pharmacy (sale)
--   adjustment  discount (negative), tax
--   collection  payment
-- For every invoice: charges + discount + tax = net amount, and payments = net amount when paid.
-- Deleted source rows (is_deleted) are left out.
WITH
admissions AS (
  SELECT admission_id, patient_id AS adm_patient, doctor_id AS adm_doctor, hospital_id AS adm_hospital
  FROM silver.admissions WHERE NOT is_deleted
),
invoices AS (            -- every invoice with its insurer, its admission and the doctor who treated
  SELECT i.*, c.insurer, a.adm_patient, a.adm_doctor, a.adm_hospital,
         coalesce(a.adm_doctor, ap.doctor_id) AS doc
  FROM (SELECT * FROM silver.invoices WHERE NOT is_deleted) i
  LEFT JOIN (SELECT invoice_id, insurer FROM silver.insurance_claims WHERE NOT is_deleted) c ON i.invoice_id = c.invoice_id
  LEFT JOIN admissions a ON i.admission_id = a.admission_id
  LEFT JOIN (SELECT appt_id, doctor_id FROM silver.appointments WHERE NOT is_deleted) ap ON i.appt_id = ap.appt_id
),
admission_invoice AS (
  SELECT admission_id, invoice_id, payer_type, insurer FROM invoices WHERE admission_id IS NOT NULL
),
treated AS (             -- what the treatments of each admission cost
  SELECT admission_id, sum(cost) AS treated FROM silver.treatments WHERE NOT is_deleted GROUP BY admission_id
),

events AS (
  -- consult: a completed appointment
  SELECT 'consult' AS event_type, 'charge' AS event_group, a.appt_id AS event_id,
         CAST(a.appt_ts AS TIMESTAMP) AS event_ts, CAST(a.consult_fee AS DECIMAL(18,2)) AS amount, 1 AS quantity,
         a.patient_id, a.doctor_id, CAST(NULL AS STRING) AS hospital_id, i.invoice_id,
         CAST(NULL AS STRING) AS admission_id, i.payer_type, CAST(NULL AS STRING) AS insurer, CAST(NULL AS STRING) AS pay_mode
  FROM (SELECT * FROM silver.appointments WHERE NOT is_deleted AND status = 'completed') a
  LEFT JOIN (SELECT appt_id, invoice_id, payer_type FROM silver.invoices WHERE NOT is_deleted) i ON a.appt_id = i.appt_id

  UNION ALL  -- treatment during an admission
  SELECT 'treatment', 'charge', t.treatment_id, CAST(t.treatment_date AS TIMESTAMP), CAST(t.cost AS DECIMAL(18,2)), 1,
         a.adm_patient, t.doctor_id, a.adm_hospital, ai.invoice_id, t.admission_id, ai.payer_type, ai.insurer, NULL
  FROM (SELECT * FROM silver.treatments WHERE NOT is_deleted) t
  LEFT JOIN admissions a         ON t.admission_id = a.admission_id
  LEFT JOIN admission_invoice ai ON t.admission_id = ai.admission_id

  UNION ALL  -- room: the admission invoice minus the treatments already counted (ward and stay)
  SELECT 'room', 'charge', i.invoice_id, CAST(i.invoice_date AS TIMESTAMP),
         CAST(i.gross_amt - coalesce(tr.treated, 0) AS DECIMAL(18,2)), 1,
         i.patient_id, i.doc, i.adm_hospital, i.invoice_id, i.admission_id, i.payer_type, i.insurer, NULL
  FROM invoices i
  LEFT JOIN treated tr ON i.admission_id = tr.admission_id
  WHERE i.admission_id IS NOT NULL

  UNION ALL  -- pharmacy sale (paid by the patient)
  SELECT 'pharmacy', 'charge', s.sale_id, CAST(s.sale_ts AS TIMESTAMP), CAST(s.qty * s.unit_price AS DECIMAL(18,2)),
         CAST(s.qty AS INT), s.patient_id, NULL, s.hospital_id, NULL, NULL, 'self', NULL, NULL
  FROM silver.pharmacy_sales s WHERE NOT s.is_deleted

  UNION ALL  -- discount on an invoice (negative)
  SELECT 'discount', 'adjustment', i.invoice_id, CAST(i.invoice_date AS TIMESTAMP), CAST(-i.discount_amt AS DECIMAL(18,2)), NULL,
         i.patient_id, i.doc, i.adm_hospital, i.invoice_id, i.admission_id, i.payer_type, i.insurer, NULL
  FROM invoices i WHERE i.discount_amt <> 0

  UNION ALL  -- tax on an invoice
  SELECT 'tax', 'adjustment', i.invoice_id, CAST(i.invoice_date AS TIMESTAMP), CAST(i.tax_amt AS DECIMAL(18,2)), NULL,
         i.patient_id, i.doc, i.adm_hospital, i.invoice_id, i.admission_id, i.payer_type, i.insurer, NULL
  FROM invoices i WHERE i.tax_amt <> 0

  UNION ALL  -- payment of an invoice
  SELECT 'payment', 'collection', p.payment_id, CAST(p.pay_date AS TIMESTAMP), CAST(p.amount AS DECIMAL(18,2)), NULL,
         i.patient_id, i.doc, i.adm_hospital, p.invoice_id, i.admission_id, i.payer_type, i.insurer, p.mode
  FROM (SELECT * FROM silver.payments WHERE NOT is_deleted) p
  LEFT JOIN (SELECT invoice_id, patient_id, admission_id, doc, adm_hospital, payer_type, insurer FROM invoices) i
         ON p.invoice_id = i.invoice_id
),

patient_versions AS (    -- same versions and keys as dim_patient
  SELECT patient_id, dob, valid_from, valid_to, xxhash64(CAST(patient_id AS STRING), CAST(valid_from AS STRING)) AS patient_key
  FROM (SELECT patient_id, dob,
               CASE WHEN row_number() OVER (PARTITION BY patient_id ORDER BY valid_from) = 1
                    THEN TIMESTAMP'1900-01-01 00:00:00' ELSE valid_from END AS valid_from,
               coalesce(valid_to, TIMESTAMP'9999-12-31 23:59:59') AS valid_to
        FROM silver.patients)
),
doctor_versions AS (     -- same versions and keys as dim_doctor, with the doctor's hospital
  SELECT v.doctor_id, v.valid_from, v.valid_to, xxhash64(CAST(v.doctor_id AS STRING), CAST(v.valid_from AS STRING)) AS doctor_key,
         dp.hospital_id AS doc_hospital
  FROM (SELECT doctor_id, dept_id,
               CASE WHEN row_number() OVER (PARTITION BY doctor_id ORDER BY valid_from) = 1
                    THEN TIMESTAMP'1900-01-01 00:00:00' ELSE valid_from END AS valid_from,
               coalesce(valid_to, TIMESTAMP'9999-12-31 23:59:59') AS valid_to
        FROM silver.doctors) v
  LEFT JOIN silver.departments dp ON v.dept_id = dp.dept_id
),

keyed AS (               -- the version of patient and doctor that was valid when the event happened
  SELECT e.event_type, e.event_group, e.event_id, e.event_ts, e.amount, CAST(e.quantity AS INT) AS quantity,
         CAST(e.patient_id AS STRING) AS patient_id, CAST(e.doctor_id AS STRING) AS doctor_id,
         CAST(coalesce(e.hospital_id, d.doc_hospital) AS STRING) AS hospital_id,   -- no hospital on the event: the doctor's
         CAST(e.invoice_id AS STRING) AS invoice_id, CAST(e.admission_id AS STRING) AS admission_id,
         CAST(coalesce(e.payer_type, 'not invoiced') AS STRING) AS payer_type,
         CAST(e.insurer AS STRING) AS insurer, CAST(e.pay_mode AS STRING) AS pay_mode,
         -- no id on the event = not applicable (NULL); an id without a matching version = unknown (-1)
         CAST(CASE WHEN e.patient_id IS NULL THEN NULL ELSE coalesce(p.patient_key, -1) END AS BIGINT) AS patient_key,
         CAST(CASE WHEN e.doctor_id  IS NULL THEN NULL ELSE coalesce(d.doctor_key,  -1) END AS BIGINT) AS doctor_key,
         p.dob
  FROM events e
  LEFT JOIN patient_versions p
         ON e.patient_id = p.patient_id AND e.event_ts >= p.valid_from AND e.event_ts < p.valid_to
  LEFT JOIN doctor_versions d
         ON e.doctor_id = d.doctor_id AND e.event_ts >= d.valid_from AND e.event_ts < d.valid_to
)

SELECT k.*,
       CAST(CASE WHEN k.hospital_id IS NOT NULL AND h.hospital_id IS NOT NULL THEN xxhash64(CAST(k.hospital_id AS STRING))
                 WHEN k.hospital_id IS NOT NULL OR k.doctor_key = -1 THEN -1         -- unknown doctor -> unknown hospital
            END AS BIGINT)                                                           AS hospital_key,
       xxhash64(coalesce(k.payer_type, '~'), coalesce(k.insurer, '~'), coalesce(k.pay_mode, '~')) AS payer_key
FROM keyed k
LEFT JOIN (SELECT hospital_id FROM silver.hospitals) h ON k.hospital_id = h.hospital_id
