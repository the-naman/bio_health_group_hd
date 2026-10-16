-- _grp_invoice_links (helper step, not saved): pairs of (BHG patient, FQF master customer) where the
-- customer's hospital-bill loan paid the patient's BHG invoice.
-- Parameter :fqf_catalog = the FQF catalog of the SAME environment, read-only.
WITH
bhg AS (
  SELECT DISTINCT e.invoice_id, p.patient_id
  FROM (SELECT DISTINCT invoice_id, patient_key FROM gold.fact_bhg_event
        WHERE invoice_id IS NOT NULL AND patient_key IS NOT NULL AND patient_key <> -1) e
  JOIN gold.dim_patient p ON e.patient_key = p.patient_key
),
fqf AS (
  SELECT e.bhg_invoice_id AS invoice_id, c.master_customer_id AS customer_id
  FROM IDENTIFIER(:fqf_catalog || '.gold.fact_fqf_event') e
  JOIN IDENTIFIER(:fqf_catalog || '.gold.dim_customer') c ON e.customer_key = c.customer_key
  WHERE e.event_type = 'disbursement' AND e.bhg_invoice_id IS NOT NULL AND e.customer_key <> -1
)
SELECT DISTINCT b.invoice_id, b.patient_id, f.customer_id
FROM bhg b JOIN fqf f ON b.invoice_id = f.invoice_id
