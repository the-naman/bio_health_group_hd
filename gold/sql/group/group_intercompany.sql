-- group_intercompany: one row per BHG invoice paid by an FQF loan. What BHG received vs what FQF paid out.
-- status: matched | amount_differs | bhg_only (BHG got paid, no FQF payout found) | fqf_only (payout, no BHG payment)
-- Parameters: :fqf_catalog (FQF catalog of the same environment), :ic_payer_type (config/group.yml: fqf_loan)
WITH
bhg AS (
  SELECT e.invoice_id, sum(e.amount) AS bhg_received, min(e.date_key) AS bhg_date_key
  FROM gold.fact_bhg_event e
  JOIN (SELECT payer_key FROM gold.dim_payer WHERE payer_type = :ic_payer_type) p ON e.payer_key = p.payer_key
  WHERE e.event_type = 'payment' AND e.invoice_id IS NOT NULL
  GROUP BY e.invoice_id
),
fqf AS (
  SELECT bhg_invoice_id AS invoice_id, sum(amount) AS fqf_paid_out, min(date_key) AS fqf_date_key
  FROM IDENTIFIER(:fqf_catalog || '.gold.fact_fqf_event')
  WHERE event_type = 'disbursement' AND is_hospital_bill
  GROUP BY bhg_invoice_id
)
SELECT
  coalesce(b.invoice_id, f.invoice_id)                          AS invoice_id,
  b.bhg_received,
  b.bhg_date_key,
  f.fqf_paid_out,
  f.fqf_date_key,
  coalesce(b.bhg_received, 0) - coalesce(f.fqf_paid_out, 0)     AS difference,
  CASE WHEN b.bhg_received IS NULL THEN 'fqf_only'
       WHEN f.fqf_paid_out IS NULL THEN 'bhg_only'
       WHEN abs(b.bhg_received - f.fqf_paid_out) > 0.01 THEN 'amount_differs'
       ELSE 'matched' END                                       AS status
FROM bhg b
FULL OUTER JOIN fqf f ON b.invoice_id = f.invoice_id
