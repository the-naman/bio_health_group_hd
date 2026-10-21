-- dim_payer: who paid, one row per combination of payer type, insurer and payment mode found in the events.
-- payer_type: self, insurance, corporate, fqf_loan (a Flexi Quant Finance loan), or 'not invoiced'.
SELECT DISTINCT payer_key, payer_type, insurer, pay_mode
FROM _bhg_events
