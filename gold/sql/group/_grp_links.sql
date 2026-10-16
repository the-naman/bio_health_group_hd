-- _grp_links (helper step, not saved): hospital-bill links whose two sides are NOT yet the same person,
-- with each side's anchor (pa = patient's, ca = customer's) and Aadhaar fingerprint (pfp, cfp).
SELECT l.invoice_id, l.patient_id, l.customer_id,
       p.anchor AS pa, p.id_fingerprint AS pfp,
       c.anchor AS ca, c.id_fingerprint AS cfp
FROM _grp_invoice_links l
JOIN _grp_members p ON p.company_code = 'bhg' AND p.member_id = l.patient_id
JOIN _grp_members c ON c.company_code = 'fqf' AND c.member_id = l.customer_id
WHERE p.anchor <> c.anchor
