"""Planted mess (Phase 4 section 10). Deterministic per environment, table and file date.

The parent (bhg) is a mature company: about half the base rates.
The child (fqf) is a fresh acquisition with scattered systems: about double.
JSON sources (bureau_api, gateway) carry typed values, so text-format mess is not applied to them;
they get their own JSON mess in formats.py.
"""
import random

from simulator.world import company_of, source_of

FACTOR = {"bhg": 0.5, "fqf": 2.0}
DATE_COLS = {"opened_date", "joining_date", "dob", "invoice_date", "pay_date", "treatment_date",
             "disb_date", "paid_date", "start_date", "due_date", "effective_from", "effective_to"}
AMOUNT_COLS = {"consult_fee", "cost", "unit_price", "gross_amt", "discount_amt", "tax_amt", "net_amt",
               "amount", "principal", "late_fee", "principal_due", "interest_due", "emi_amount",
               "max_exposure_per_customer"}
OPTIONAL = {"insurance_provider", "discharge_ts", "Employment__c"}
PHONE_COLS = {"phone", "Phone"}
CITY_ALIAS = {"Bengaluru": ["Bangalore", "BLR", "bengaluru "], "Mumbai": ["Bombay", "MUMBAI"],
              "Delhi": ["New Delhi", "delhi"], "Chennai": ["Madras", "chennai"], "Hyderabad": ["Hyd", "HYDERABAD"]}
DUP_TABLES = {"appointments", "pharmacy_sales", "payments", "repayments", "customers"}
TEXT_SOURCES = {"files", "legacy", "excel"}      # text exports where dates and amounts get mangled
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _date(v, r):
    if len(v) < 10 or v[4] != "-":
        return v
    y, m, d = v[:4], v[5:7], v[8:10]
    return r.choice([f"{d}/{m}/{y}", f"{d}-{MONTHS[int(m) - 1]}-{y}", f"{m}/{d}/{y}"])


def _phone(v, r):
    if not v or len(v) != 10:
        return v
    return r.choice([f"+91-{v}", f"0{v}", f"{v[:5]} {v[5:]}", f"+91 {v[:5]}-{v[5:]}"])


def apply(env, table, file_date, rows):
    r = random.Random(f"mess-{env}-{table}-{file_date}")
    k = FACTOR[company_of(table)]
    src = source_of(table)
    text = src in TEXT_SOURCES
    out = []
    for row in rows:
        row = dict(row)
        for c, v in row.items():
            if c in OPTIONAL and r.random() < 0.02 * k:
                row[c] = r.choice(["NULL", "N/A", "-", "null"])          # fake null
                continue
            if not isinstance(v, str) or v == "":
                continue
            if text and c in DATE_COLS and r.random() < 0.03 * k:
                row[c] = _date(v, r)                                      # mixed date formats
            elif text and c in AMOUNT_COLS and r.random() < 0.03 * k:
                row[c] = f"₹{float(v):,.2f}"                              # amount as text
            elif c in PHONE_COLS and r.random() < min(0.30 * k, 0.6):
                row[c] = _phone(v, r)
            elif c in ("city", "City__c") and v in CITY_ALIAS and r.random() < 0.05 * k:
                row[c] = r.choice(CITY_ALIAS[v])
            elif c in ("full_name", "Name", "status") and r.random() < 0.05 * k:
                row[c] = r.choice([v.upper(), v.lower(), f"  {v} "])
        for c in ("aadhaar", "Aadhaar__c"):                               # invalid Aadhaar
            if c in row and r.random() < 0.005 * k:
                row[c] = row[c][:11]
        out.append(row)
        if table in DUP_TABLES and r.random() < 0.02 * k:
            out.append(dict(row))                                         # duplicate row
    return out
