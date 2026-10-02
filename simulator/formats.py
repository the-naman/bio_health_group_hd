"""Turn rows into the file shape of each source system.

  files, mysql, salesforce -> CSV (comma, header row)
  legacy                   -> pipe-delimited text, dd-mm-yyyy dates, no quoting
  excel                    -> CSV whose header names drift from file to file
  bureau_api, gateway      -> JSON lines, nested like a real API response
"""
import csv, io, json, random, re
from datetime import datetime, timedelta, timezone

from simulator.world import COLUMNS, NEW_COLUMNS, NEW_COL_DATE, source_of

IST = timezone(timedelta(hours=5, minutes=30))
ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})(.*)$")

# excel: the same column arrives under different headings (index 0 = the agreed name)
HEADER_DRIFT = {
    "policy_id": ["policy_id", "Policy ID", "PolicyId"],
    "product_id": ["product_id", "Product", "product code"],
    "max_exposure_per_customer": ["max_exposure_per_customer", "Max Exposure", "max_exposure"],
    "max_dti": ["max_dti", "Max DTI %", "dti_limit"],
    "min_bureau_score": ["min_bureau_score", "Min Score", "minimum_cibil"],
    "effective_from": ["effective_from", "Effective From", "valid_from"],
    "effective_to": ["effective_to", "Effective To", "valid_to"],
    "last_modified": ["last_modified", "Last Modified", "updated on"],
}


def columns(table, file_date):
    cols = list(COLUMNS[table])
    if table in NEW_COLUMNS and file_date >= NEW_COL_DATE:
        cols.append(NEW_COLUMNS[table])
    return cols


def _delimited(table, rows, file_date, delimiter, header=None, quoting=csv.QUOTE_MINIMAL):
    cols = columns(table, file_date)
    buf = io.StringIO()
    w = csv.writer(buf, delimiter=delimiter, quoting=quoting, lineterminator="\n", escapechar="\\")
    w.writerow(header or cols)
    for row in rows:
        w.writerow([row.get(c, "") for c in cols])
    return buf.getvalue()


def _legacy(table, rows, file_date, delimiter="|"):
    out = []
    for row in rows:
        row = dict(row)
        for c, v in row.items():
            m = ISO_DATE.match(v) if isinstance(v, str) else None
            if m:
                row[c] = f"{m.group(3)}-{m.group(2)}-{m.group(1)}{m.group(4)}"     # dd-mm-yyyy[ hh:mm:ss]
        out.append(row)
    return _delimited(table, out, file_date, delimiter, quoting=csv.QUOTE_NONE)


def _excel(env, table, rows, file_date):
    r = random.Random(f"drift-{env}-{table}-{file_date}")
    v = r.choices([0, 1, 2], [50, 25, 25])[0]
    header = [HEADER_DRIFT.get(c, [c] * 3)[v] for c in columns(table, file_date)]
    return _delimited(table, rows, file_date, ",", header=header)


def _epoch(text):
    return int(datetime.fromisoformat(text).replace(tzinfo=IST).timestamp())


def _paise(text):
    return int(round(float(text) * 100))


def _json_lines(env, table, rows, file_date):
    r = random.Random(f"json-{env}-{table}-{file_date}")
    lines = []
    for x in rows:
        if table == "repayments":                       # payment gateway
            obj = {"id": "pay_" + x["repay_id"], "entity": "payment", "amount": _paise(x["amount"]),
                   "currency": "INR", "status": "captured", "method": x["mode"],
                   "created_at": _epoch(x["paid_date"] + x["last_modified"][10:]),
                   "notes": {"loan_id": x["loan_id"], "emi_no": x["emi_no"]},
                   "late_fee": _paise(x["late_fee"]), "updated_at": _epoch(x["last_modified"])}
            if r.random() < 0.04:
                obj["amount"] = str(obj["amount"])      # number sent as text
            if r.random() < 0.02:
                obj["notes"].pop("emi_no")              # missing field
        else:                                           # credit bureau
            score = int(x["bureau_score"])
            obj = {"request_id": x["check_id"], "application_ref": x["application_id"],
                   "pulled_at": x["check_ts"].replace(" ", "T") + "+05:30",
                   "score": {"value": score, "model": "V3",
                             "band": "good" if score >= 700 else "fair" if score >= 650 else "poor"},
                   "obligations": {"dti_pct": float(x["dti_pct"])},
                   "decision": {"result": x["decision"],
                                "reasons": [] if x["decision"] == "approve" else ["policy_check_failed"]},
                   "updated_at": x["last_modified"].replace(" ", "T") + "+05:30"}
            if r.random() < 0.04:
                obj["score"]["value"] = None            # bureau returned no score
        lines.append(json.dumps(obj, separators=(",", ":")))
    return "\n".join(lines) + ("\n" if lines else "")


def render(env, table, rows, file_date, broken=False):
    """File body for one table and one file date. broken=True returns an unreadable file (planned incident)."""
    src = source_of(table)
    if src in ("bureau_api", "gateway"):
        body = _json_lines(env, table, rows, file_date)
        return (body[: max(len(body) - 25, 0)] + '{"id":') if broken else body      # cut-off JSON
    if src == "legacy":
        return _legacy(table, rows, file_date, delimiter="," if broken else "|")   # wrong delimiter
    if broken:
        return _delimited(table, rows, file_date, ";")                             # wrong delimiter
    if src == "excel":
        return _excel(env, table, rows, file_date)
    return _delimited(table, rows, file_date, ",")
