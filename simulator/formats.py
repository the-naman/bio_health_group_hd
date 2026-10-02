"""Turn rows into the file shape of each source system.
 
  files, mysql, salesforce -> CSV (comma, header row)
  legacy                   -> pipe-delimited text, dd-mm-yyyy dates, no quoting
  excel                    -> CSV whose header names drift from file to file
  bureau_api, gateway      -> JSON lines, nested like a real API response
  bank                     -> PDF statement (text PDF, written without any library)
  branch_scans             -> PNG image of a cash deposit slip (needs Pillow)
"""
import csv, io, json, random, re
from datetime import datetime, timedelta, timezone
 
from simulator.world import BINARY, COLUMNS, NEW_COLUMNS, NEW_COL_DATE, source_of
 
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
 
 
def _pdf(pages):
    """Minimal text PDF: one list of lines per page, Courier 9pt, A4."""
    objs = [b"<< /Type /Catalog /Pages 2 0 R >>", None, b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier >>"]
    kids = []
    for lines in pages:
        text = ["BT /F1 9 Tf 40 800 Td 12 TL"]
        for line in lines:
            safe = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
            text.append(f"({safe}) Tj T*")
        text.append("ET")
        stream = "\n".join(text).encode("latin-1")
        objs.append(b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream")
        objs.append(f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 595 842] "
                    f"/Resources << /Font << /F1 3 0 R >> >> /Contents {len(objs)} 0 R >>".encode())
        kids.append(f"{len(objs)} 0 R")
    objs[1] = f"<< /Type /Pages /Kids [{' '.join(kids)}] /Count {len(kids)} >>".encode()
    out, offsets = bytearray(b"%PDF-1.4\n"), []
    for i, body in enumerate(objs, 1):
        offsets.append(len(out))
        out += f"{i} 0 obj\n".encode() + body + b"\nendobj\n"
    xref = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    for off in offsets:
        out += f"{off:010d} 00000 n \n".encode()
    out += f"trailer\n<< /Size {len(objs) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    return bytes(out)
 
 
def _bank_pdf(rows, file_date):
    head = ["STATE COMMERCIAL BANK - DAILY SETTLEMENT STATEMENT",
            "Customer: Flexi Quant Finance      Account ending: 4821      Currency: INR",
            f"Statement no: BST{file_date:%Y%m%d}      Statement date: {file_date}", "",
            f"{'Sr':<5}{'Value date':<12}{'Bank ref':<17}{'Narration':<12}{'Amount':>14}  Status", "-" * 70]
    body = [f"{i:<5}{x['value_date']:<12}{x['bank_ref']:<17}{x['loan_id']:<12}{x['amount']:>14}  {x['status']}"
            for i, x in enumerate(rows, 1)]
    total = sum(float(x["amount"]) for x in rows if x["status"] == "SETTLED")
    foot = ["-" * 70, f"Lines: {len(rows)}      Total settled: {total:.2f}"]
    per_page = 50
    chunks = [body[i:i + per_page] for i in range(0, len(body), per_page)] or [[]]
    pages = [head + chunk for chunk in chunks]
    pages[-1] += foot
    return _pdf(pages)
 
 
def _slip_png(rows, file_date):
    from PIL import Image, ImageDraw, ImageFont
    by_branch = {}
    for x in rows:
        n, amt = by_branch.get(x["branch_id"], (0, 0.0))
        by_branch[x["branch_id"]] = (n + int(x["receipts"]), amt + float(x["amount"]))
    days = sorted(x["deposit_date"] for x in rows)
    lines = ["FLEXI QUANT FINANCE", "CASH DEPOSIT SLIP", "",
             f"Slip no: CDS{file_date:%Y%m%d}",
             f"Period: {days[0]} to {days[-1]}",
             "Bank: State Commercial Bank   Account ending: 4821", "",
             f"{'Branch':<10}{'Receipts':>10}{'Amount (INR)':>18}"]
    for b in sorted(by_branch):
        lines.append(f"{b:<10}{by_branch[b][0]:>10}{by_branch[b][1]:>18.2f}")
    lines += [f"{'TOTAL':<10}{sum(v[0] for v in by_branch.values()):>10}{sum(v[1] for v in by_branch.values()):>18.2f}",
              "", "Deposited by: branch cashier"]
    try:
        font = ImageFont.load_default(size=20)
    except TypeError:                                   # older Pillow: small built-in font
        font = ImageFont.load_default()
    img = Image.new("L", (640, 40 + 30 * len(lines)), 255)
    draw = ImageDraw.Draw(img)
    for i, line in enumerate(lines):
        draw.text((30, 20 + 30 * i), line, fill=0, font=font)
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return buf.getvalue()
 
 
def render(env, table, rows, file_date, broken=False):
    """File body for one table and one file date (text, or bytes for documents).
    broken=True returns an unreadable file (planned incident)."""
    src = source_of(table)
    if table in BINARY:
        body = _bank_pdf(rows, file_date) if src == "bank" else _slip_png(rows, file_date)
        return body[: len(body) // 2] if broken else body                           # cut-off file
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
 