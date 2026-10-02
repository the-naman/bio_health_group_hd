"""Deterministic synthetic world for Bio Health Group (bhg) and Flexi Quant Finance (fqf).

The same code runs in both repos. For one environment it always builds the same world,
so links between the companies (shared people, hospital-bill loans) match in both.
Every row version carries `_emit`: the business date of the daily file that sends it.
"""
import random, string
from datetime import date, datetime, timedelta

BHG_START = date(2025, 4, 5)
FQF_START = date(2025, 9, 5)
CUTOVER = date(2026, 10, 4)          # last day of the full load
NEW_COL_DATE = date(2026, 10, 15)    # patients.preferred_language, loans.channel appear
SCALE = {"dev": 0.10, "test": 0.50, "prod": 1.00}

CITIES = [("Bengaluru", "Karnataka"), ("Mumbai", "Maharashtra"), ("Delhi", "Delhi"),
          ("Chennai", "Tamil Nadu"), ("Hyderabad", "Telangana")]
FIRST_M = ["Aarav", "Vihaan", "Arjun", "Rohan", "Karan", "Rahul", "Aditya", "Siddharth", "Nikhil", "Varun",
           "Imran", "Joseph", "Harish", "Manoj", "Suresh", "Pranav", "Kabir", "Ishaan", "Dev", "Yash"]
FIRST_F = ["Ananya", "Diya", "Isha", "Priya", "Sneha", "Kavya", "Meera", "Riya", "Pooja", "Neha",
           "Fatima", "Mary", "Lakshmi", "Divya", "Swati", "Aditi", "Sara", "Tara", "Nisha", "Anjali"]
LAST = ["Sharma", "Verma", "Iyer", "Reddy", "Nair", "Patel", "Gupta", "Rao", "Khan", "Singh",
        "Das", "Menon", "Joshi", "Kulkarni", "Shetty", "Pillai", "Bose", "Mehta", "Chopra", "Fernandes"]
SPECIALTIES = ["General Medicine", "Cardiology", "Orthopaedics", "Paediatrics", "Gynaecology", "Oncology"]
INSURERS = ["Star Health", "HDFC Ergo", "ICICI Lombard", "Niva Bupa", "Care Health"]
PROCEDURES = [("PRC01", "Blood panel", 1500), ("PRC02", "MRI scan", 9000), ("PRC03", "CT scan", 6000),
              ("PRC04", "Physiotherapy", 1200), ("PRC05", "Minor surgery", 25000), ("PRC06", "Major surgery", 120000),
              ("PRC07", "Chemotherapy cycle", 45000), ("PRC08", "Dialysis", 3500)]
DRUGS = [("DRG01", "Paracetamol 650", 30), ("DRG02", "Amoxicillin 500", 120), ("DRG03", "Metformin 500", 60),
         ("DRG04", "Atorvastatin 10", 150), ("DRG05", "Pantoprazole 40", 90), ("DRG06", "Insulin pen", 650),
         ("DRG07", "Cetirizine 10", 25), ("DRG08", "Azithromycin 500", 110)]
PRODUCTS = [("PRD01", "Hospital Bill Loan", "hospital_bill", 13.5, 20000, 500000, 24),
            ("PRD02", "Personal Loan", "personal", 15.0, 50000, 1000000, 36),
            ("PRD03", "Salary Advance", "personal", 18.0, 10000, 200000, 12),
            ("PRD04", "Small Business Loan", "business", 16.5, 100000, 1500000, 36)]

COLUMNS = {
    # bhg
    "hospitals": ["hospital_id", "hospital_name", "city", "state", "beds", "opened_date", "last_modified"],
    "departments": ["dept_id", "hospital_id", "dept_name", "specialty", "last_modified"],
    "doctors": ["doctor_id", "dept_id", "full_name", "specialty", "phone", "joining_date", "status", "updated_at", "is_deleted"],
    "patients": ["patient_id", "aadhaar", "full_name", "gender", "dob", "phone", "city", "insurance_provider", "last_modified"],
    "appointments": ["appt_id", "patient_id", "doctor_id", "appt_ts", "status", "consult_fee", "last_modified"],
    "admissions": ["admission_id", "patient_id", "hospital_id", "dept_id", "doctor_id", "admit_ts", "discharge_ts", "ward_type", "status", "last_modified"],
    "treatments": ["treatment_id", "admission_id", "doctor_id", "procedure_code", "procedure_name", "treatment_date", "cost", "last_modified"],
    "pharmacy_sales": ["sale_id", "hospital_id", "patient_id", "drug_code", "drug_name", "qty", "unit_price", "sale_ts", "last_modified"],
    "invoices": ["invoice_id", "patient_id", "admission_id", "appt_id", "invoice_date", "gross_amt", "discount_amt", "tax_amt", "net_amt", "payer_type", "status", "last_modified"],
    "payments": ["payment_id", "invoice_id", "pay_date", "amount", "mode", "payment_ref", "last_modified"],
    "insurance_claims": ["Id", "Invoice__c", "Patient__c", "Insurer__c", "ClaimAmount__c", "ApprovedAmount__c", "Status__c", "SubmittedDate__c", "LastModifiedDate", "IsDeleted"],
    # fqf
    "branches": ["branch_id", "branch_name", "city", "state", "opened_date", "updated_at", "is_deleted"],
    "loan_products": ["product_id", "product_name", "loan_type", "rate_pct", "min_amt", "max_amt", "max_tenure_m", "updated_at", "is_deleted"],
    "emi_schedule": ["loan_id", "emi_no", "due_date", "principal_due", "interest_due", "emi_amount", "updated_at", "is_deleted"],
    "customers": ["Id", "Name", "Aadhaar__c", "Phone", "BirthDate__c", "Gender__c", "City__c", "Branch__c", "MonthlyIncome__c", "Employment__c", "LastModifiedDate", "IsDeleted"],
    "loan_applications": ["Id", "Customer__c", "Product__c", "Branch__c", "Amount__c", "Tenure__c", "BHG_Invoice__c", "Status__c", "AppliedDate__c", "LastModifiedDate", "IsDeleted"],
    "collections": ["Id", "Loan__c", "DPD__c", "Bucket__c", "Action__c", "PromiseDate__c", "LastModifiedDate", "IsDeleted"],
    "credit_checks": ["check_id", "application_id", "bureau_score", "dti_pct", "decision", "check_ts", "last_modified"],
    "loans": ["loan_id", "application_id", "customer_id", "product_id", "branch_id", "principal", "rate_pct", "tenure_m", "start_date", "status", "last_modified"],
    "disbursements": ["disb_id", "loan_id", "disb_date", "amount", "payee_type", "bhg_invoice_id", "bank_ref", "last_modified"],
    "repayments": ["repay_id", "loan_id", "emi_no", "paid_date", "amount", "mode", "late_fee", "last_modified"],
    "credit_policy": ["policy_id", "product_id", "max_exposure_per_customer", "max_dti", "min_bureau_score", "effective_from", "effective_to", "last_modified"],
}
NEW_COLUMNS = {"patients": "preferred_language", "loans": "channel"}
SOURCE = {
    # bhg (parent): 3 sources, all CSV. Tables not listed here come from "files".
    "doctors": "mysql", "insurance_claims": "salesforce",
    # fqf (child): 6 scattered sources, 4 formats
    "customers": "salesforce", "loan_applications": "salesforce", "collections": "salesforce",
    "branches": "mysql", "loan_products": "mysql",
    "loans": "legacy", "disbursements": "legacy", "emi_schedule": "legacy",   # pipe-delimited text
    "credit_checks": "bureau_api",                                              # JSON lines
    "repayments": "gateway",                                                    # JSON lines
    "credit_policy": "excel",                                                   # CSV, drifting headers
}
EXT = {"legacy": "txt", "bureau_api": "json", "gateway": "json"}               # everything else: csv
TABLES = {
    "bhg": ["hospitals", "departments", "doctors", "patients", "appointments", "admissions", "treatments",
            "pharmacy_sales", "invoices", "payments", "insurance_claims"],
    "fqf": ["branches", "loan_products", "credit_policy", "customers", "loan_applications", "credit_checks",
            "loans", "disbursements", "emi_schedule", "repayments", "collections"],
}


def source_of(table):
    return SOURCE.get(table, "files")


def ext_of(table):
    return EXT.get(source_of(table), "csv")


def company_of(table):
    return "bhg" if table in TABLES["bhg"] else "fqf"


class World:
    def __init__(self, env, end_date):
        self.env, self.end, self.s = env, end_date, SCALE[env]
        self.r = random.Random(f"lh-world-{env}")
        self.rows = {t: [] for t in COLUMNS}
        self.seq = {}
        self.people, self.patients, self.customers = [], {}, {}
        self.fqf_cust_of_person = {}
        self.open_adm, self.loans, self.due = [], {}, {}
        self.invoice_due = {}           # invoice_id -> (net, patient_id, payer)
        self.loan_for_invoice = {}

    # ---------- helpers ----------
    def n(self, mean):
        mean *= self.s
        k = int(mean)
        return k + (1 if self.r.random() < mean - k else 0)

    def nid(self, prefix, width):
        self.seq[prefix] = self.seq.get(prefix, 0) + 1
        return f"{prefix}{self.seq[prefix]:0{width}d}"

    def sfid(self, prefix):
        return prefix + "".join(self.r.choices(string.ascii_letters + string.digits, k=15))

    def ts(self, d, h0=8, h1=20):
        return datetime(d.year, d.month, d.day, self.r.randint(h0, h1), self.r.randint(0, 59), self.r.randint(0, 59))

    def add(self, table, row, emit):
        row["_emit"] = emit
        self.rows[table].append(row)

    def money(self, x):
        return f"{x:.2f}"

    def new_person(self, d):
        g = self.r.choice("MF")
        first = self.r.choice(FIRST_M if g == "M" else FIRST_F)
        age = self.r.randint(18, 80)
        p = {
            "aadhaar": self.r.choice("01") + "".join(self.r.choices(string.digits, k=11)),
            "name": f"{first} {self.r.choice(LAST)}", "gender": g,
            "dob": d - timedelta(days=age * 365 + self.r.randint(0, 364)),
            "phone": self.r.choice("6789") + "".join(self.r.choices(string.digits, k=9)),
            "city": self.r.randrange(5),
        }
        self.people.append(p)
        return p

    # ---------- bhg ----------
    def bhg_masters(self, d):
        for i, (city, state) in enumerate(CITIES, 1):
            hid = f"HSP{i:02d}"
            self.add("hospitals", {"hospital_id": hid, "hospital_name": f"Bio Health {city}", "city": city,
                                   "state": state, "beds": str(150 + 50 * i), "opened_date": f"{2008 + i}-06-01",
                                   "last_modified": self.ts(d).isoformat(sep=" ")}, d)
            for j, sp in enumerate(SPECIALTIES, 1):
                self.add("departments", {"dept_id": f"DEP{i:02d}{j:02d}", "hospital_id": hid, "dept_name": sp,
                                         "specialty": sp, "last_modified": self.ts(d).isoformat(sep=" ")}, d)
        self.doctors = []
        for k in range(60):
            i, j = k % 5 + 1, (k // 5) % 6 + 1
            doc = {"doctor_id": f"DOC{k + 1:04d}", "dept_id": f"DEP{i:02d}{j:02d}",
                   "full_name": "Dr. " + self.new_person(d)["name"], "specialty": SPECIALTIES[j - 1],
                   "phone": self.r.choice("789") + "".join(self.r.choices(string.digits, k=9)),
                   "joining_date": str(date(2012, 1, 1) + timedelta(days=self.r.randint(0, 4000))),
                   "status": "active", "updated_at": self.ts(d).isoformat(sep=" "), "is_deleted": "0"}
            self.doctors.append(doc)
            self.add("doctors", dict(doc), d)

    def new_patient(self, d, person=None):
        p = person or self.new_person(d)
        pid = self.nid("PAT", 6)
        row = {"patient_id": pid, "aadhaar": p["aadhaar"], "full_name": p["name"], "gender": p["gender"],
               "dob": str(p["dob"]), "phone": p["phone"], "city": CITIES[p["city"]][0],
               "insurance_provider": self.r.choice(INSURERS) if self.r.random() < 0.45 else "",
               "last_modified": self.ts(d).isoformat(sep=" ")}
        if d >= NEW_COL_DATE:
            row["preferred_language"] = self.r.choice(["English", "Hindi", "Kannada", "Tamil", "Telugu", "Marathi"])
        self.patients[pid] = (p, row)
        p["patient_id"] = pid
        self.add("patients", dict(row), d)
        return pid

    def invoice(self, d, pid, adm_id, appt_id, gross, payer):
        iid = self.nid("INV", 7)
        disc = round(gross * self.r.choice([0, 0, 0, 0.05, 0.1]), 2)
        tax = round((gross - disc) * 0.05, 2)
        net = round(gross - disc + tax, 2)
        row = {"invoice_id": iid, "patient_id": pid, "admission_id": adm_id or "", "appt_id": appt_id or "",
               "invoice_date": str(d), "gross_amt": self.money(gross), "discount_amt": self.money(disc),
               "tax_amt": self.money(tax), "net_amt": self.money(net), "payer_type": payer, "status": "unpaid",
               "last_modified": self.ts(d).isoformat(sep=" ")}
        self.add("invoices", row, d)
        self.invoice_due[iid] = (net, pid, payer, row)
        return iid, net

    def pay(self, d, iid, amount, mode):
        self.add("payments", {"payment_id": self.nid("PAY", 7), "invoice_id": iid, "pay_date": str(d),
                              "amount": self.money(amount), "mode": mode,
                              "payment_ref": "TXN" + "".join(self.r.choices(string.digits, k=10)),
                              "last_modified": self.ts(d).isoformat(sep=" ")}, d)
        net, pid, payer, row = self.invoice_due.pop(iid)
        upd = dict(row)
        upd["status"], upd["last_modified"] = "paid", self.ts(d, 20, 23).isoformat(sep=" ")
        self.add("invoices", upd, d)

    def bhg_day(self, d):
        r = self.r
        for _ in range(self.n(8)):
            self.new_patient(d)
        pids = list(self.patients)
        # appointments -> consult invoice
        for _ in range(self.n(40)):
            appt = self.nid("APT", 7)
            pid, doc = r.choice(pids), r.choice(self.doctors)
            status = r.choices(["completed", "cancelled", "no_show"], [88, 8, 4])[0]
            fee = r.choice([500, 700, 800, 1000, 1200, 1500])
            emit = d + timedelta(days=r.randint(1, 10)) if r.random() < 0.01 else d
            doc_id = doc["doctor_id"] if r.random() > 0.005 else f"DOC{9000 + r.randint(0, 999)}"
            self.add("appointments", {"appt_id": appt, "patient_id": pid, "doctor_id": doc_id,
                                      "appt_ts": self.ts(d).isoformat(sep=" "), "status": status,
                                      "consult_fee": str(fee), "last_modified": self.ts(d).isoformat(sep=" ")}, emit)
            if status == "completed":
                iid, net = self.invoice(d, pid, None, appt, fee, "self")
                if r.random() < 0.9:
                    self.pay(d, iid, net, r.choice(["UPI", "card", "cash"]))
        # admissions open
        for _ in range(self.n(5)):
            adm = self.nid("ADM", 7)
            doc = r.choice(self.doctors)
            pid = r.choice(pids)
            hid = "HSP" + doc["dept_id"][3:5]
            stay = r.randint(1, 6)
            row = {"admission_id": adm, "patient_id": pid, "hospital_id": hid, "dept_id": doc["dept_id"],
                   "doctor_id": doc["doctor_id"], "admit_ts": self.ts(d).isoformat(sep=" "), "discharge_ts": "",
                   "ward_type": r.choice(["general", "semi-private", "private", "icu"]), "status": "admitted",
                   "last_modified": self.ts(d).isoformat(sep=" ")}
            late_parent = r.random() < 0.005
            self.add("admissions", dict(row), d + timedelta(days=2) if late_parent else d)
            self.open_adm.append([d + timedelta(days=stay), row, 0.0])
        # treatments on open admissions
        for _ in range(self.n(10)):
            if not self.open_adm:
                break
            a = r.choice(self.open_adm)
            code, name, base = r.choice(PROCEDURES)
            cost = round(base * r.uniform(0.85, 1.25), 2)
            a[2] += cost
            emit = d + timedelta(days=r.randint(1, 10)) if r.random() < 0.01 else d
            self.add("treatments", {"treatment_id": self.nid("TRT", 7), "admission_id": a[1]["admission_id"],
                                    "doctor_id": a[1]["doctor_id"], "procedure_code": code, "procedure_name": name,
                                    "treatment_date": str(d), "cost": self.money(cost),
                                    "last_modified": self.ts(d).isoformat(sep=" ")}, emit)
        # discharges -> admission invoice
        still = []
        for a in self.open_adm:
            if a[0] > d:
                still.append(a)
                continue
            row = dict(a[1])
            row["discharge_ts"], row["status"] = self.ts(d).isoformat(sep=" "), "discharged"
            row["last_modified"] = row["discharge_ts"]
            self.add("admissions", row, d)
            room = {"general": 2000, "semi-private": 3500, "private": 6000, "icu": 15000}[row["ward_type"]]
            gross = a[2] + room * max(1, (d - date.fromisoformat(a[1]["admit_ts"][:10])).days)
            pid = row["patient_id"]
            p, prow = self.patients[pid]
            payer = "insurance" if prow["insurance_provider"] and r.random() < 0.6 else "self"
            if d >= FQF_START and payer == "self" and gross > 20000 and r.random() < 0.35:
                payer = "fqf_loan"
            iid, net = self.invoice(d, pid, row["admission_id"], None, gross, payer)
            if payer == "fqf_loan":
                self.loan_for_invoice[iid] = (d, pid, net)
            elif payer == "insurance":
                approved = round(net * r.uniform(0.6, 1.0), 2)
                self.add("insurance_claims", {"Id": self.sfid("a0C"), "Invoice__c": iid, "Patient__c": pid,
                                              "Insurer__c": prow["insurance_provider"], "ClaimAmount__c": self.money(net),
                                              "ApprovedAmount__c": self.money(approved), "Status__c": "Approved",
                                              "SubmittedDate__c": str(d), "LastModifiedDate": self.ts(d).isoformat(),
                                              "IsDeleted": "false"}, d + timedelta(days=r.randint(0, 3)))
                self.pay(d, iid, net, "insurance")
            else:
                self.pay(d, iid, net, r.choice(["UPI", "card", "netbanking"]))
        self.open_adm = still
        # pharmacy
        for _ in range(self.n(30)):
            code, name, price = r.choice(DRUGS)
            emit = d + timedelta(days=r.randint(1, 10)) if r.random() < 0.01 else d
            self.add("pharmacy_sales", {"sale_id": self.nid("PHS", 7), "hospital_id": f"HSP{r.randint(1, 5):02d}",
                                        "patient_id": r.choice(pids) if r.random() < 0.8 else "",
                                        "drug_code": code, "drug_name": name, "qty": str(r.randint(1, 5)),
                                        "unit_price": self.money(price), "sale_ts": self.ts(d).isoformat(sep=" "),
                                        "last_modified": self.ts(d).isoformat(sep=" ")}, emit)
        # patient updates (SCD2)
        for _ in range(self.n(3)):
            pid = r.choice(pids)
            p, prow = self.patients[pid]
            upd = dict(prow)
            field = r.choice(["city", "phone", "insurance_provider"])
            if field == "city":
                upd["city"] = r.choice(CITIES)[0]
            elif field == "phone":
                upd["phone"] = r.choice("6789") + "".join(r.choices(string.digits, k=9))
            else:
                upd["insurance_provider"] = r.choice(INSURERS)
            upd["last_modified"] = self.ts(d).isoformat(sep=" ")
            if d >= NEW_COL_DATE and "preferred_language" not in upd:
                upd["preferred_language"] = r.choice(["English", "Hindi", "Kannada", "Tamil"])
            self.patients[pid] = (p, upd)
            self.add("patients", dict(upd), d)
        # rare doctor change (mysql soft delete / status)
        if r.random() < 0.02:
            doc = r.choice(self.doctors)
            doc["status"] = "on_leave" if doc["status"] == "active" else "active"
            doc["updated_at"] = self.ts(d).isoformat(sep=" ")
            self.add("doctors", dict(doc), d)

    # ---------- fqf ----------
    def fqf_masters(self, d):
        for i, (city, state) in enumerate(CITIES, 1):
            self.add("branches", {"branch_id": f"BR{i:02d}", "branch_name": f"Flexi Quant {city}", "city": city,
                                  "state": state, "opened_date": f"{2019 + i % 3}-04-01",
                                  "updated_at": self.ts(d).isoformat(sep=" "), "is_deleted": "0"}, d)
        self.products, self.policies = {}, {}
        for pid, name, typ, rate, mn, mx, ten in PRODUCTS:
            prow = {"product_id": pid, "product_name": name, "loan_type": typ, "rate_pct": str(rate),
                    "min_amt": str(mn), "max_amt": str(mx), "max_tenure_m": str(ten),
                    "updated_at": self.ts(d).isoformat(sep=" "), "is_deleted": "0"}
            pol = {"policy_id": f"POL{pid[3:]}01", "product_id": pid, "max_exposure_per_customer": str(mx * 2),
                   "max_dti": "45", "min_bureau_score": "650", "effective_from": str(d), "effective_to": "",
                   "last_modified": self.ts(d).isoformat(sep=" ")}
            self.products[pid], self.policies[pid] = prow, pol
            self.add("loan_products", dict(prow), d)
            self.add("credit_policy", dict(pol), d)

    def revise_masters(self, d):
        """SCD2 material: a credit policy changes about every 45 days, a product rate about every 90 days."""
        r = self.r
        age = (d - FQF_START).days
        if age and age % 45 == 0:
            pid = r.choice(list(self.policies))
            old = self.policies[pid]
            closed = dict(old)
            closed["effective_to"], closed["last_modified"] = str(d - timedelta(days=1)), self.ts(d).isoformat(sep=" ")
            self.add("credit_policy", closed, d)
            ver = int(old["policy_id"][-2:]) + 1
            new = dict(old)
            new.update({"policy_id": f"POL{pid[3:]}{ver:02d}", "effective_from": str(d), "effective_to": "",
                        "min_bureau_score": str(r.choice([640, 650, 660, 675, 700])),
                        "max_dti": str(r.choice([40, 45, 50])), "last_modified": self.ts(d).isoformat(sep=" ")})
            self.policies[pid] = new
            self.add("credit_policy", dict(new), d)
        if age and age % 90 == 0:
            pid = r.choice(list(self.products))
            prow = self.products[pid]
            prow["rate_pct"] = str(round(float(prow["rate_pct"]) + r.choice([-0.5, 0.25, 0.5]), 2))
            prow["updated_at"] = self.ts(d).isoformat(sep=" ")
            self.add("loan_products", dict(prow), d)

    def new_customer(self, d, person=None, lookalike=False):
        if person is None:
            person = self.new_person(d)
            if lookalike and self.patients:
                src, _ = self.r.choice(list(self.patients.values()))
                person["phone"], person["dob"] = src["phone"], src["dob"]
        key = id(person)
        if key in self.fqf_cust_of_person:
            return self.fqf_cust_of_person[key]
        cid = self.sfid("001")
        row = {"Id": cid, "Name": person["name"], "Aadhaar__c": person["aadhaar"], "Phone": person["phone"],
               "BirthDate__c": str(person["dob"]), "Gender__c": person["gender"],
               "City__c": CITIES[person["city"]][0], "Branch__c": f"BR{person['city'] + 1:02d}",
               "MonthlyIncome__c": str(self.r.randrange(18000, 250000, 500)),
               "Employment__c": self.r.choice(["salaried", "self_employed", "business", ""]),
               "LastModifiedDate": self.ts(d).isoformat(), "IsDeleted": "false"}
        self.customers[cid] = row
        self.fqf_cust_of_person[key] = cid
        self.add("customers", dict(row), d)
        if self.r.random() < 0.03:                       # same person entered again in another branch system
            dup = dict(row)
            dup["Id"] = self.sfid("001")
            dup["Name"] = self.r.choice([row["Name"].upper(), row["Name"].split()[0][0] + ". " + row["Name"].split()[-1]])
            dup["Branch__c"] = f"BR{self.r.randint(1, 5):02d}"
            dup["LastModifiedDate"] = self.ts(d + timedelta(days=2)).isoformat()
            self.customers[dup["Id"]] = dup
            self.add("customers", dict(dup), d + timedelta(days=self.r.randint(1, 5)))
        return cid

    def apply(self, d, cid, product, amount, tenure, inv=""):
        aid = self.sfid("a01")
        row = {"Id": aid, "Customer__c": cid, "Product__c": product[0], "Branch__c": self.customers[cid]["Branch__c"],
               "Amount__c": self.money(amount), "Tenure__c": str(tenure), "BHG_Invoice__c": inv,
               "Status__c": "Submitted", "AppliedDate__c": str(d), "LastModifiedDate": self.ts(d, 8, 11).isoformat(),
               "IsDeleted": "false"}
        self.add("loan_applications", dict(row), d)
        score = self.r.randint(560, 860)
        dti = round(self.r.uniform(10, 60), 1)
        ok = score >= 650 and dti <= 45 or (inv and self.r.random() < 0.85)
        self.add("credit_checks", {"check_id": self.nid("CHK", 7), "application_id": aid, "bureau_score": str(score),
                                   "dti_pct": str(dti), "decision": "approve" if ok else "reject",
                                   "check_ts": self.ts(d, 11, 13).isoformat(sep=" "),
                                   "last_modified": self.ts(d, 11, 13).isoformat(sep=" ")}, d)
        row["Status__c"], row["LastModifiedDate"] = ("Approved" if ok else "Rejected"), self.ts(d, 14, 16).isoformat()
        self.add("loan_applications", dict(row), d)
        if ok:
            self.open_loan(d, aid, cid, product, amount, tenure, inv)
        return ok

    def open_loan(self, d, aid, cid, product, amount, tenure, inv):
        lid = self.nid("LN", 7)
        rate = product[3]
        row = {"loan_id": lid, "application_id": aid, "customer_id": cid, "product_id": product[0],
               "branch_id": self.customers[cid]["Branch__c"], "principal": self.money(amount), "rate_pct": str(rate),
               "tenure_m": str(tenure), "start_date": str(d), "status": "active",
               "last_modified": self.ts(d, 16, 18).isoformat(sep=" ")}
        if d >= NEW_COL_DATE:
            row["channel"] = self.r.choice(["branch", "app", "partner_hospital" if inv else "web"])
        self.add("loans", dict(row), d)
        self.add("disbursements", {"disb_id": self.nid("DSB", 7), "loan_id": lid, "disb_date": str(d),
                                   "amount": self.money(amount), "payee_type": "bhg_hospital" if inv else "customer",
                                   "bhg_invoice_id": inv, "bank_ref": "UTR" + "".join(self.r.choices(string.digits, k=12)),
                                   "last_modified": self.ts(d, 16, 18).isoformat(sep=" ")}, d)
        mr = rate / 1200
        emi = amount * mr * (1 + mr) ** tenure / ((1 + mr) ** tenure - 1)
        bal = amount
        for k in range(1, tenure + 1):
            interest = bal * mr
            prin = emi - interest
            bal -= prin
            due = d + timedelta(days=30 * k)
            self.add("emi_schedule", {"loan_id": lid, "emi_no": str(k), "due_date": str(due),
                                      "principal_due": self.money(prin), "interest_due": self.money(interest),
                                      "emi_amount": self.money(emi), "updated_at": self.ts(d, 16, 18).isoformat(sep=" "),
                                      "is_deleted": "0"}, d)
            self.due.setdefault(due, []).append((lid, k, round(emi, 2)))
        self.loans[lid] = {"row": row, "left": tenure}
        if inv:
            self.pay(d, inv, self.invoice_due[inv][0], "fqf_loan")

    def fqf_day(self, d):
        r = self.r
        # hospital-bill loans for today's fqf_loan invoices (shared people)
        for inv, (idate, pid, net) in list(self.loan_for_invoice.items()):
            if idate != d:
                continue
            person, _ = self.patients[pid]
            cid = self.new_customer(d, person)
            tenure = r.choice([6, 9, 12, 18, 24])
            if not self.apply(d, cid, PRODUCTS[0], net, tenure, inv):
                self.pay(d + timedelta(days=0), inv, net, "card")
            del self.loan_for_invoice[inv]
        # new customers: hospital-bill borrowers above are shared; a few more shared, ~2% look-alikes
        for _ in range(self.n(1.8)):
            u = r.random()
            if u < 0.05 and self.patients:
                person, _ = self.patients[r.choice(list(self.patients))]
                self.new_customer(d, person)
            else:
                self.new_customer(d, lookalike=u > 0.98)
        # other applications
        cids = list(self.customers)
        for _ in range(self.n(5)):
            prod = r.choice(PRODUCTS[1:])
            amt = r.randrange(prod[4], prod[5] // 4, 1000)
            self.apply(d, r.choice(cids), prod, amt, r.choice([6, 12, 18, 24, 36][: 3 if prod[6] == 12 else 5]))
        # EMIs due today -> repayments / collections
        for lid, k, emi in self.due.pop(d, []):
            u = r.random()
            if u < 0.80:
                pay_d, fee = d, 0
            elif u < 0.95:
                pay_d, fee = d + timedelta(days=r.randint(1, 30)), 500
            else:
                pay_d = None
                self.add("collections", {"Id": self.sfid("a0K"), "Loan__c": lid, "DPD__c": "30", "Bucket__c": "SMA-1",
                                         "Action__c": r.choice(["call", "sms", "field_visit"]),
                                         "PromiseDate__c": str(d + timedelta(days=37)),
                                         "LastModifiedDate": self.ts(d + timedelta(days=30)).isoformat(),
                                         "IsDeleted": "false"}, d + timedelta(days=30))
            if pay_d:
                emit = pay_d + timedelta(days=r.randint(1, 10)) if r.random() < 0.04 else pay_d
                ref = lid if r.random() > 0.02 else f"LN{9000000 + r.randint(0, 99999)}"
                self.add("repayments", {"repay_id": self.nid("RPY", 7), "loan_id": ref, "emi_no": str(k),
                                        "paid_date": str(pay_d), "amount": self.money(emi),
                                        "mode": r.choice(["nach", "upi", "netbanking"]), "late_fee": str(fee),
                                        "last_modified": self.ts(pay_d).isoformat(sep=" ")}, emit)
                ln = self.loans[lid]
                ln["left"] -= 1
                if ln["left"] == 0:
                    upd = dict(ln["row"])
                    upd["status"], upd["last_modified"] = "closed", self.ts(pay_d, 20, 23).isoformat(sep=" ")
                    self.add("loans", upd, pay_d)
        self.revise_masters(d)
        # customer updates / rare soft delete
        for _ in range(self.n(1)):
            cid = r.choice(cids)
            upd = dict(self.customers[cid])
            upd["MonthlyIncome__c"] = str(r.randrange(18000, 250000, 500))
            upd["LastModifiedDate"] = self.ts(d).isoformat()
            if r.random() < 0.05:
                upd["IsDeleted"] = "true"
            self.customers[cid] = upd
            self.add("customers", dict(upd), d)

    # ---------- run ----------
    def build(self):
        d = BHG_START
        while d <= self.end:
            if d == BHG_START:
                self.bhg_masters(d)
                for _ in range(int(max(5000 - 8 * 545, 500) * self.s)):
                    self.new_patient(d)
            self.bhg_day(d)
            if d >= FQF_START:
                if d == FQF_START:
                    self.fqf_masters(d)
                    for _ in range(int(80 * self.s)):
                        self.new_customer(d)
                self.fqf_day(d)
            d += timedelta(days=1)
        # rows whose emit date is after `end` are not sent yet
        for t in self.rows:
            self.rows[t] = [x for x in self.rows[t] if x["_emit"] <= self.end]
        return self
