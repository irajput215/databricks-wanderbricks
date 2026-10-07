"""
Generate the messy source data for the Healthcare Claims DE practice project.

Deterministic: same seed -> byte-identical files.  Small on purpose (~4 MB total).

    python src/generate_sources.py

Layers produced
---------------
data/raw/*.csv | *.jsonl      flat-file extracts + payment event stream (as landed)
data/landing/api/*.json       paginated claims API responses (source system)
"""
from __future__ import annotations

import bisect
import csv
import json
import random
from datetime import date, datetime, timedelta

from paths import LANDING, RAW, SNAPSHOT_DATE

SEED = 20250701
rng = random.Random(SEED)

START = date(2024, 1, 1)
END = date(2025, 6, 30)

N_MEMBERS = 1_200
N_PROVIDERS = 220
N_CLAIMS = 12_000
PAGE_SIZE = 500

# --------------------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------------------
PLANS = [
    ("PL01", "Gold PPO", "PPO", 620.00),
    ("PL02", "Silver HMO", "HMO", 410.00),
    ("PL03", "Bronze HDHP", "HDHP", 260.00),
    ("PL04", "Platinum PPO", "PPO", 810.00),
    ("PL05", "EPO Select", "EPO", 350.00),
]

STATES = ["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
          "NJ", "AZ", "WA", "MA", "TN", "IN", "MO", "MD", "CO", "VA"]
STATE_W = [14, 12, 10, 9, 7, 6, 5, 5, 4, 4, 4, 3, 3, 3, 2, 2, 2, 2, 2, 2]

SPECIALTIES = [
    "Primary Care", "Cardiology", "Orthopedics", "Radiology", "Endocrinology",
    "Oncology", "Neurology", "Dermatology", "Gastroenterology", "Psychiatry",
    "Nephrology", "Pulmonology", "OB/GYN", "Emergency Medicine", "Physical Therapy",
]

# (code, description, category, standard_fee, specialty)
PROCEDURES = [
    ("99213", "Office visit, established patient, 15 min", "E&M", 150.00, "Primary Care"),
    ("99214", "Office visit, established patient, 25 min", "E&M", 220.00, "Primary Care"),
    ("99204", "Office visit, new patient", "E&M", 320.00, "Primary Care"),
    ("99395", "Preventive medicine visit, adult", "Preventive", 280.00, "Primary Care"),
    ("93000", "Electrocardiogram, routine ECG", "Diagnostic", 120.00, "Cardiology"),
    ("93306", "Echocardiography, complete", "Imaging", 900.00, "Cardiology"),
    ("93458", "Cardiac catheterization, left heart", "Surgery", 4200.00, "Cardiology"),
    ("97110", "Therapeutic exercise, 15 min", "Therapy", 90.00, "Physical Therapy"),
    ("97140", "Manual therapy techniques, 15 min", "Therapy", 85.00, "Physical Therapy"),
    ("97530", "Therapeutic activities, 15 min", "Therapy", 95.00, "Physical Therapy"),
    ("27447", "Total knee arthroplasty", "Surgery", 18500.00, "Orthopedics"),
    ("29881", "Knee arthroscopy with meniscectomy", "Surgery", 6200.00, "Orthopedics"),
    ("20610", "Arthrocentesis, major joint", "Procedure", 260.00, "Orthopedics"),
    ("73721", "MRI, lower extremity joint", "Imaging", 1450.00, "Radiology"),
    ("72148", "MRI, lumbar spine", "Imaging", 1600.00, "Radiology"),
    ("71046", "Chest X-ray, 2 views", "Imaging", 180.00, "Radiology"),
    ("74177", "CT abdomen and pelvis with contrast", "Imaging", 1900.00, "Radiology"),
    ("80053", "Comprehensive metabolic panel", "Lab", 60.00, "Primary Care"),
    ("85025", "Blood count, complete with differential", "Lab", 45.00, "Primary Care"),
    ("83036", "Hemoglobin A1c", "Lab", 55.00, "Endocrinology"),
    ("96413", "Chemotherapy infusion, 1 hour", "Oncology", 3200.00, "Oncology"),
    ("77385", "Radiation treatment delivery", "Oncology", 2100.00, "Oncology"),
    ("70551", "MRI, brain, without contrast", "Imaging", 1700.00, "Neurology"),
    ("95810", "Polysomnography, sleep study", "Diagnostic", 2400.00, "Neurology"),
    ("11102", "Tangential biopsy of skin", "Procedure", 210.00, "Dermatology"),
    ("45378", "Colonoscopy, diagnostic", "Procedure", 1500.00, "Gastroenterology"),
    ("43239", "Upper GI endoscopy with biopsy", "Procedure", 1250.00, "Gastroenterology"),
    ("90834", "Psychotherapy, 45 min", "Behavioral", 180.00, "Psychiatry"),
    ("90935", "Hemodialysis, single evaluation", "Procedure", 480.00, "Nephrology"),
    ("94010", "Spirometry, complete", "Diagnostic", 140.00, "Pulmonology"),
    ("59400", "Routine obstetric care, vaginal delivery", "OB", 3800.00, "OB/GYN"),
    ("99284", "Emergency department visit, level 4", "E&M", 950.00, "Emergency Medicine"),
]
PROC_BY_SPECIALTY: dict[str, list[tuple]] = {}
for _p in PROCEDURES:
    PROC_BY_SPECIALTY.setdefault(_p[4], []).append(_p)

DIAGNOSES = [
    ("E11.9", "Type 2 diabetes without complications", "Endocrine"),
    ("I10", "Essential hypertension", "Circulatory"),
    ("M17.11", "Unilateral osteoarthritis of right knee", "Musculoskeletal"),
    ("M54.5", "Low back pain", "Musculoskeletal"),
    ("J45.909", "Unspecified asthma, uncomplicated", "Respiratory"),
    ("K21.9", "Gastro-esophageal reflux disease", "Digestive"),
    ("F32.9", "Major depressive disorder, single episode", "Mental"),
    ("N18.3", "Chronic kidney disease, stage 3", "Genitourinary"),
    ("E78.5", "Hyperlipidemia", "Endocrine"),
    ("I25.10", "Atherosclerotic heart disease", "Circulatory"),
    ("C50.911", "Malignant neoplasm of right female breast", "Neoplasm"),
    ("C34.90", "Malignant neoplasm of lung", "Neoplasm"),
    ("Z00.00", "General adult medical examination", "Preventive"),
    ("S83.511", "Sprain of anterior cruciate ligament", "Injury"),
    ("J06.9", "Acute upper respiratory infection", "Respiratory"),
    ("R07.9", "Chest pain, unspecified", "Symptoms"),
    ("G47.33", "Obstructive sleep apnea", "Nervous"),
    ("L20.9", "Atopic dermatitis", "Skin"),
    ("K57.30", "Diverticulosis of large intestine", "Digestive"),
    ("O80", "Encounter for full-term uncomplicated delivery", "Pregnancy"),
    ("M79.671", "Pain in right foot", "Musculoskeletal"),
    ("E66.9", "Obesity, unspecified", "Endocrine"),
    ("I48.91", "Unspecified atrial fibrillation", "Circulatory"),
    ("J18.9", "Pneumonia, unspecified organism", "Respiratory"),
    ("N39.0", "Urinary tract infection", "Genitourinary"),
    ("F41.1", "Generalized anxiety disorder", "Mental"),
    ("G43.909", "Migraine, unspecified", "Nervous"),
    ("H25.11", "Age-related nuclear cataract, right eye", "Sensory"),
    ("M25.511", "Pain in right shoulder", "Musculoskeletal"),
    ("Z79.899", "Long term drug therapy", "Preventive"),
]

FIRST = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
         "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph",
         "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Nancy", "Matthew",
         "Lisa", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley", "Aisha",
         "Diego", "Priya", "Wei", "Fatima", "Omar", "Elena", "Hiro", "Ana", "Kwame"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
        "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
        "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Patel",
        "Nguyen", "Kim", "Chen", "Ali", "Okafor", "Novak", "Rossi", "Haddad", "Silva"]

# --------------------------------------------------------------------------------------
# Defect injection plan (all defects are deliberate - see SCHEMA.md)
# --------------------------------------------------------------------------------------
DEFECTS = {
    "dup_claim_versions": 60,        # same claim_id twice: old version + current version
    "mixed_case_status": 40,         # 'approved', ' DENIED ', 'Submitted'
    "dirty_state": 25,               # 'ca', ' TX ', 'California'
    "orphan_service_rows": 40,       # claim_id that does not exist in claims
    "negative_paid": 10,             # paid_amount < 0
    "null_paid_approved": 25,        # status Approved but paid_amount IS NULL (payment pending)
    "paid_gt_billed": 12,            # paid_amount > billed_amount
    "unenrolled_claims": 8,          # claim_date before member enrollment_date
    "orphan_claim_members": 15,      # claims.member_id not in members
    "late_arriving": 60,             # ingested_at >> updated_at
    "dup_payment_rows": 25,          # payment_id loaded twice
    "payment_sum_mismatch": 30,      # sum(payments) <> claims.paid_amount
    "billed_vs_service_mismatch": 15,  # claims.billed_amount <> sum(service_amount)
    "negative_service_lines": 8,     # legitimate reversal/adjustment lines
    "malformed_api_records": 6,      # records that fail schema validation
}

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def rand_date(a: date, b: date) -> date:
    return a + timedelta(days=rng.randint(0, (b - a).days))


def money(x: float) -> float:
    return round(x + 1e-9, 2)


def month_starts(a: date, b: date) -> list[date]:
    out, cur = [], date(a.year, a.month, 1)
    while cur <= b:
        out.append(cur)
        cur = date(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)
    return out


def write_csv(path, header, rows) -> None:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


# --------------------------------------------------------------------------------------
# 1. Members (eligibility system extract)
# --------------------------------------------------------------------------------------
def build_members() -> list[dict]:
    plan_w = [0.10, 0.35, 0.25, 0.05, 0.25]
    members = []
    for i in range(1, N_MEMBERS + 1):
        mid = f"MBR{i:05d}"
        gender = rng.choices(["M", "F", "U"], [0.47, 0.51, 0.02])[0]
        first = rng.choice(FIRST)
        # most members are long-tenured; ~10% enroll during the claims window
        if rng.random() < 0.10:
            enrolled = rand_date(date(2024, 1, 1), date(2025, 5, 31))
        else:
            enrolled = rand_date(date(2021, 1, 1), date(2023, 12, 31))
        members.append({
            "member_id": mid,
            "first_name": first,
            "last_name": rng.choice(LAST),
            "dob": rand_date(date(1940, 1, 1), date(2005, 12, 31)).isoformat(),
            "gender": gender,
            "state": rng.choices(STATES, STATE_W)[0],
            "enrollment_date": enrolled.isoformat(),
            "plan_id": rng.choices([p[0] for p in PLANS], plan_w)[0],
            "updated_at": f"{rand_date(date(2025, 1, 1), date(2025, 6, 30))} "
                          f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00",
        })
    return members


# --------------------------------------------------------------------------------------
# 2. Providers + SCD2 specialty history
# --------------------------------------------------------------------------------------
def build_providers() -> tuple[list[dict], list[dict]]:
    providers, history = [], []
    changed = set(rng.sample(range(1, N_PROVIDERS + 1), 30))  # 30 providers changed specialty
    for i in range(1, N_PROVIDERS + 1):
        pid = f"PRV{i:04d}"
        st = rng.choices(STATES, STATE_W)[0]
        spec = rng.choice(SPECIALTIES)
        providers.append({
            "provider_id": pid,
            "npi": f"{rng.randint(1000000000, 1999999999)}",
            "provider_name": f"{rng.choice(LAST)} {rng.choice(['Medical Group', 'Clinic', 'Health Partners', 'Associates', 'Care Center'])}",
            "specialty": spec,
            "state": st,
            "network_status": rng.choices(["In-Network", "Out-of-Network"], [0.85, 0.15])[0],
            "updated_at": f"{rand_date(date(2025, 1, 1), date(2025, 6, 30))} "
                          f"{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00",
        })
        if i in changed:
            old = rng.choice([s for s in SPECIALTIES if s != spec])
            switch = rand_date(date(2024, 3, 1), date(2025, 3, 1))
            history.append({"provider_id": pid, "specialty": old,
                            "effective_start": "2022-01-01", "effective_end": (switch - timedelta(days=1)).isoformat(),
                            "is_current": "false", "change_reason": "Credentialing update"})
            history.append({"provider_id": pid, "specialty": spec,
                            "effective_start": switch.isoformat(), "effective_end": "",
                            "is_current": "true", "change_reason": "Credentialing update"})
    return providers, history


# --------------------------------------------------------------------------------------
# 3. Claims (paginated REST API extract)
# --------------------------------------------------------------------------------------
def services_for_claim(claim_type: str, specialty: str, service_date: date) -> list[dict]:
    n = rng.choices([1, 2, 3, 4, 5, 6, 7, 8], [30, 26, 16, 10, 7, 5, 3, 3])[0]
    if claim_type == "Institutional":
        n = min(8, n + rng.randint(1, 3))
    pool = PROC_BY_SPECIALTY.get(specialty) or PROC_BY_SPECIALTY["Primary Care"]
    rows = []
    for line in range(1, n + 1):
        proc = rng.choice(pool)
        units = rng.choices([1, 2, 3], [0.85, 0.12, 0.03])[0]
        amount = money(proc[3] * units * rng.uniform(0.80, 1.60))
        rows.append({
            "claim_id": "",
            "service_id": line,
            "procedure_code": proc[0],
            "service_date": (service_date + timedelta(days=rng.randint(-3, 3))).isoformat(),
            "units": units,
            "service_amount": amount,
        })
    return rows


def build_claims(members: list[dict], providers: list[dict]):
    claims, services = [], []
    spec_by_provider = {p["provider_id"]: p["specialty"] for p in providers}
    months = month_starts(START, END)
    # steady ~5% MoM growth + mild seasonality, so window/KPI questions have signal
    season_by_month = {1: 1.06, 2: 1.04, 3: 1.03, 4: 1.03, 5: 1.01, 6: 0.99,
                       7: 0.97, 8: 0.98, 9: 1.00, 10: 1.01, 11: 0.96, 12: 0.88}
    weights = [1.05 ** i * season_by_month[m.month] for i, m in enumerate(months)]
    total_w = sum(weights)
    month_counts = [round(N_CLAIMS * w / total_w) for w in weights]
    # only bill members who were already enrolled on the claim date
    by_enrol = sorted(members, key=lambda m: m["enrollment_date"])
    enrol_keys = [m["enrollment_date"] for m in by_enrol]

    claim_no = 0
    # the last 5 providers are newly credentialed and have not billed yet (feeds the anti-join drills)
    billable = providers[:-5]
    for m, cnt in zip(months, month_counts):
        for _ in range(cnt):
            claim_no += 1
            cid = f"CLM{claim_no:07d}"
            month_end = min(date(m.year + (m.month == 12), m.month % 12 + 1, 1) - timedelta(days=1), END)
            claim_date = rand_date(m, month_end)
            eligible = bisect.bisect_right(enrol_keys, claim_date.isoformat())
            mbr = by_enrol[rng.randrange(max(1, eligible))]
            prov = rng.choice(billable)
            specialty = spec_by_provider[prov["provider_id"]]
            claim_type = rng.choices(["Professional", "Institutional"], [0.78, 0.22])[0]
            lines = services_for_claim(claim_type, specialty, claim_date)
            billed = money(sum(x["service_amount"] for x in lines))
            status = rng.choices(["Approved", "Denied", "Submitted"], [0.72, 0.18, 0.10])[0]
            if status == "Approved":
                paid = money(billed * rng.uniform(0.42, 0.78))
            elif status == "Denied":
                paid = 0.00
            else:
                paid = None
            submitted = claim_date + timedelta(days=rng.randint(0, 7))
            updated = datetime.combine(submitted + timedelta(days=rng.randint(1, 45)),
                                       datetime.min.time()) + timedelta(
                hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
            for ln in lines:
                ln["claim_id"] = cid
                ln["claim_type"] = claim_type
            services.extend(lines)
            claims.append({
                "claim_id": cid, "member_id": mbr["member_id"], "provider_id": prov["provider_id"],
                "claim_date": claim_date.isoformat(), "submitted_date": submitted.isoformat(),
                "updated_at": updated.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status, "claim_type": claim_type,
                "primary_diagnosis_code": rng.choice(DIAGNOSES)[0],
                "billed_amount": billed, "paid_amount": paid,
                "ingested_at": "", "source_system": "CLAIMS_API",
            })
    return claims, services


def inject_claim_defects(claims, services, members) -> None:
    def sample(n, pool=None):
        return rng.sample(pool if pool is not None else claims, n)

    # duplicate versions: an older snapshot of the same claim_id
    for c in sample(DEFECTS["dup_claim_versions"]):
        old = dict(c)
        old["status"] = rng.choice(["Submitted", "Approved", "Denied"])
        old["paid_amount"] = None if old["status"] == "Submitted" else money(
            (c["billed_amount"] or 0) * rng.uniform(0.4, 0.8))
        old["updated_at"] = (datetime.strptime(c["updated_at"], "%Y-%m-%d %H:%M:%S")
                             - timedelta(days=rng.randint(3, 30))).strftime("%Y-%m-%d %H:%M:%S")
        old["_dup"] = True
        old["ingested_at"] = ""
        claims.append(old)
    # malformed / case variants
    for c in sample(DEFECTS["mixed_case_status"]):
        c["status"] = rng.choice(["approved", " APPROVED ", "denied", " DENIED ", "submitted"])
    for c in sample(DEFECTS["null_paid_approved"]):
        if c["status"] == "Approved":
            c["paid_amount"] = None
    for c in sample(DEFECTS["negative_paid"]):
        c["paid_amount"] = money(-abs(c["paid_amount"] or rng.uniform(50, 900)))
    for c in sample(DEFECTS["paid_gt_billed"]):
        c["paid_amount"] = money((c["billed_amount"] or 1000) * rng.uniform(1.02, 1.25))
    member_ids = {m["member_id"] for m in members}
    for c in sample(DEFECTS["orphan_claim_members"]):
        c["member_id"] = f"MBR9{rng.randint(0, 9)}{rng.randint(1000, 9999)}"
    enrol = {m["member_id"]: m["enrollment_date"] for m in members}
    eligible = [c for c in claims if c["member_id"] in member_ids]
    # only re-date claims of recently enrolled members, so the defect stays inside the reporting window
    late_enrol = [c for c in eligible if date.fromisoformat(enrol[c["member_id"]]) >= date(2024, 5, 1)]
    for c in rng.sample(late_enrol, DEFECTS["unenrolled_claims"]):
        c["claim_date"] = (date.fromisoformat(enrol[c["member_id"]]) - timedelta(days=rng.randint(5, 90))).isoformat()
    # ingestion timestamps: normal vs late-arriving
    late = set(id(c) for c in rng.sample(claims, DEFECTS["late_arriving"]))
    for c in claims:
        base = datetime.strptime(c["updated_at"], "%Y-%m-%d %H:%M:%S")
        lag = timedelta(days=rng.randint(75, 150)) if id(c) in late else timedelta(days=rng.randint(0, 3))
        c["ingested_at"] = (base + lag).strftime("%Y-%m-%d %H:%M:%S")

    # billed_amount no longer equals sum(service lines) for a few claims (source defect)
    by_claim: dict[str, list[dict]] = {}
    for s in services:
        by_claim.setdefault(s["claim_id"], []).append(s)
    claim_by_id = {}
    for c in claims:
        claim_by_id.setdefault(c["claim_id"], c)
    for cid in rng.sample(sorted(by_claim), DEFECTS["billed_vs_service_mismatch"]):
        c = claim_by_id.get(cid)
        if c:
            c["billed_amount"] = money((c["billed_amount"] or 0) * rng.uniform(0.9, 1.12))
    # orphan service rows
    for i in range(DEFECTS["orphan_service_rows"]):
        tmpl = dict(rng.choice(services))
        tmpl["claim_id"] = f"CLM9{i:06d}"
        tmpl["service_id"] = 1
        services.append(tmpl)
    # negative adjustment lines (legitimate reversals)
    for s in rng.sample(services, DEFECTS["negative_service_lines"]):
        s["service_amount"] = money(-abs(s["service_amount"]))


def write_api_pages(claims: list[dict]) -> None:
    """Write paginated API responses; malformed records are mixed in on purpose."""
    order = sorted(claims, key=lambda c: c["claim_id"])
    bad_idx = sorted(rng.sample(range(len(order)), DEFECTS["malformed_api_records"]))
    kind_of = {idx: k for k, idx in enumerate(bad_idx)}
    total = len(order)
    pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    for p in range(pages):
        data = []
        for i in range(p * PAGE_SIZE, min((p + 1) * PAGE_SIZE, total)):
            rec = dict(order[i])
            if i in kind_of:
                kind = kind_of[i]
                if kind == 0:
                    rec.pop("claim_id")
                elif kind == 1:
                    rec["claim_date"] = "03/14/2024"          # wrong format
                elif kind == 2:
                    rec["paid_amount"] = "$1,250.50"          # currency string (coerced, not rejected)
                elif kind == 3:
                    rec["status"] = "N/A"                     # out of domain
                elif kind == 4:
                    rec["billed_amount"] = None               # required amount missing
                else:
                    rec["member_id"] = None                   # required key missing
            rec.pop("_dup", None)
            data.append(rec)
        with open(LANDING / f"claims_page_{p + 1:03d}.json", "w", encoding="utf-8") as fh:
            json.dump({"page": p + 1, "limit": PAGE_SIZE, "total": total,
                       "generated_at": f"{SNAPSHOT_DATE}T02:05:00Z", "data": data}, fh, indent=None)


# --------------------------------------------------------------------------------------
# 4. Payments (JSONL event stream)
# --------------------------------------------------------------------------------------
def build_payments(claims: list[dict]) -> list[dict]:
    payments = []
    payment_no = 0
    for c in claims:
        paid = c.get("paid_amount")
        if not paid or paid <= 0 or c["status"].strip().lower() != "approved":
            continue
        n = rng.choices([1, 2, 3], [0.72, 0.22, 0.06])[0]
        splits = sorted(rng.uniform(0.2, 1.0) for _ in range(n - 1))
        parts, prev = [], 0.0
        for s in splits + [1.0]:
            parts.append(s - prev)
            prev = s
        claim_updated = datetime.strptime(c["updated_at"], "%Y-%m-%d %H:%M:%S")
        for part in parts:
            payment_no += 1
            amt = money(paid * part)
            payments.append({
                "payment_id": f"PMT{payment_no:08d}",
                "claim_id": c["claim_id"],
                "payment_date": (claim_updated + timedelta(days=rng.randint(3, 40))).date().isoformat(),
                "payment_amount": amt,
                "payment_method": rng.choices(["ACH", "Check", "Virtual Card"], [0.8, 0.12, 0.08])[0],
                "payment_status": rng.choices(["Paid", "Voided"], [0.97, 0.03])[0],
            })
    # payment register does not always tie back to the claim (reconciliation gap)
    approved = [c for c in claims if c["status"].strip().lower() == "approved" and (c.get("paid_amount") or 0) > 0]
    for c in rng.sample(approved, DEFECTS["payment_sum_mismatch"]):
        own = [p for p in payments if p["claim_id"] == c["claim_id"]]
        if own:
            own[0]["payment_amount"] = money(own[0]["payment_amount"] * rng.uniform(0.55, 0.85))
    for _ in range(DEFECTS["dup_payment_rows"]):
        payments.append(dict(rng.choice(payments)))
    return payments


# --------------------------------------------------------------------------------------
# 5. Dirty flat-file writers
# --------------------------------------------------------------------------------------
def write_members(members, claims) -> None:
    rows = []
    dirty = set(rng.sample(range(len(members)), DEFECTS["dirty_state"]))
    variants = ["{s}", "{s}", " {s} ", "  {s}"]
    long_names = {"CA": "California", "TX": "Texas", "NY": "New York", "FL": "Florida"}
    for i, m in enumerate(members):
        st = m["state"]
        if i in dirty:
            mode = rng.choice(variants + ["long", "lower"])
            st = long_names.get(st, st) if mode == "long" else (st.lower() if mode == "lower" else mode.format(s=st))
        rows.append([m["member_id"], m["first_name"], m["last_name"], m["dob"], m["gender"],
                     st, m["enrollment_date"], m["plan_id"], m["updated_at"]])
    # duplicate member versions (source re-sends term/reinstate changes)
    for m in rng.sample(members, 6):
        rows.append([m["member_id"], m["first_name"], m["last_name"], m["dob"], m["gender"],
                     m["state"], m["enrollment_date"], m["plan_id"], "2025-02-01 08:00:00"])
    write_csv(RAW / "members.csv",
              ["member_id", "first_name", "last_name", "dob", "gender", "state",
               "enrollment_date", "plan_id", "updated_at"], rows)


def write_claims_extract_notes(claims) -> None:
    """Tiny side-car file describing the extract (no data duplication)."""
    with open(RAW / "claims_extract_manifest.json", "w", encoding="utf-8") as fh:
        json.dump({
            "source_system": "CLAIMS_API",
            "extract_type": "paginated_rest_api",
            "landing_path": "data/landing/api/claims_page_*.json",
            "page_size": PAGE_SIZE,
            "watermark_column": "updated_at",
            "snapshot_date": SNAPSHOT_DATE,
            "record_count_including_defects": len(claims),
        }, fh, indent=2)


def write_claim_services(services) -> None:
    write_csv(RAW / "claim_services.csv",
              ["claim_id", "service_id", "procedure_code", "service_date", "units",
               "service_amount", "claim_type"],
              [[s["claim_id"], s["service_id"], s["procedure_code"], s["service_date"],
                s["units"], s["service_amount"], s["claim_type"]] for s in services])


def write_payments(payments) -> None:
    with open(RAW / "payments.jsonl", "w", encoding="utf-8") as fh:
        for p in payments:
            rec = dict(p)
            # payment system emits amounts as formatted strings for a slice of records
            if rng.random() < 0.05:
                rec["payment_amount"] = f"${p['payment_amount']:,.2f}"
            elif rng.random() < 0.01:
                rec["payment_amount"] = "N/A"
            fh.write(json.dumps(rec) + "\n")


def main() -> None:
    members = build_members()
    providers, history = build_providers()
    claims, services = build_claims(members, providers)
    inject_claim_defects(claims, services, members)
    payments = build_payments(claims)

    write_members(members, claims)
    write_csv(RAW / "providers.csv",
              ["provider_id", "npi", "provider_name", "specialty", "state",
               "network_status", "updated_at"],
              [[p["provider_id"], p["npi"], p["provider_name"], p["specialty"],
                p["state"], p["network_status"], p["updated_at"]] for p in providers])
    write_csv(RAW / "provider_specialty_history.csv",
              ["provider_id", "specialty", "effective_start", "effective_end",
               "is_current", "change_reason"],
              [[h["provider_id"], h["specialty"], h["effective_start"], h["effective_end"],
                h["is_current"], h["change_reason"]] for h in history])
    write_csv(RAW / "plans.csv", ["plan_id", "plan_name", "plan_type", "monthly_premium"], PLANS)
    write_csv(RAW / "procedures.csv",
              ["procedure_code", "procedure_description", "procedure_category", "standard_fee", "specialty"],
              PROCEDURES)
    write_csv(RAW / "diagnoses.csv",
              ["diagnosis_code", "diagnosis_description", "diagnosis_category"], DIAGNOSES)
    write_claim_services(services)
    write_payments(payments)
    write_api_pages(claims)
    write_claims_extract_notes(claims)

    print(f"members          {len(members):>7,} (+6 duplicate versions)")
    print(f"providers        {len(providers):>7,} (+{len(history)} SCD2 rows)")
    print(f"claims           {len(claims):>7,} (incl. {DEFECTS['dup_claim_versions']} duplicate versions)")
    print(f"claim_services   {len(services):>7,} (incl. {DEFECTS['orphan_service_rows']} orphan rows)")
    print(f"payments         {len(payments):>7,} (incl. {DEFECTS['dup_payment_rows']} duplicate rows)")
    print(f"api pages        {(len(claims) + PAGE_SIZE - 1) // PAGE_SIZE:>7,} "
          f"({DEFECTS['malformed_api_records']} malformed records)")
    print("raw + landing written to data/")


if __name__ == "__main__":
    main()
