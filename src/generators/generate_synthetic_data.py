"""
Synthetic Healthcare RCM source-data generator.

Simulates the messy reality of a hospital revenue cycle landscape:

  * Two EMR instances (post-merger) that hold the same entities under
    completely different column names, date formats and code values.
  * A central billing / claims system that submits professional claims.
  * Payer remittance advice (an 835 ERA flattened to CSV) carrying
    payments, contractual adjustments and CARC denial codes.
  * Reference data: CPT, ICD-10, payers, CARC codes, NPI registry.

Deliberate data-quality defects are injected (nulls, duplicates, bad
dates, orphan foreign keys, negative charges) so the Silver layer's
quality framework has something real to catch.

Usage:
    python -m src.generators.generate_synthetic_data --out data/landing --scale 1.0
"""

from __future__ import annotations

import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from faker import Faker

SEED = 42
fake = Faker("en_US")
Faker.seed(SEED)
random.seed(SEED)

# --------------------------------------------------------------------------
# Reference domains
# --------------------------------------------------------------------------

CPT_CODES = [
    ("99213", "Office/outpatient visit, established patient, 20-29 min", "E&M", 125.00),
    ("99214", "Office/outpatient visit, established patient, 30-39 min", "E&M", 185.00),
    ("99215", "Office/outpatient visit, established patient, 40-54 min", "E&M", 255.00),
    ("99283", "Emergency dept visit, moderate severity", "Emergency", 480.00),
    ("99284", "Emergency dept visit, high severity", "Emergency", 760.00),
    ("99285", "Emergency dept visit, high severity with threat to life", "Emergency", 1180.00),
    ("71046", "Radiologic exam, chest, 2 views", "Radiology", 235.00),
    ("74177", "CT abdomen and pelvis with contrast", "Radiology", 1450.00),
    ("70551", "MRI brain without contrast", "Radiology", 1980.00),
    ("80053", "Comprehensive metabolic panel", "Laboratory", 68.00),
    ("85025", "Complete blood count with differential", "Laboratory", 42.00),
    ("36415", "Collection of venous blood by venipuncture", "Laboratory", 22.00),
    ("93000", "Electrocardiogram, complete", "Cardiology", 95.00),
    ("93306", "Echocardiography, transthoracic, complete", "Cardiology", 890.00),
    ("45378", "Diagnostic colonoscopy", "Surgery", 1240.00),
    ("47562", "Laparoscopic cholecystectomy", "Surgery", 6850.00),
    ("27447", "Total knee arthroplasty", "Surgery", 18400.00),
    ("29881", "Arthroscopy, knee, with meniscectomy", "Surgery", 4320.00),
    ("59400", "Routine obstetric care including delivery", "Obstetrics", 5600.00),
    ("97110", "Therapeutic exercise, each 15 minutes", "Therapy", 78.00),
]

ICD10_CODES = [
    ("I10", "Essential (primary) hypertension", "Circulatory"),
    ("E11.9", "Type 2 diabetes mellitus without complications", "Endocrine"),
    ("J44.9", "Chronic obstructive pulmonary disease, unspecified", "Respiratory"),
    ("M17.11", "Unilateral primary osteoarthritis, right knee", "Musculoskeletal"),
    ("K80.20", "Calculus of gallbladder without cholecystitis", "Digestive"),
    ("R07.9", "Chest pain, unspecified", "Symptoms"),
    ("N39.0", "Urinary tract infection, site not specified", "Genitourinary"),
    ("J18.9", "Pneumonia, unspecified organism", "Respiratory"),
    ("I50.9", "Heart failure, unspecified", "Circulatory"),
    ("F41.9", "Anxiety disorder, unspecified", "Mental"),
    ("Z00.00", "General adult medical examination without abnormal findings", "Factors"),
    ("S72.001A", "Fracture of unspecified part of neck of right femur", "Injury"),
]

PAYERS = [
    ("PAY001", "Medicare Part B", "Government", 0.82, 34),
    ("PAY002", "Medicaid State Plan", "Government", 0.61, 52),
    ("PAY003", "Blue Cross Blue Shield", "Commercial", 0.88, 28),
    ("PAY004", "UnitedHealthcare", "Commercial", 0.85, 31),
    ("PAY005", "Aetna", "Commercial", 0.86, 29),
    ("PAY006", "Cigna", "Commercial", 0.84, 33),
    ("PAY007", "Humana Medicare Advantage", "Medicare Advantage", 0.79, 38),
    ("PAY008", "Tricare", "Government", 0.80, 41),
    ("PAY009", "Workers Compensation", "Workers Comp", 0.90, 62),
    ("PAY010", "Self Pay", "Self Pay", 0.21, 95),
]

# CARC = Claim Adjustment Reason Code (X12 835). Weighted toward the
# denial reasons that actually dominate hospital A/R work queues.
CARC_CODES = [
    ("CO-16", "Claim/service lacks information or has submission/billing error", "Technical", True, 0.20),
    ("CO-97", "Benefit for this service is included in another service already adjudicated", "Bundling", False, 0.10),
    ("CO-50", "Non-covered service, not deemed a medical necessity", "Medical Necessity", True, 0.12),
    ("CO-29", "The time limit for filing has expired", "Timely Filing", False, 0.06),
    ("CO-18", "Exact duplicate claim or service", "Duplicate", True, 0.09),
    ("CO-197", "Precertification/authorization absent", "Authorization", True, 0.15),
    ("CO-109", "Claim not covered by this payer/contractor", "Coordination of Benefits", True, 0.08),
    ("PR-1", "Deductible amount", "Patient Responsibility", False, 0.09),
    ("PR-2", "Coinsurance amount", "Patient Responsibility", False, 0.06),
    ("CO-45", "Charge exceeds fee schedule/maximum allowable", "Contractual", False, 0.05),
]

DEPARTMENTS = [
    ("DEP01", "Emergency Department", "Emergency"),
    ("DEP02", "Internal Medicine", "Ambulatory"),
    ("DEP03", "Orthopedic Surgery", "Surgical"),
    ("DEP04", "Cardiology", "Ambulatory"),
    ("DEP05", "Radiology", "Ancillary"),
    ("DEP06", "Laboratory", "Ancillary"),
    ("DEP07", "Obstetrics", "Inpatient"),
    ("DEP08", "Physical Therapy", "Ambulatory"),
]

SPECIALTIES = [
    "Internal Medicine", "Emergency Medicine", "Orthopedic Surgery",
    "Cardiology", "Radiology", "General Surgery", "Obstetrics & Gynecology",
    "Family Medicine", "Anesthesiology", "Physical Medicine",
]

ENCOUNTER_TYPES = ["Inpatient", "Outpatient", "Emergency", "Observation", "Telehealth"]
CLAIM_STATUSES = ["PAID", "PARTIALLY_PAID", "DENIED", "IN_PROCESS", "SUBMITTED", "APPEALED"]

RUN_DATE = date(2025, 12, 31)
HISTORY_START = date(2024, 1, 1)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    print(f"  wrote {len(rows):>7,} rows -> {path}")


def rand_date(start: date, end: date) -> date:
    return start + timedelta(days=random.randint(0, (end - start).days))


def weighted_choice(items, weight_index):
    weights = [i[weight_index] for i in items]
    return random.choices(items, weights=weights, k=1)[0]


def maybe_null(value, probability: float):
    """Inject a null with the given probability."""
    return "" if random.random() < probability else value


# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

def gen_reference(out: Path) -> None:
    print("\n[reference]")
    write_csv(out / "reference" / "cpt_codes.csv",
              ["cpt_code", "description", "service_category", "standard_charge_amount"],
              [list(c) for c in CPT_CODES])

    write_csv(out / "reference" / "icd10_codes.csv",
              ["icd10_code", "description", "chapter"],
              [list(c) for c in ICD10_CODES])

    write_csv(out / "reference" / "payers.csv",
              ["payer_id", "payer_name", "payer_type", "contracted_rate", "avg_days_to_pay"],
              [list(p) for p in PAYERS])

    write_csv(out / "reference" / "carc_codes.csv",
              ["carc_code", "description", "denial_category", "is_appealable"],
              [[c[0], c[1], c[2], str(c[3]).lower()] for c in CARC_CODES])


def gen_providers(out: Path, n: int) -> list[dict]:
    """NPI registry style provider file, shared across both EMRs."""
    print("\n[providers]")
    providers = []
    for i in range(n):
        npi = f"1{random.randint(100000000, 999999999)}"
        providers.append({
            "npi": npi,
            "provider_id": f"PRV{i + 1:04d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "credential": random.choice(["MD", "MD", "MD", "DO", "NP", "PA-C"]),
            "specialty": random.choice(SPECIALTIES),
            "department_id": random.choice(DEPARTMENTS)[0],
            "hospital": random.choice(["HOSP_A", "HOSP_B"]),
            "active_flag": "Y" if random.random() > 0.08 else "N",
        })
    write_csv(out / "reference" / "npi_providers.csv", list(providers[0].keys()),
              [list(p.values()) for p in providers])

    write_csv(out / "reference" / "departments.csv",
              ["department_id", "department_name", "service_line"],
              [list(d) for d in DEPARTMENTS])
    return providers


# --------------------------------------------------------------------------
# EMR extracts - two hospitals, two very different schemas
# --------------------------------------------------------------------------

def gen_patients(out: Path, n: int) -> list[dict]:
    """
    Hospital A: CamelCase headers, ISO dates, M/F gender.
    Hospital B: snake_case headers, US dates, MALE/FEMALE gender.

    A slice of patients appear in BOTH systems (same SSN-ish key) to
    exercise cross-system de-duplication, and a slice receive an updated
    record in a later extract to exercise SCD Type 2.
    """
    print("\n[patients]")
    patients = []
    for i in range(n):
        hospital = "HOSP_A" if i % 2 == 0 else "HOSP_B"
        gender = random.choice(["M", "F"])
        dob = fake.date_of_birth(minimum_age=1, maximum_age=95)
        patients.append({
            "patient_key": f"P{i + 1:06d}",
            "mrn": f"{'A' if hospital == 'HOSP_A' else 'B'}-{100000 + i}",
            "first_name": fake.first_name_male() if gender == "M" else fake.first_name_female(),
            "last_name": fake.last_name(),
            "dob": dob,
            "gender": gender,
            "address": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            "phone": fake.numerify("###-###-####"),
            "email": fake.email(),
            "hospital": hospital,
            "updated_at": datetime.combine(rand_date(HISTORY_START, RUN_DATE), datetime.min.time()),
        })

    # --- Hospital A extract -------------------------------------------------
    rows_a = []
    for p in (x for x in patients if x["hospital"] == "HOSP_A"):
        rows_a.append([
            p["patient_key"], p["mrn"], p["first_name"], p["last_name"],
            p["dob"].isoformat(), p["gender"],
            maybe_null(p["address"], 0.03), p["city"], p["state"],
            maybe_null(p["zip"], 0.02), maybe_null(p["phone"], 0.06), p["email"],
            p["updated_at"].strftime("%Y-%m-%d %H:%M:%S"),
        ])
    # duplicate records - the same MRN exported twice by the source
    rows_a += [list(r) for r in random.sample(rows_a, k=max(1, len(rows_a) // 100))]
    write_csv(out / "emr" / "hospital_a" / "patients.csv",
              ["PatientID", "MRN", "FirstName", "LastName", "DateOfBirth", "Gender",
               "AddressLine1", "City", "State", "ZipCode", "PhoneNumber", "EmailAddress",
               "LastUpdatedTimestamp"],
              rows_a)

    # --- Hospital B extract -------------------------------------------------
    rows_b = []
    for p in (x for x in patients if x["hospital"] == "HOSP_B"):
        rows_b.append([
            p["patient_key"], p["mrn"], f"{p['last_name']}, {p['first_name']}",
            p["dob"].strftime("%m/%d/%Y"),
            "MALE" if p["gender"] == "M" else "FEMALE",
            maybe_null(p["address"], 0.04), p["city"], p["state"],
            maybe_null(p["zip"], 0.02), maybe_null(p["phone"], 0.08), p["email"],
            p["updated_at"].strftime("%m/%d/%Y %H:%M"),
        ])
    write_csv(out / "emr" / "hospital_b" / "patients.csv",
              ["id", "medical_record_no", "full_name", "birth_date", "sex",
               "street", "city_name", "state_code", "postal_code", "contact_number",
               "email_id", "modified_ts"],
              rows_b)

    # --- Day-2 delta: address / phone changes drive SCD2 --------------------
    hosp_a = [p for p in patients if p["hospital"] == "HOSP_A"]
    changed = random.sample(hosp_a, k=int(len(hosp_a) * 0.12))
    delta_rows = []
    for p in changed:
        new_addr = fake.street_address()
        new_city = fake.city()
        p_updated = datetime.combine(RUN_DATE, datetime.min.time()) + timedelta(hours=6)
        delta_rows.append([
            p["patient_key"], p["mrn"], p["first_name"], p["last_name"],
            p["dob"].isoformat(), p["gender"], new_addr, new_city, p["state"],
            fake.zipcode(), fake.numerify("###-###-####"), p["email"],
            p_updated.strftime("%Y-%m-%d %H:%M:%S"),
        ])
    write_csv(out / "emr" / "hospital_a" / "patients_delta.csv",
              ["PatientID", "MRN", "FirstName", "LastName", "DateOfBirth", "Gender",
               "AddressLine1", "City", "State", "ZipCode", "PhoneNumber", "EmailAddress",
               "LastUpdatedTimestamp"],
              delta_rows)
    return patients


def gen_encounters(out: Path, patients: list[dict], providers: list[dict], n: int) -> list[dict]:
    print("\n[encounters]")
    encounters = []
    for i in range(n):
        p = random.choice(patients)
        prov = random.choice([x for x in providers if x["hospital"] == p["hospital"]] or providers)
        etype = random.choices(ENCOUNTER_TYPES, weights=[0.12, 0.45, 0.22, 0.09, 0.12])[0]
        start = rand_date(HISTORY_START, RUN_DATE - timedelta(days=5))
        los = 0 if etype in ("Outpatient", "Telehealth") else random.randint(0, 9)
        encounters.append({
            "encounter_id": f"ENC{i + 1:07d}",
            "patient_key": p["patient_key"],
            "provider_id": prov["provider_id"],
            "department_id": prov["department_id"],
            "encounter_type": etype,
            "admit_date": start,
            "discharge_date": start + timedelta(days=los),
            "primary_diagnosis": random.choice(ICD10_CODES)[0],
            "hospital": p["hospital"],
            "insurance_payer_id": weighted_choice(PAYERS, 3)[0],
        })

    for hosp, fname, header in [
        ("HOSP_A", "encounters.csv",
         ["EncounterID", "PatientID", "ProviderID", "DepartmentID", "EncounterType",
          "AdmitDate", "DischargeDate", "PrimaryDiagnosisCode", "PayerID"]),
        ("HOSP_B", "encounters.csv",
         ["visit_id", "id", "rendering_provider", "dept_id", "visit_type",
          "admission_dt", "discharge_dt", "primary_dx", "insurance_id"]),
    ]:
        rows = []
        for e in (x for x in encounters if x["hospital"] == hosp):
            if hosp == "HOSP_A":
                rows.append([e["encounter_id"], e["patient_key"], e["provider_id"],
                             e["department_id"], e["encounter_type"],
                             e["admit_date"].isoformat(), e["discharge_date"].isoformat(),
                             e["primary_diagnosis"], e["insurance_payer_id"]])
            else:
                # Hospital B also ships a handful of discharge dates BEFORE
                # the admit date - a classic source-system defect.
                disch = e["discharge_date"]
                if random.random() < 0.004:
                    disch = e["admit_date"] - timedelta(days=2)
                rows.append([e["encounter_id"], e["patient_key"], e["provider_id"],
                             e["department_id"], e["encounter_type"].upper(),
                             e["admit_date"].strftime("%m/%d/%Y"), disch.strftime("%m/%d/%Y"),
                             e["primary_diagnosis"], e["insurance_payer_id"]])
        folder = "hospital_a" if hosp == "HOSP_A" else "hospital_b"
        write_csv(out / "emr" / folder / fname, header, rows)
    return encounters


# --------------------------------------------------------------------------
# Billing system: claims, claim lines, transactions, remittance
# --------------------------------------------------------------------------

def gen_claims_and_transactions(out: Path, encounters: list[dict], providers: list[dict]):
    print("\n[claims / transactions / remittance]")
    prov_by_id = {p["provider_id"]: p for p in providers}
    payer_by_id = {p[0]: p for p in PAYERS}

    claims, claim_lines, transactions, remits = [], [], [], []
    claim_seq = txn_seq = line_seq = 0

    for enc in encounters:
        # not every encounter is billed within the window
        if random.random() < 0.08:
            continue

        claim_seq += 1
        claim_id = f"CLM{claim_seq:07d}"
        payer = payer_by_id[enc["insurance_payer_id"]]
        prov = prov_by_id.get(enc["provider_id"])

        # charge lag: days between service and claim submission
        charge_lag = max(0, int(random.gauss(6, 4)))
        service_date = min(enc["discharge_date"], RUN_DATE)
        submit_date = min(service_date + timedelta(days=charge_lag), RUN_DATE)

        # ---- claim lines --------------------------------------------------
        n_lines = random.choices([1, 2, 3, 4, 5], weights=[0.42, 0.26, 0.16, 0.10, 0.06])[0]
        total_charge = 0.0
        for _ in range(n_lines):
            cpt = random.choice(CPT_CODES)
            units = random.choices([1, 1, 1, 2, 3], weights=[0.7, 0.1, 0.1, 0.07, 0.03])[0]
            charge = round(cpt[3] * units * random.uniform(0.95, 1.25), 2)
            total_charge += charge
            line_seq += 1
            claim_lines.append([
                f"CLN{line_seq:08d}", claim_id, cpt[0],
                random.choice(ICD10_CODES)[0], units, f"{charge:.2f}",
                service_date.isoformat(),
                random.choice(["", "", "", "25", "59", "RT", "LT"]),  # CPT modifier
            ])
        total_charge = round(total_charge, 2)

        # ---- adjudication outcome ----------------------------------------
        base_denial_risk = 0.14
        if payer[2] == "Self Pay":
            base_denial_risk = 0.02
        if prov and prov["active_flag"] == "N":
            base_denial_risk += 0.10
        if charge_lag > 14:
            base_denial_risk += 0.06

        is_denied = random.random() < base_denial_risk
        days_to_pay = max(5, int(random.gauss(payer[4], payer[4] * 0.35)))
        remit_date = submit_date + timedelta(days=days_to_pay)
        settled = remit_date <= RUN_DATE

        contractual_adj = round(total_charge * (1 - payer[3]), 2)
        allowed_amount = round(total_charge - contractual_adj, 2)

        if not settled:
            status = "IN_PROCESS" if random.random() < 0.7 else "SUBMITTED"
            paid, patient_resp, carc = 0.0, 0.0, ""
        elif is_denied:
            carc_row = random.choices(CARC_CODES, weights=[c[4] for c in CARC_CODES])[0]
            carc = carc_row[0]
            # some denials are worked and later paid
            if carc_row[3] and random.random() < 0.38:
                status = "PAID"
                paid = round(allowed_amount * random.uniform(0.85, 1.0), 2)
                patient_resp = round(allowed_amount - paid, 2)
                remit_date = remit_date + timedelta(days=random.randint(20, 60))
                if remit_date > RUN_DATE:
                    remit_date, status, paid, patient_resp = (
                        submit_date + timedelta(days=days_to_pay), "APPEALED", 0.0, 0.0)
            else:
                status = "DENIED"
                paid, patient_resp = 0.0, 0.0
        else:
            coins_rate = 0.0 if payer[2] == "Government" else random.choice([0.0, 0.0, 0.1, 0.2])
            patient_resp = round(allowed_amount * coins_rate, 2)
            paid = round(allowed_amount - patient_resp, 2)
            status = "PAID" if patient_resp == 0 else "PARTIALLY_PAID"
            carc = "PR-2" if patient_resp > 0 else ""

        claims.append([
            claim_id, enc["encounter_id"], enc["patient_key"], enc["insurance_payer_id"],
            enc["provider_id"], service_date.isoformat(), submit_date.isoformat(),
            f"{total_charge:.2f}", f"{allowed_amount:.2f}", status,
            "PROFESSIONAL" if enc["encounter_type"] != "Inpatient" else "INSTITUTIONAL",
            enc["hospital"],
        ])

        # ---- transaction ledger: charge, payment, adjustment --------------
        txn_seq += 1
        transactions.append([f"TXN{txn_seq:08d}", claim_id, enc["encounter_id"],
                             enc["patient_key"], "CHARGE", f"{total_charge:.2f}",
                             submit_date.isoformat(), enc["insurance_payer_id"], ""])

        if settled:
            txn_seq += 1
            transactions.append([f"TXN{txn_seq:08d}", claim_id, enc["encounter_id"],
                                 enc["patient_key"], "CONTRACTUAL_ADJUSTMENT",
                                 f"{-contractual_adj:.2f}", remit_date.isoformat(),
                                 enc["insurance_payer_id"], "CO-45"])
            if paid > 0:
                txn_seq += 1
                transactions.append([f"TXN{txn_seq:08d}", claim_id, enc["encounter_id"],
                                     enc["patient_key"], "INSURANCE_PAYMENT",
                                     f"{-paid:.2f}", remit_date.isoformat(),
                                     enc["insurance_payer_id"], ""])
            if patient_resp > 0 and random.random() < 0.55:
                pay_date = remit_date + timedelta(days=random.randint(10, 75))
                if pay_date <= RUN_DATE:
                    txn_seq += 1
                    transactions.append([f"TXN{txn_seq:08d}", claim_id, enc["encounter_id"],
                                         enc["patient_key"], "PATIENT_PAYMENT",
                                         f"{-patient_resp:.2f}", pay_date.isoformat(),
                                         enc["insurance_payer_id"], ""])

            # --- write-offs: aged A/R has to leave the books somehow -----
            # Bad debt on denials that were worked and lost, plus small
            # balance write-offs. Without these, denied claims age forever
            # and the 180+ bucket grows without bound.
            if status == "DENIED":
                # Write off the REMAINING balance, not the gross charge. The
                # contractual adjustment has already come off the account;
                # writing off the full charge again drives the claim into a
                # credit balance and breaks the A/R roll-forward.
                remaining = round(total_charge - contractual_adj, 2)
                wo_date = remit_date + timedelta(days=random.randint(75, 150))
                if wo_date <= RUN_DATE and random.random() < 0.62 and remaining > 0:
                    txn_seq += 1
                    transactions.append([f"TXN{txn_seq:08d}", claim_id, enc["encounter_id"],
                                         enc["patient_key"], "WRITEOFF",
                                         f"{-remaining:.2f}", wo_date.isoformat(),
                                         enc["insurance_payer_id"], carc])
            elif patient_resp > 0:
                # Unpaid patient balance: small ones written off, larger ones
                # sent to bad debt after ageing.
                unpaid = not any(t[1] == claim_id and t[4] == "PATIENT_PAYMENT"
                                 for t in transactions[-4:])
                if unpaid:
                    threshold = 15.00
                    if patient_resp <= threshold:
                        wo_date = remit_date + timedelta(days=45)
                    else:
                        wo_date = remit_date + timedelta(days=random.randint(120, 210))
                    if wo_date <= RUN_DATE and random.random() < 0.55:
                        txn_seq += 1
                        transactions.append([f"TXN{txn_seq:08d}", claim_id,
                                             enc["encounter_id"], enc["patient_key"],
                                             "WRITEOFF", f"{-patient_resp:.2f}",
                                             wo_date.isoformat(),
                                             enc["insurance_payer_id"], "BAD_DEBT"])

            remits.append([
                f"ERA{claim_seq:07d}", claim_id, enc["insurance_payer_id"],
                remit_date.isoformat(), f"{total_charge:.2f}", f"{allowed_amount:.2f}",
                f"{paid:.2f}", f"{patient_resp:.2f}", carc,
                "1" if status == "DENIED" else "0",
            ])

    # ---- inject defects ---------------------------------------------------
    for _ in range(int(len(claims) * 0.004)):                     # orphan FK
        c = list(random.choice(claims))
        c[0] = f"CLM9{random.randint(100000, 999999)}"
        c[3] = "PAY999"
        claims.append(c)
    for _ in range(int(len(transactions) * 0.002)):               # negative charge
        t = list(random.choice(transactions))
        t[0] = f"TXN9{random.randint(1000000, 9999999)}"
        t[4], t[5] = "CHARGE", "-501.00"
        transactions.append(t)
    claims += [list(r) for r in random.sample(claims, k=int(len(claims) * 0.003))]  # dupes

    random.shuffle(transactions)

    write_csv(out / "billing" / "claims.csv",
              ["claim_id", "encounter_id", "patient_id", "payer_id", "provider_id",
               "service_date", "submission_date", "total_charge_amount",
               "allowed_amount", "claim_status", "claim_type", "source_hospital"],
              claims)

    write_csv(out / "billing" / "claim_lines.csv",
              ["claim_line_id", "claim_id", "cpt_code", "icd10_code", "units",
               "line_charge_amount", "service_date", "modifier"],
              claim_lines)

    write_csv(out / "billing" / "transactions.csv",
              ["transaction_id", "claim_id", "encounter_id", "patient_id",
               "transaction_type", "transaction_amount", "post_date", "payer_id",
               "adjustment_reason_code"],
              transactions)

    write_csv(out / "remittance" / "era_835.csv",
              ["remittance_id", "claim_id", "payer_id", "remit_date",
               "billed_amount", "allowed_amount", "paid_amount",
               "patient_responsibility", "carc_code", "denied_flag"],
              remits)


# --------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic healthcare RCM landing data")
    ap.add_argument("--out", default="data/landing", help="output directory")
    ap.add_argument("--scale", type=float, default=1.0,
                    help="row multiplier (1.0 ~= 5k patients / 20k encounters)")
    args = ap.parse_args()

    out = Path(args.out)
    n_patients = int(5_000 * args.scale)
    n_providers = max(20, int(180 * args.scale))
    n_encounters = int(20_000 * args.scale)

    print(f"Generating synthetic RCM data (scale={args.scale}) into {out.resolve()}")
    gen_reference(out)
    providers = gen_providers(out, n_providers)
    patients = gen_patients(out, n_patients)
    encounters = gen_encounters(out, patients, providers, n_encounters)
    gen_claims_and_transactions(out, encounters, providers)

    total = sum(f.stat().st_size for f in out.rglob("*.csv"))
    print(f"\nDone. {len(list(out.rglob('*.csv')))} files, {total / 1_048_576:.1f} MB")


if __name__ == "__main__":
    main()
