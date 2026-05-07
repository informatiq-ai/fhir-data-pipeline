#!/usr/bin/env python3
"""
generate_charlson_patients.py

Generates 1000 synthetic high-comorbidity patients with Charlson Comorbidity
Index conditions and writes two output files:
  - data/synthetic/fhir_bundle_charlson.json   (700 FHIR R4 transaction bundles)
  - data/synthetic/ecw_patients_charlson.csv   (300 ECW rows + 10 duplicates = 310)
  - data/synthetic/ecw_labs_charlson.csv       (1-3 labs per ECW patient)

Fixed seed=42 for deterministic output.
"""
import csv
import json
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SEED = 42
OUT_DIR = Path(__file__).parent

# 17 Charlson conditions: (name, charlson_weight, [representative_icd10_codes])
CHARLSON_CONDITIONS = [
    ("Myocardial infarction",          1, ["I21.9", "I22.0"]),
    ("Congestive heart failure",       1, ["I50.9", "I50.32"]),
    ("Peripheral vascular disease",    1, ["I70.209", "I73.9"]),
    ("Cerebrovascular disease",        1, ["I63.9", "G45.9", "I69.398"]),
    ("Dementia",                       1, ["F03.90", "G30.9", "F01.50"]),
    ("Chronic pulmonary disease",      1, ["J44.1", "J45.51", "J43.9"]),
    ("Rheumatic disease",              1, ["M05.79", "M06.09", "M32.9"]),
    ("Peptic ulcer disease",           1, ["K25.9", "K26.9"]),
    ("Mild liver disease",             1, ["K70.0", "K73.9", "K74.60"]),
    ("Diabetes without complication",  1, ["E11.9", "E10.9"]),
    ("Diabetes with complication",     2, ["E11.21", "E11.40", "E10.51"]),
    ("Hemiplegia or paraplegia",       2, ["G81.90", "G82.50", "G83.9"]),
    ("Renal disease",                  2, ["N18.3", "N18.5", "N19"]),
    ("Any malignancy",                 2, ["C34.10", "C50.911", "C61"]),
    ("Moderate/severe liver disease",  3, ["K70.40", "K72.10", "I85.00"]),
    ("Metastatic solid tumor",         6, ["C77.9", "C78.89", "C80.1"]),
    ("AIDS/HIV",                       6, ["B20", "B24"]),
]

# Tenant distribution: (name, output_channel, count)
TENANTS = [
    ("INTEGRIS_BAPTIST", "fhir", 350),
    ("OU_HEALTH",        "fhir", 350),
    ("MERCY_OKC",        "ecw",  150),
    ("ST_FRANCIS_TULSA", "ecw",  150),
]

# ECW lab tests: (local_code, display_name_matching_LOINC_map, unit, ref_low, ref_high)
ECW_LAB_POOL = [
    ("CHARLSON-HBA1C",  "hba1c",            "%",              4.8,   5.6),
    ("CHARLSON-CREAT",  "creatinine",        "mg/dL",          0.7,   1.3),
    ("CHARLSON-EGFR",   "egfr",              "mL/min/1.73m2",  60.0,  120.0),
    ("CHARLSON-LDL",    "ldl cholesterol",   "mg/dL",          0.0,   99.0),
    ("CHARLSON-GLUC",   "fasting glucose",   "mg/dL",          70.0,  99.0),
    ("CHARLSON-WBC",    "wbc",               "K/uL",           4.5,   11.0),
    ("CHARLSON-HGB",    "hemoglobin",        "g/dL",           12.0,  17.5),
    ("CHARLSON-CHOL",   "total cholesterol", "mg/dL",          0.0,   199.0),
]

NPIS = [
    "NPI-7788990011", "NPI-9988776655", "NPI-1122334455", "NPI-3344556677",
    "NPI-0987654321", "NPI-5544332211", "NPI-1234567890", "NPI-6677889900",
]

INSURANCE_PLANS = [
    "BCBSOK-PPO", "MEDICARE-A", "CIGNA-PPO", "AETNA-HMO",
    "MEDICAID-OK", "SELFPAY", "HUMANA-PPO", "UNITEDHC",
]

OK_ZIPS = [
    "73102", "73106", "73107", "73108", "73109", "73110", "73111", "73112",
    "73114", "73115", "73116", "73117", "73118", "73119", "73120", "73121",
    "73127", "73128", "73129", "73130", "73131", "73132", "73134", "73135",
    "73141", "73142", "73145", "73149", "73150", "73159", "73160", "73162",
    "73169", "73170", "73172", "73173", "74012", "74015", "74021", "74055",
    "74074", "74075", "74105", "74106", "74107", "74110", "74112", "74114",
    "74119", "74127", "74128", "74133", "74134", "74135", "74136", "74137",
    "74403", "74604", "73701", "73703", "73720", "73734",
]

OK_CITIES = [
    "Oklahoma City", "Tulsa", "Norman", "Edmond", "Broken Arrow",
    "Lawton", "Moore", "Midwest City", "Enid", "Stillwater",
]

FIRST_NAMES_M = [
    "James", "Robert", "John", "William", "David", "Richard", "Joseph", "Thomas",
    "Charles", "Christopher", "Daniel", "Matthew", "Anthony", "Mark", "Donald",
    "Steven", "Paul", "Andrew", "Kenneth", "Kevin", "Brian", "George", "Timothy",
    "Ronald", "Edward", "Jason", "Jeffrey", "Gary", "Nicholas", "Eric",
    "Jonathan", "Larry", "Justin", "Scott", "Brandon", "Benjamin", "Samuel",
    "Raymond", "Gregory", "Frank", "Alexander", "Patrick", "Jack", "Harold",
]

FIRST_NAMES_F = [
    "Mary", "Patricia", "Jennifer", "Linda", "Barbara", "Elizabeth", "Susan",
    "Jessica", "Sarah", "Karen", "Lisa", "Nancy", "Betty", "Margaret", "Sandra",
    "Ashley", "Dorothy", "Kimberly", "Emily", "Donna", "Michelle", "Carol",
    "Amanda", "Melissa", "Deborah", "Stephanie", "Rebecca", "Sharon", "Laura",
    "Cynthia", "Kathleen", "Amy", "Angela", "Shirley", "Anna", "Brenda",
    "Pamela", "Emma", "Nicole", "Helen", "Samantha", "Katherine", "Christine",
    "Debra", "Rachel", "Carolyn", "Janet", "Catherine", "Maria", "Heather",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson",
    "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen",
    "Hill", "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera",
    "Campbell", "Mitchell", "Carter", "Roberts", "Turner", "Phillips", "Evans",
    "Parker", "Edwards", "Collins", "Stewart", "Morris", "Rogers", "Reed",
    "Cook", "Morgan", "Bell", "Murphy", "Bailey", "Cooper", "Richardson",
    "Cox", "Howard", "Ward", "Brooks", "Watson", "Kelly", "Sanders", "Price",
]

STREET_NAMES = [
    "Oak", "Maple", "Cedar", "Elm", "Pine", "Birch", "Walnut", "Hickory",
    "Main", "First", "Second", "Third", "Park", "Lake", "River", "Hill",
    "Valley", "Forest", "Meadow", "Summit", "Ridge", "Canyon", "Prairie",
    "Washington", "Lincoln", "Jefferson", "Madison", "Monroe", "Adams",
]
STREET_TYPES = ["St", "Ave", "Blvd", "Dr", "Ln", "Way", "Ct", "Pl", "Rd", "Trail"]

TEXT_RESULT_VALUES = ["Positive", "Trace"]


# ---------------------------------------------------------------------------
# Demographics helpers (all use seeded rng for determinism)
# ---------------------------------------------------------------------------

def random_dob(rng: random.Random) -> date:
    start = date(1930, 1, 1)
    delta = (date(1980, 12, 31) - start).days
    return start + timedelta(days=rng.randint(0, delta))


def random_address(rng: random.Random) -> str:
    return f"{rng.randint(100, 9999)} {rng.choice(STREET_NAMES)} {rng.choice(STREET_TYPES)}"


def random_phone(rng: random.Random) -> str:
    return f"({rng.randint(200, 999):03d}){rng.randint(200, 999):03d}-{rng.randint(1000, 9999):04d}"


def random_date_str(rng: random.Random, year_start: int, year_end: int) -> str:
    y = rng.randint(year_start, year_end)
    m = rng.randint(1, 12)
    d = rng.randint(1, 28)
    return f"{y}-{m:02d}-{d:02d}"


def random_uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128)))


def highest_charlson_idx(condition_indices: list[int]) -> int:
    """Return index of highest-weight condition; on tie, prefer the higher index."""
    return max(condition_indices, key=lambda i: (CHARLSON_CONDITIONS[i][1], i))


# ---------------------------------------------------------------------------
# Patient generation
# ---------------------------------------------------------------------------

def generate_patients(rng: random.Random) -> list[dict]:
    patients = []
    patient_n = 0

    for tenant_name, channel, count in TENANTS:
        for _ in range(count):
            patient_n += 1
            gender = rng.choice(["M", "F"])
            first_name = rng.choice(FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F)
            last_name = rng.choice(LAST_NAMES)
            dob = random_dob(rng)

            # 3–7 distinct Charlson conditions
            n_cond = rng.randint(3, 7)
            condition_indices = sorted(rng.sample(range(17), n_cond))
            condition_codes = [
                rng.choice(CHARLSON_CONDITIONS[i][2]) for i in condition_indices
            ]

            patients.append({
                "patient_id":   f"CHARLSON_{patient_n:04d}",
                "first_name":   first_name,
                "last_name":    last_name,
                "dob":          dob.isoformat(),
                "dob_raw":      dob,
                "gender":       gender,
                "ssn_last4":    str(rng.randint(1000, 9999)),
                "address":      random_address(rng),
                "city":         rng.choice(OK_CITIES),
                "state":        "OK",
                "zip":          rng.choice(OK_ZIPS),
                "phone":        random_phone(rng),
                "pcp_npi":      rng.choice(NPIS),
                "insurance":    rng.choice(INSURANCE_PLANS),
                "mrn":          f"MRN-{rng.randint(10000, 99999)}",
                "last_visit_date": random_date_str(rng, 2022, 2025),
                "tenant":       tenant_name,
                "channel":      channel,
                "condition_indices": condition_indices,
                "condition_codes":   condition_codes,
                "dob_malformed": None,
            })

    return patients


def apply_dq_issues(patients: list[dict]) -> dict:
    """
    Inject intentional DQ issues and return a summary of counts.

    DQ issues:
      1. 15 blank first_name (indices 0–9 FHIR + 850–854 ECW)
      2. 25 INVALID_DX_99 condition codes (indices 10–34)
      3. 20 malformed DOB YYYYMMDD (ECW only, indices 700–719)
      DQ-4 (10 duplicate rows) and DQ-5 (30 text lab values) applied at write time.
    """
    # DQ-1: blank first_name — 10 FHIR + 5 ECW = 15
    blank_first_name_indices = list(range(0, 10)) + list(range(850, 855))
    for i in blank_first_name_indices:
        patients[i]["first_name"] = ""

    # DQ-2: inject INVALID_DX_99 as one condition code
    for i in range(10, 35):
        patients[i]["condition_codes"][0] = "INVALID_DX_99"

    # DQ-3: malformed DOB (YYYYMMDD) for first 20 MERCY_OKC patients (indices 700–719)
    for i in range(700, 720):
        dob = patients[i]["dob_raw"]
        patients[i]["dob_malformed"] = dob.strftime("%Y%m%d")

    return {
        "blank_first_name": len(blank_first_name_indices),
        "invalid_dx": 25,
        "malformed_dob": 20,
    }


def verify_condition_coverage(patients: list[dict]) -> None:
    covered = set()
    for p in patients:
        for ci in p["condition_indices"]:
            covered.add(ci)
    missing = [CHARLSON_CONDITIONS[i][0] for i in range(17) if i not in covered]
    if missing:
        raise ValueError(f"Missing coverage for Charlson conditions: {missing}")


# ---------------------------------------------------------------------------
# FHIR bundle builder
# ---------------------------------------------------------------------------

def build_fhir_bundle(patient: dict, bundle_n: int, rng: random.Random) -> dict:
    pid = f"patient-{bundle_n:05d}"
    eid = f"encounter-{bundle_n:05d}"
    tenant = patient["tenant"]

    # Admit/discharge
    admit_date = date(rng.randint(2020, 2025), rng.randint(1, 12), rng.randint(1, 28))
    admit_ts = f"{admit_date.isoformat()}T{rng.randint(6, 18):02d}:00:00+00:00"
    discharge_date = admit_date + timedelta(days=rng.randint(2, 7))
    discharge_ts = f"{discharge_date.isoformat()}T{rng.randint(8, 18):02d}:00:00+00:00"

    # Patient name
    name_obj = {"use": "official", "family": patient["last_name"]}
    if patient["first_name"]:
        name_obj["given"] = [patient["first_name"]]

    patient_resource = {
        "resourceType": "Patient",
        "id": pid,
        "identifier": [
            {
                "use": "official",
                "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "MR"}]},
                "system": f"urn:oid:{tenant.lower()}.mrn",
                "value": patient["mrn"],
            },
            {
                "use": "secondary",
                "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "SS"}]},
                "system": "http://hl7.org/fhir/sid/us-ssn",
                "value": f"XXX-XX-{patient['ssn_last4']}",
            },
        ],
        "name": [name_obj],
        "gender": "male" if patient["gender"] == "M" else "female",
        "birthDate": patient["dob"],
        "address": [
            {
                "use": "home",
                "line": [patient["address"]],
                "city": patient["city"],
                "state": patient["state"],
                "postalCode": patient["zip"],
                "country": "US",
            }
        ],
    }

    encounter_resource = {
        "resourceType": "Encounter",
        "id": eid,
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "IMP",
            "display": "inpatient encounter",
        },
        "subject": {"reference": f"urn:uuid:{pid}"},
        "period": {"start": admit_ts, "end": discharge_ts},
        "serviceProvider": {"display": tenant},
    }

    condition_entries = []
    for cond_seq, icd10_code in enumerate(patient["condition_codes"], start=1):
        cid = f"condition-{bundle_n:05d}-{cond_seq}"
        condition_entries.append({
            "fullUrl": f"urn:uuid:{cid}",
            "resource": {
                "resourceType": "Condition",
                "id": cid,
                "clinicalStatus": {
                    "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}]
                },
                "verificationStatus": {
                    "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-ver-status", "code": "confirmed"}]
                },
                "code": {
                    "coding": [{"system": "http://hl7.org/fhir/sid/icd-10-cm", "code": icd10_code}]
                },
                "subject": {"reference": f"urn:uuid:{pid}"},
            },
            "request": {"method": "POST", "url": "Condition"},
        })

    return {
        "resourceType": "Bundle",
        "id": random_uuid(rng),
        "meta": {
            "tag": [{"system": "https://oklahoma-hdu.gov/tenant", "code": tenant, "display": tenant}]
        },
        "type": "transaction",
        "timestamp": admit_ts,
        "entry": [
            {
                "fullUrl": f"urn:uuid:{pid}",
                "resource": patient_resource,
                "request": {"method": "PUT", "url": f"Patient/{pid}"},
            },
            {
                "fullUrl": f"urn:uuid:{eid}",
                "resource": encounter_resource,
                "request": {"method": "POST", "url": "Encounter"},
            },
        ] + condition_entries,
    }


# ---------------------------------------------------------------------------
# ECW CSV builders
# ---------------------------------------------------------------------------

ECW_PAT_FIELDNAMES = [
    "patient_id", "first_name", "last_name", "dob", "gender", "ssn_last4",
    "address", "city", "state", "zip", "phone", "pcp_npi", "insurance_id",
    "last_visit_date", "primary_dx_icd10",
]

ECW_LAB_FIELDNAMES = [
    "result_id", "patient_id", "collection_date", "test_code", "test_name",
    "result_value", "result_unit", "reference_range_low", "reference_range_high",
    "abnormal_flag", "ordering_provider_npi", "status",
]


def build_ecw_patient_row(patient: dict) -> dict:
    # Use malformed DOB if flagged
    dob_str = patient["dob_malformed"] if patient["dob_malformed"] else patient["dob"]

    # Primary DX: ICD-10 code of the highest-scoring Charlson condition
    best_idx = highest_charlson_idx(patient["condition_indices"])
    pos = patient["condition_indices"].index(best_idx)
    primary_dx = patient["condition_codes"][pos]

    return {
        "patient_id":       patient["patient_id"],
        "first_name":       patient["first_name"],
        "last_name":        patient["last_name"],
        "dob":              dob_str,
        "gender":           patient["gender"],
        "ssn_last4":        patient["ssn_last4"],
        "address":          patient["address"],
        "city":             patient["city"],
        "state":            patient["state"],
        "zip":              patient["zip"],
        "phone":            patient["phone"],
        "pcp_npi":          patient["pcp_npi"],
        "insurance_id":     patient["insurance"],
        "last_visit_date":  patient["last_visit_date"],
        "primary_dx_icd10": primary_dx,
    }


def build_ecw_lab_rows(
    ecw_patients: list[dict],
    rng: random.Random,
    text_result_budget: int = 30,
) -> list[dict]:
    rows = []
    text_count = 0

    for patient in ecw_patients:
        n_labs = rng.randint(1, 3)
        lab_sample = rng.sample(ECW_LAB_POOL, min(n_labs, len(ECW_LAB_POOL)))
        coll_date = random_date_str(rng, 2023, 2025)
        npi = rng.choice(NPIS)

        for lab_code, lab_display, unit, ref_low, ref_high in lab_sample:
            lab_n = len(rows) + 1
            use_text = text_count < text_result_budget

            if use_text:
                result_value = rng.choice(TEXT_RESULT_VALUES)
                ref_lo_str = ""
                ref_hi_str = ""
                abnormal_flag = ""
                text_count += 1
            else:
                # Slightly abnormal values for renal/DM patients
                result_value = round(rng.uniform(ref_low * 0.8, ref_high * 1.4), 1)
                ref_lo_str = str(ref_low)
                ref_hi_str = str(ref_high)
                abnormal_flag = "H" if result_value > ref_high else ("L" if result_value < ref_low else "N")

            rows.append({
                "result_id":             f"CHARLSON-LAB-{lab_n:06d}",
                "patient_id":            patient["patient_id"],
                "collection_date":       coll_date,
                "test_code":             lab_code,
                "test_name":             lab_display,
                "result_value":          result_value,
                "result_unit":           unit if not use_text else "N/A",
                "reference_range_low":   ref_lo_str,
                "reference_range_high":  ref_hi_str,
                "abnormal_flag":         abnormal_flag,
                "ordering_provider_npi": npi,
                "status":                "final",
            })

    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    rng = random.Random(SEED)

    # Generate base patients
    patients = generate_patients(rng)
    assert len(patients) == 1000

    # Apply DQ mutations
    dq_summary = apply_dq_issues(patients)

    # Verify all 17 Charlson conditions are represented
    verify_condition_coverage(patients)

    # Split into FHIR (first 700) and ECW (last 300)
    fhir_patients = [p for p in patients if p["channel"] == "fhir"]   # 700
    ecw_patients  = [p for p in patients if p["channel"] == "ecw"]    # 300
    assert len(fhir_patients) == 700
    assert len(ecw_patients) == 300

    # --- FHIR bundles ---
    bundles = []
    for bundle_n, patient in enumerate(fhir_patients, start=1):
        bundles.append(build_fhir_bundle(patient, bundle_n, rng))

    fhir_path = OUT_DIR / "fhir_bundle_charlson.json"
    with open(fhir_path, "w", encoding="utf-8") as f:
        json.dump(bundles, f, indent=2)

    # --- ECW patients CSV ---
    ecw_rows = [build_ecw_patient_row(p) for p in ecw_patients]
    # DQ-4: append 10 duplicate rows (exact copies of first 10 ECW patients)
    duplicate_rows = ecw_rows[:10]
    ecw_rows_with_dupes = ecw_rows + duplicate_rows
    dq_summary["duplicate_rows"] = 10

    ecw_pat_path = OUT_DIR / "ecw_patients_charlson.csv"
    with open(ecw_pat_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ECW_PAT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(ecw_rows_with_dupes)

    # --- ECW labs CSV ---
    lab_rows = build_ecw_lab_rows(ecw_patients, rng, text_result_budget=30)
    dq_summary["text_lab_rows"] = 30

    ecw_lab_path = OUT_DIR / "ecw_labs_charlson.csv"
    with open(ecw_lab_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ECW_LAB_FIELDNAMES)
        writer.writeheader()
        writer.writerows(lab_rows)

    # Verify Charlson condition coverage in output
    conditions_seen = set()
    for p in patients:
        for ci in p["condition_indices"]:
            conditions_seen.add(ci)
    n_covered = len(conditions_seen)

    total_dq = (
        dq_summary["blank_first_name"]
        + dq_summary["malformed_dob"]
        + dq_summary["duplicate_rows"]
        + dq_summary["invalid_dx"]
        + dq_summary["text_lab_rows"]
    )

    print("Charlson patient generation complete")
    print(f"FHIR bundles written    : {len(bundles)}  →  {fhir_path}")
    print(f"ECW patients written    : {len(ecw_rows_with_dupes)}  →  {ecw_pat_path}")
    print(f"ECW labs written        : {len(lab_rows)}  →  {ecw_lab_path}")
    print(f"Charlson conditions covered: {n_covered}/17")
    print(f"Intentional DQ issues   : {total_dq}")


if __name__ == "__main__":
    main()
