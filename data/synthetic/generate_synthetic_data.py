#!/usr/bin/env python3
"""
data/synthetic/generate_synthetic_data.py

Synthetic data generator for fhir-data-pipeline volume-scale testing.
Produces realistic batch data across all ingestion paths (HL7 v2, FHIR R4, CSV).

Run:
    python data/synthetic/generate_synthetic_data.py

Output files (all written to data/synthetic/):
    hl7_adt_batch.txt        1000 HL7 v2 ADT^A01/A03 messages
    hl7_oru_batch.txt         500 HL7 v2 ORU^R01 lab result messages
    fhir_bundle_batch.json    500 FHIR R4 transaction Bundles (JSON array)
    ecw_patients.csv          300 eClinicalWorks-style patient rows
    ecw_labs.csv              500 eClinicalWorks-style lab result rows
    DATA_QUALITY_GUIDE.md     QA checklist for all injected data quality issues

Reproducibility: random.seed(42) and Faker.seed(42) — identical output every run.
"""

import csv
import json
import os
import random
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

try:
    from faker import Faker
except ImportError:
    print("ERROR: Faker not installed. Run: pip install faker")
    sys.exit(1)

# ── Configuration ─────────────────────────────────────────────────────────────

SEED = 42
PATIENT_COUNT = 500
OUTPUT_DIR = Path(__file__).parent

# Import the canonical tenant tag system URI from fhir_ingester so meta.tag
# stays in sync with extract_tenant_from_meta() without duplicating the string.
_repo_root = str(OUTPUT_DIR.parent.parent)
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)
from ingestion.fhir_ingester import TENANT_TAG_SYSTEM  # noqa: E402

TENANT_POOL = ["INTEGRIS_BAPTIST", "OU_HEALTH", "MERCY_OKC", "ST_FRANCIS_TULSA"]
TENANT_WEIGHTS = [0.40, 0.25, 0.20, 0.15]

SENDING_APP = {
    "INTEGRIS_BAPTIST": "EPIC",
    "OU_HEALTH":        "CERNER",
    "MERCY_OKC":        "MEDITECH",
    "ST_FRANCIS_TULSA": "EPIC",
}

PROVIDER_POOL = [
    ("NPI-1234567890", "Smith",    "Jonathan", "MD"),
    ("NPI-0987654321", "Patel",    "Anita",    "MD"),
    ("NPI-1122334455", "Johnson",  "David",    "MD"),
    ("NPI-9988776655", "Williams", "Sarah",    "MD"),
    ("NPI-5544332211", "Garcia",   "Maria",    "NP"),
    ("NPI-6677889900", "Brown",    "Michael",  "MD"),
    ("NPI-3344556677", "Davis",    "Jennifer", "MD"),
    ("NPI-7788990011", "Wilson",   "Robert",   "NP"),
]

# ICD-10 codes present in silver_to_gold SNOMED map → will map successfully
MAPPED_ICD10 = [
    ("I21.9", "Acute myocardial infarction, unspecified"),
    ("I10",   "Essential (primary) hypertension"),
    ("E11.9", "Type 2 diabetes mellitus without complications"),
    ("E11.65","Type 2 diabetes mellitus with hyperglycemia"),
    ("N18.3", "Chronic kidney disease, stage 3"),
    ("N18.4", "Chronic kidney disease, stage 4"),
    ("J44.1", "COPD with acute exacerbation"),
    ("I50.9", "Heart failure, unspecified"),
    ("F32.9", "Major depressive disorder, unspecified"),
]

# ICD-10 codes NOT in the SNOMED map → will produce UNMAPPED entries in terminology_unmapped_codes
UNMAPPED_ICD10 = [
    ("Z00.00", "Encounter for general adult medical examination"),
    ("Z23",    "Encounter for immunization"),
    ("M54.5",  "Low back pain"),
    ("J06.9",  "Acute upper respiratory infection, unspecified"),
    ("K21.0",  "GERD with esophagitis"),
]

# Lab tests: (loinc_code, local_display_name, unit, ref_low, ref_high, typical_low, typical_high)
KNOWN_LABS = [
    ("4548-4",  "hba1c",             "%",    4.8,  5.6,  4.0,  13.0),
    ("2345-7",  "glucose",           "mg/dL",70,   99,   65,   350),
    ("2160-0",  "creatinine",        "mg/dL",0.7,  1.3,  0.5,  8.0),
    ("6690-2",  "wbc",               "K/uL", 4.5,  11.0, 2.0,  25.0),
    ("718-7",   "hemoglobin",        "g/dL", 12.0, 17.5, 6.0,  18.5),
    ("777-3",   "platelet count",    "K/uL", 150,  400,  50,   700),
    ("2089-1",  "ldl cholesterol",   "mg/dL",0,    99,   40,   250),
    ("2085-9",  "hdl",               "mg/dL",40,   60,   20,   100),
    ("2093-3",  "total cholesterol", "mg/dL",0,    199,  100,  350),
]

# eClinicalWorks local codes NOT in LOINC map → trigger unmapped terminology log
ECW_UNMAPPED_CODES = [
    ("ECW-HBA1C-001",  "Glycosylated Hemoglobin (ECW)"),
    ("ECW-GLUC-002",   "Fasting Blood Sugar (ECW)"),
    ("ECW-CREAT-003",  "Serum Creatinine (ECW)"),
    ("ECW-CHOL-004",   "Total Cholesterol Panel (ECW)"),
    ("ECW-CBC-DIFF",   "Complete Blood Count with Differential (ECW)"),
    ("ECW-BMP-007",    "Basic Metabolic Panel (ECW)"),
    ("ECW-TSH-008",    "Thyroid Stimulating Hormone (ECW)"),
    ("ECW-PSA-009",    "Prostate Specific Antigen (ECW)"),
    ("ECW-INR-010",    "International Normalized Ratio (ECW)"),
    ("ECW-URIC-011",   "Uric Acid, Serum (ECW)"),
]

# eClinicalWorks local codes that DO map (display string matches LOINC map key)
ECW_MAPPED_CODES = [
    ("ECW-A1C",    "hba1c",            "%",    4.8,  5.6,  4.0,  13.0),
    ("ECW-GLUFS",  "fasting glucose",  "mg/dL",70,   99,   65,   200),
    ("ECW-CRTN",   "creatinine",       "mg/dL",0.7,  1.3,  0.5,  6.0),
    ("ECW-WBC",    "wbc",              "K/uL", 4.5,  11.0, 2.0,  20.0),
    ("ECW-HGB",    "hemoglobin",       "g/dL", 12.0, 17.5, 6.0,  18.0),
    ("ECW-PLT",    "platelet count",   "K/uL", 150,  400,  50,   600),
    ("ECW-LDL",    "ldl cholesterol",  "mg/dL",0,    99,   40,   220),
    ("ECW-HDL",    "hdl",              "mg/dL",40,   60,   20,   90),
    ("ECW-CHOL",   "total cholesterol","mg/dL",0,    199,  100,  320),
]

MARITAL_CODES = ["S", "M", "D", "W"]
RACE_CODES = [
    "2106-3^White^HL70005",
    "2054-5^Black or African American^HL70005",
    "2028-9^Asian^HL70005",
    "2076-8^Native Hawaiian or Other Pacific Islander^HL70005",
    "1002-5^American Indian or Alaska Native^HL70005",
]
DISCHARGE_DISPOS = [
    ("01", "Discharged to home"),
    ("02", "Discharged to skilled nursing facility"),
    ("03", "Discharged to skilled nursing facility"),
    ("06", "Discharged to home with home health service"),
    ("07", "Left against medical advice"),
]

# Invalid LOINC codes for DQ injection into FHIR bundles
INVALID_LOINC_CODES = [
    "INVALID-9999-1", "INVALID-9999-2", "INVALID-9999-3",
    "LOCAL-GLUC-001", "LOCAL-HBA1C-01", "UNMAPPED-TEST-1",
    "ECW-LOCAL-0001", "ECW-LOCAL-0002", "BAD-LOINC-0001",
    "NOLOINC-000001",
]


# ── Patient pool ──────────────────────────────────────────────────────────────

@dataclass
class Patient:
    patient_id: int
    first_name: str
    last_name: str
    dob: date
    gender: str            # M or F
    ssn_last4: Optional[str]
    address: str
    city: str
    state: str
    postal_code: str
    phone: str
    tenant_id: str
    mrn: str
    pcp_npi: str


def build_patient_pool(fake: Faker, rng: random.Random) -> list[Patient]:
    """
    Generate 500 synthetic patients.
    Patient 1 = Carlos Ramirez (DOB 1976-12-04, INTEGRIS_BAPTIST) — reserved.
    80% have SSN-4; 20% null to exercise MPI pass fallback.
    """
    carlos = Patient(
        patient_id=1,
        first_name="Carlos",
        last_name="Ramirez",
        dob=date(1976, 12, 4),
        gender="M",
        ssn_last4="6789",
        address="742 Evergreen Terrace",
        city="Oklahoma City",
        state="OK",
        postal_code="73102",
        phone="(405)555-0182",
        tenant_id="INTEGRIS_BAPTIST",
        mrn="MRN-29471",
        pcp_npi=PROVIDER_POOL[0][0],
    )

    patients: list[Patient] = [carlos]
    ok_cities = ["Oklahoma City", "Tulsa", "Norman", "Broken Arrow", "Edmond",
                 "Lawton", "Moore", "Midwest City", "Enid", "Stillwater"]
    ok_states = ["OK"] * 7 + ["TX", "KS", "AR"]

    for i in range(2, PATIENT_COUNT + 1):
        gender = rng.choice(["M", "F"])
        first_name = fake.first_name_male() if gender == "M" else fake.first_name_female()
        last_name = fake.last_name()
        age_days = rng.randint(18 * 365, 85 * 365)
        dob = date(2025, 1, 1) - timedelta(days=age_days)
        ssn_last4 = f"{rng.randint(1000, 9999)}" if rng.random() < 0.80 else None
        tenant_id = rng.choices(TENANT_POOL, weights=TENANT_WEIGHTS)[0]
        pcp_npi = rng.choice(PROVIDER_POOL)[0]
        state = rng.choice(ok_states)
        city = rng.choice(ok_cities) if state == "OK" else fake.city()
        mrn_num = rng.randint(10000, 99999)
        patients.append(Patient(
            patient_id=i,
            first_name=first_name,
            last_name=last_name,
            dob=dob,
            gender=gender,
            ssn_last4=ssn_last4,
            address=fake.street_address(),
            city=city,
            state=state,
            postal_code=fake.numerify("#####"),
            phone=fake.numerify("(###)###-####"),
            tenant_id=tenant_id,
            mrn=f"MRN-{mrn_num}",
            pcp_npi=pcp_npi,
        ))

    return patients


# ── HL7 helpers ───────────────────────────────────────────────────────────────

def hl7_ts(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def hl7_date(d: date) -> str:
    return d.strftime("%Y%m%d")


def provider_hl7(npi: str, last: str, first: str, cred: str) -> str:
    return f"{npi}^{last}^{first}^^^{cred}^^^^^^^^^NPI"


def build_a01(
    patient: Patient,
    admit_dt: datetime,
    visit_num: str,
    msg_ctrl: str,
    batch_id: str,
    rng: random.Random,
    *,
    override_name: Optional[str] = None,
    override_dob: Optional[str] = None,
    override_gender: Optional[str] = None,
    override_facility: Optional[str] = None,
    override_tenant: Optional[str] = None,
) -> str:
    tenant = override_tenant if override_tenant is not None else patient.tenant_id
    app = SENDING_APP[tenant]
    facility = override_facility if override_facility is not None else tenant
    ts = hl7_ts(admit_dt)

    name = override_name if override_name is not None else (
        f"{patient.last_name}^{patient.first_name}^^^"
    )
    dob = override_dob if override_dob is not None else hl7_date(patient.dob)
    gender = override_gender if override_gender is not None else patient.gender

    prov = rng.choice(PROVIDER_POOL)
    prov_hl7 = provider_hl7(prov[0], prov[1], prov[2], prov[3])
    race = rng.choice(RACE_CODES)
    marital = rng.choice(MARITAL_CODES)
    icd = rng.choice(MAPPED_ICD10)
    loc = f"MED^{rng.randint(100,499)}^A^{tenant}^^N^^^{tenant}"
    acct = f"ACCT-{rng.randint(100000,999999)}"

    ssn_seg = ""
    if patient.ssn_last4:
        ssn_seg = f"~SSN-XXXXX{patient.ssn_last4}^^^{tenant}^SS"

    staff = rng.choice(PROVIDER_POOL)
    staff_hl7 = f"STAFF{rng.randint(100,999)}^{staff[1]}^{staff[2]}^^^{staff[3]}"

    lines = [
        f"MSH|^~\\&|{app}|{facility}|HIE_GATEWAY|OKLAHOMA_HDU|{ts}||ADT^A01^ADT_A01|{msg_ctrl}|P|2.5.1|||NE|AL|USA|ASCII|||",
        f"EVN|A01|{ts}|||{staff_hl7}|{ts}",
        f"PID|1||{patient.mrn}^^^{tenant}^MR{ssn_seg}||{name}||{dob}|{gender}||{race}|{patient.address}^^{patient.city}^{patient.state}^{patient.postal_code}^USA^H||{patient.phone}|||{marital}||{acct}",
        f"PV1|1|I|{loc}|E^Emergency^HL70007||{prov_hl7}|{prov_hl7}|||MED|||||||{prov_hl7}|INP^Inpatient^HL70018|{visit_num}||||||||||||||||||{tenant}||ADM|{ts}|||",
        f"DG1|1||{icd[0]}^{icd[1]}^ICD10|||W",
        f"ZTN|TENANT_ID={tenant}|SOURCE_SYSTEM={app}|FEED_TYPE=ADT|BATCH_ID={batch_id}|RECEIVED_TS={ts}",
    ]
    return "\n".join(lines)


def build_a03(
    patient: Patient,
    discharge_dt: datetime,
    visit_num: str,
    msg_ctrl: str,
    batch_id: str,
    rng: random.Random,
    *,
    override_tenant: Optional[str] = None,
) -> str:
    tenant = override_tenant if override_tenant is not None else patient.tenant_id
    app = SENDING_APP[tenant]
    ts = hl7_ts(discharge_dt)
    dispo = rng.choice(DISCHARGE_DISPOS)
    loc = f"MED^{rng.randint(100,499)}^A^{tenant}^^N^^^{tenant}"
    prov = rng.choice(PROVIDER_POOL)
    prov_hl7 = provider_hl7(prov[0], prov[1], prov[2], prov[3])
    name = f"{patient.last_name}^{patient.first_name}^^^"
    dob = hl7_date(patient.dob)

    lines = [
        f"MSH|^~\\&|{app}|{tenant}|HIE_GATEWAY|OKLAHOMA_HDU|{ts}||ADT^A03^ADT_A03|{msg_ctrl}|P|2.5.1|||NE|AL|USA|ASCII|||",
        f"EVN|A03|{ts}",
        f"PID|1||{patient.mrn}^^^{tenant}^MR||{name}||{dob}|{patient.gender}",
        f"PV1|1|I|{loc}||{dispo[0]}^{dispo[1]}^HL70112|||MED|||||||{prov_hl7}|INP|{visit_num}",
        f"ZTN|TENANT_ID={tenant}|SOURCE_SYSTEM={app}|FEED_TYPE=ADT|BATCH_ID={batch_id}|RECEIVED_TS={ts}",
    ]
    return "\n".join(lines)


# ── HL7 ADT batch ─────────────────────────────────────────────────────────────

def generate_hl7_adt(patients: list[Patient], rng: random.Random) -> str:
    """
    Generate 1000 HL7 ADT messages: 500 A01 (admit) + 500 A03 (discharge).

    Data quality issues injected:
    - A01 indices 10-19 : missing PID-5 (patient name)
    - A01 indices 20-29 : malformed DOB in PID-7
    - A01 indices 30-39 : invalid gender code in PID-8
    - A01 indices 40-49 : missing MSH-4 (sending facility)
    - A01 indices 490-499: duplicate of A01 indices 0-9 (same msg ctrl ID + patient)

    Returns the full file content as a string.
    """
    BAD_DOBS = [
        "19991399", "20001432", "00000000", "76/12/04",
        "UNKNOWN",  "99999999", "19850230", "20240631",
        "19800000", "20191301",
    ]
    INVALID_GENDERS = ["X", "?", "9", "MALE", "", "FEMALE", "TRANS", "NB", "3", "U1"]

    visit_base = datetime(2024, 1, 1, 8, 0, 0)

    messages: list[str] = []
    # Store first 10 A01 messages verbatim for duplicate injection
    first_ten_a01: list[str] = []

    for i, patient in enumerate(patients[:500]):
        offset_days = rng.randint(0, 364)
        offset_hours = rng.randint(0, 23)
        admit_dt = visit_base + timedelta(days=offset_days, hours=offset_hours)
        stay_days = rng.randint(1, 14)
        discharge_dt = admit_dt + timedelta(days=stay_days, hours=rng.randint(0, 8))

        visit_num = f"VN-2024-{i+1:06d}"
        admit_ts_str = hl7_ts(admit_dt)
        msg_ctrl_a01 = f"MSG{admit_ts_str}{i+1:06d}A01"
        msg_ctrl_a03 = f"MSG{hl7_ts(discharge_dt)}{i+1:06d}A03"
        batch_id = f"ADT-BATCH-2024-{i+1:05d}"

        # Even tenant distribution: 250 messages per tenant (125 patients × 2 msg each)
        tenant_override = TENANT_POOL[i % 4]

        # Apply DQ overrides for A01
        override_name     = None
        override_dob      = None
        override_gender   = None
        override_facility = None

        if 10 <= i <= 19:
            override_name = "^^^^^"                        # DQ: missing PID-5
        if 20 <= i <= 29:
            override_dob = BAD_DOBS[i - 20]               # DQ: malformed DOB
        if 30 <= i <= 39:
            override_gender = INVALID_GENDERS[i - 30]     # DQ: invalid gender
        if 40 <= i <= 49:
            override_facility = ""                         # DQ: missing MSH-4

        # Duplicate injection: A01 indices 490-499 repeat indices 0-9
        if 490 <= i <= 499:
            dup_msg = first_ten_a01[i - 490]
            messages.append(dup_msg)
        else:
            a01 = build_a01(
                patient, admit_dt, visit_num, msg_ctrl_a01, batch_id, rng,
                override_name=override_name,
                override_dob=override_dob,
                override_gender=override_gender,
                override_facility=override_facility,
                override_tenant=tenant_override,
            )
            messages.append(a01)
            if i < 10:
                first_ten_a01.append(a01)

        # A03 discharge (always clean)
        a03 = build_a03(patient, discharge_dt, visit_num, msg_ctrl_a03, batch_id, rng,
                        override_tenant=tenant_override)
        messages.append(a03)

    return "\n\n".join(messages) + "\n"


# ── HL7 ORU batch ─────────────────────────────────────────────────────────────

def generate_hl7_oru(patients: list[Patient], rng: random.Random) -> str:
    """
    Generate 500 HL7 ORU^R01 lab result messages.

    Data quality issues injected:
    - Message indices 0-9  : unmapped local lab codes (not in LOINC map)
    - Message indices 10-19: out-of-physiologically-plausible-range result values
    - Message indices 20-29: missing OBX-5 (observation value)

    Returns the full file content as a string.
    """
    OUT_OF_RANGE_VALUES = {
        "hba1c":           ("45.2", "%"),
        "glucose":         ("1520",  "mg/dL"),
        "creatinine":      ("28.7",  "mg/dL"),
        "wbc":             ("95.0",  "K/uL"),
        "hemoglobin":      ("0.8",   "g/dL"),
        "platelet count":  ("2100",  "K/uL"),
        "ldl cholesterol": ("890",   "mg/dL"),
        "hdl":             ("2",     "mg/dL"),
        "total cholesterol":("987",  "mg/dL"),
        "fasting glucose": ("1888",  "mg/dL"),
    }

    result_base = datetime(2024, 1, 15, 10, 0, 0)
    messages: list[str] = []

    # Use first 500 patients (cycling if needed)
    for i in range(500):
        patient = patients[i % len(patients)]
        offset_days = rng.randint(0, 350)
        result_dt = result_base + timedelta(days=offset_days, hours=rng.randint(0, 12))
        collect_dt = result_dt - timedelta(hours=rng.randint(1, 6))

        ts = hl7_ts(result_dt)
        collect_ts = hl7_ts(collect_dt)
        tenant = patient.tenant_id
        app = SENDING_APP[tenant] + "_LAB"
        msg_ctrl = f"MSG{ts}{i+1:06d}ORU"
        batch_id = f"ORU-BATCH-2024-{i+1:05d}"
        ord_id = f"ORD-2024-{rng.randint(100000,999999)}"
        fil_id = f"FIL-2024-{rng.randint(100000,999999)}"
        prov = rng.choice(PROVIDER_POOL)
        prov_hl7 = provider_hl7(prov[0], prov[1], prov[2], prov[3])
        name_hl7 = f"{patient.last_name}^{patient.first_name}^^^"

        if i < 10:
            # DQ: unmapped local lab code — OBX uses a non-LOINC local code
            unmapped = ECW_UNMAPPED_CODES[i]
            local_code = unmapped[0]
            local_display = unmapped[1]
            val = f"{rng.uniform(4.0, 12.0):.1f}"
            unit = "%"
            ref_range = "4.8-5.6"
            abnormal = "H" if float(val) > 5.6 else "N"
            obr_code = f"{local_code}^{local_display}^L"
            obx_code = f"{local_code}^{local_display}^L"
            obx_value = f"|{val}|{unit}^{unit}^UCUM|{ref_range}|{abnormal}^High^HL70078|||F"
        elif 10 <= i <= 19:
            # DQ: physiologically implausible result value
            lab = rng.choice(KNOWN_LABS)
            loinc, display, unit, ref_lo, ref_hi, _, _ = lab
            oor_val, oor_unit = OUT_OF_RANGE_VALUES.get(display, ("9999", "units"))
            obr_code = f"{loinc}^{display.title()}^LN"
            obx_code = f"{loinc}^{display.title()}^LN"
            obx_value = f"|{oor_val}|{oor_unit}^{oor_unit}^UCUM|{ref_lo}-{ref_hi}|H^High^HL70078|||F"
        elif 20 <= i <= 29:
            # DQ: missing OBX-5 (observation value field is empty)
            lab = rng.choice(KNOWN_LABS)
            loinc, display, unit, ref_lo, ref_hi, _, _ = lab
            obr_code = f"{loinc}^{display.title()}^LN"
            obx_code = f"{loinc}^{display.title()}^LN"
            obx_value = "|||" + f"{ref_lo}-{ref_hi}|||F"  # OBX-5 empty
        else:
            # Normal result
            lab = rng.choice(KNOWN_LABS)
            loinc, display, unit, ref_lo, ref_hi, typ_lo, typ_hi = lab
            val = round(rng.uniform(typ_lo, typ_hi), 1)
            abnormal = "H" if val > ref_hi else ("L" if val < ref_lo else "N")
            abn_display = "High" if abnormal == "H" else ("Low" if abnormal == "L" else "Normal")
            obr_code = f"{loinc}^{display.title()}^LN"
            obx_code = f"{loinc}^{display.title()}^LN"
            obx_value = f"|{val}|{unit}^{unit}^UCUM|{ref_lo}-{ref_hi}|{abnormal}^{abn_display}^HL70078|||F"

        msh = f"MSH|^~\\&|{app}|{tenant}|HIE_GATEWAY|OKLAHOMA_HDU|{ts}||ORU^R01^ORU_R01|{msg_ctrl}|P|2.5.1|||NE|AL|USA|ASCII|||"
        pid = f"PID|1||{patient.mrn}^^^{tenant}^MR||{name_hl7}||{hl7_date(patient.dob)}|{patient.gender}"
        pv1 = f"PV1|1|O|CLINIC^001^A^{tenant}|||{prov_hl7}|||LAB"
        orc = f"ORC|RE|{ord_id}^{tenant}_LAB|{fil_id}^{tenant}_LAB||CM||||{collect_ts}|||{prov_hl7}"
        obr = f"OBR|1|{ord_id}^{tenant}_LAB|{fil_id}^{tenant}_LAB|{obr_code}|||{collect_ts}|||||||||{prov_hl7}||||||{ts}|||F"
        obx = f"OBX|1|NM|{obx_code}|1{obx_value}|||{hl7_ts(result_dt)}||LAB-TECH-{rng.randint(100,999)}"
        ztn = f"ZTN|TENANT_ID={tenant}|SOURCE_SYSTEM={app}|FEED_TYPE=ORU|BATCH_ID={batch_id}|RECEIVED_TS={ts}"

        messages.append("\n".join([msh, pid, pv1, orc, obr, obx, ztn]))

    return "\n\n".join(messages) + "\n"


# ── FHIR R4 bundle batch ──────────────────────────────────────────────────────

def fhir_reference(res_type: str, fhir_id: str) -> str:
    return f"{res_type}/{fhir_id}"


def build_fhir_patient(patient: Patient, fhir_id: str) -> dict:
    resource = {
        "resourceType": "Patient",
        "id": fhir_id,
        "identifier": [
            {
                "use": "official",
                "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "MR"}]},
                "system": f"urn:oid:{patient.tenant_id.lower()}.mrn",
                "value": patient.mrn,
            }
        ],
        "name": [
            {
                "use": "official",
                "family": patient.last_name,
                "given": [patient.first_name],
            }
        ],
        "gender": "male" if patient.gender == "M" else "female",
        "birthDate": patient.dob.isoformat(),
        "address": [
            {
                "use": "home",
                "line": [patient.address],
                "city": patient.city,
                "state": patient.state,
                "postalCode": patient.postal_code,
                "country": "US",
            }
        ],
    }
    if patient.ssn_last4:
        resource["identifier"].append({
            "use": "secondary",
            "type": {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "SS"}]},
            "system": "http://hl7.org/fhir/sid/us-ssn",
            "value": f"XXX-XX-{patient.ssn_last4}",
        })
    return resource


def build_fhir_encounter(
    patient_ref: str,
    enc_id: str,
    admit_dt: datetime,
    discharge_dt: datetime,
    tenant: str,
    prov: tuple,
) -> dict:
    return {
        "resourceType": "Encounter",
        "id": enc_id,
        "status": "finished",
        "class": {
            "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
            "code": "IMP",
            "display": "inpatient encounter",
        },
        "subject": {"reference": patient_ref},
        "period": {
            "start": admit_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "end": discharge_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        },
        "participant": [
            {
                "type": [{"coding": [{"system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType", "code": "ATND"}]}],
                "individual": {"display": f"{prov[2]} {prov[1]}, {prov[3]}"},
            }
        ],
        "serviceProvider": {"display": tenant},
    }


def build_fhir_observation(
    patient_ref: str,
    enc_ref: str,
    obs_id: str,
    loinc_code: str,
    loinc_display: str,
    value: float,
    unit: str,
    ref_lo: float,
    ref_hi: float,
    effective_dt: datetime,
) -> dict:
    abnormal = "H" if value > ref_hi else ("L" if value < ref_lo else "N")
    return {
        "resourceType": "Observation",
        "id": obs_id,
        "status": "final",
        "code": {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": loinc_code,
                    "display": loinc_display,
                }
            ],
            "text": loinc_display,
        },
        "subject": {"reference": patient_ref},
        "encounter": {"reference": enc_ref},
        "effectiveDateTime": effective_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "issued": effective_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "valueQuantity": {
            "value": value,
            "unit": unit,
            "system": "http://unitsofmeasure.org",
            "code": unit,
        },
        "interpretation": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
                        "code": abnormal,
                    }
                ]
            }
        ],
        "referenceRange": [{"low": {"value": ref_lo, "unit": unit}, "high": {"value": ref_hi, "unit": unit}}],
        "observation_status": "final",
    }


def build_fhir_condition(
    patient_ref: str,
    enc_ref: str,
    cond_id: str,
    icd10_code: str,
    icd10_display: str,
    onset_dt: datetime,
    rank: int = 1,
) -> dict:
    return {
        "resourceType": "Condition",
        "id": cond_id,
        "clinicalStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-clinical", "code": "active"}]
        },
        "verificationStatus": {
            "coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-ver-status", "code": "confirmed"}]
        },
        "category": [
            {"coding": [{"system": "http://terminology.hl7.org/CodeSystem/condition-category", "code": "encounter-diagnosis"}]}
        ],
        "code": {
            "coding": [
                {
                    "system": "http://hl7.org/fhir/sid/icd-10-cm",
                    "code": icd10_code,
                    "display": icd10_display,
                }
            ],
            "text": icd10_display,
        },
        "subject": {"reference": patient_ref},
        "encounter": {"reference": enc_ref},
        "onsetDateTime": onset_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        "rank": rank,
    }


def generate_fhir_bundles(patients: list[Patient], rng: random.Random) -> str:
    """
    Generate 500 FHIR R4 transaction Bundles.

    Data quality issues injected:
    - Bundle indices 0-9  : Patient resource missing birthDate
    - Bundle indices 10-19: Observation uses invalid LOINC codes
    - Bundle indices 20-29: Bundle.meta.tag is empty (tenant cannot be resolved)

    Returns JSON string of a top-level array of 500 bundle objects.
    """
    bundles = []
    visit_base = datetime(2024, 2, 1, 8, 0, 0)

    for i, patient in enumerate(patients[:500]):
        bundle_id = str(uuid.uuid4())
        patient_fhir_id = f"patient-{patient.patient_id:05d}"
        enc_fhir_id = f"encounter-{i+1:05d}"
        tenant = TENANT_POOL[i % 4]  # even distribution: 125 bundles per tenant

        offset_days = rng.randint(0, 330)
        admit_dt = visit_base + timedelta(days=offset_days, hours=rng.randint(6, 20))
        discharge_dt = admit_dt + timedelta(days=rng.randint(1, 10))
        prov = rng.choice(PROVIDER_POOL)

        # Patient resource
        fhir_patient = build_fhir_patient(patient, patient_fhir_id)
        if i < 10:
            fhir_patient.pop("birthDate", None)   # DQ: missing birthDate

        # Encounter resource
        fhir_encounter = build_fhir_encounter(
            patient_ref=f"urn:uuid:{patient_fhir_id}",
            enc_id=enc_fhir_id,
            admit_dt=admit_dt,
            discharge_dt=discharge_dt,
            tenant=tenant,
            prov=prov,
        )

        entries = [
            {"fullUrl": f"urn:uuid:{patient_fhir_id}", "resource": fhir_patient,
             "request": {"method": "PUT", "url": f"Patient/{patient_fhir_id}"}},
            {"fullUrl": f"urn:uuid:{enc_fhir_id}", "resource": fhir_encounter,
             "request": {"method": "POST", "url": "Encounter"}},
        ]

        # Observation
        lab = rng.choice(KNOWN_LABS)
        loinc, display, unit, ref_lo, ref_hi, typ_lo, typ_hi = lab
        obs_value = round(rng.uniform(typ_lo, typ_hi), 1)
        obs_id = f"obs-{i+1:05d}"
        effective_dt = admit_dt + timedelta(hours=rng.randint(2, 20))

        if 10 <= i <= 19:
            # DQ: invalid LOINC code
            bad_code = INVALID_LOINC_CODES[i - 10]
            obs = build_fhir_observation(
                patient_ref=f"urn:uuid:{patient_fhir_id}",
                enc_ref=f"urn:uuid:{enc_fhir_id}",
                obs_id=obs_id,
                loinc_code=bad_code,
                loinc_display=f"Unknown test ({bad_code})",
                value=obs_value,
                unit=unit,
                ref_lo=ref_lo,
                ref_hi=ref_hi,
                effective_dt=effective_dt,
            )
        else:
            obs = build_fhir_observation(
                patient_ref=f"urn:uuid:{patient_fhir_id}",
                enc_ref=f"urn:uuid:{enc_fhir_id}",
                obs_id=obs_id,
                loinc_code=loinc,
                loinc_display=display.title(),
                value=obs_value,
                unit=unit,
                ref_lo=ref_lo,
                ref_hi=ref_hi,
                effective_dt=effective_dt,
            )
        entries.append({
            "fullUrl": f"urn:uuid:{obs_id}",
            "resource": obs,
            "request": {"method": "POST", "url": "Observation"},
        })

        # Condition (50% of bundles)
        if rng.random() < 0.50:
            cond_icd = rng.choice(MAPPED_ICD10)
            cond_id = f"cond-{i+1:05d}"
            cond = build_fhir_condition(
                patient_ref=f"urn:uuid:{patient_fhir_id}",
                enc_ref=f"urn:uuid:{enc_fhir_id}",
                cond_id=cond_id,
                icd10_code=cond_icd[0],
                icd10_display=cond_icd[1],
                onset_dt=admit_dt,
            )
            entries.append({
                "fullUrl": f"urn:uuid:{cond_id}",
                "resource": cond,
                "request": {"method": "POST", "url": "Condition"},
            })

        # Bundle meta.tag (DQ: empty for indices 20-29)
        if 20 <= i <= 29:
            meta = {"tag": []}   # DQ: missing tenant tag
        else:
            meta = {
                "tag": [
                    {
                        "system": TENANT_TAG_SYSTEM,
                        "code": tenant,
                        "display": tenant,
                    }
                ]
            }

        bundle = {
            "resourceType": "Bundle",
            "id": bundle_id,
            "meta": meta,
            "type": "transaction",
            "timestamp": admit_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "entry": entries,
        }
        bundles.append(bundle)

    return json.dumps(bundles, indent=2)


# ── eClinicalWorks patient CSV ────────────────────────────────────────────────

def generate_ecw_patients(patients: list[Patient], rng: random.Random) -> list[dict]:
    """
    Generate 300 eClinicalWorks-style patient rows.
    Draws from the master patient pool — same patients appear across ingestion paths.

    Data quality issues injected (20 total):
    - Rows 0-4  : blank first_name or last_name
    - Rows 5-9  : invalid ICD-10 codes
    - Rows 10-14: DOB in wrong format (MM/DD/YYYY)
    - Rows 15-19: fully duplicate rows (exact copy of rows 0-4 inserted again at 295-299)
    """
    INVALID_ICD10_CODES = [
        "ZZ999", "BADCODE", "99999", "X00.000000", "123",
    ]
    pcp_npi_pool = [p[0] for p in PROVIDER_POOL]
    ins_pool = [
        "BCBSOK-PPO", "AETNA-HMO", "CIGNA-PPO", "HUMANA-PPO",
        "UNITEDHC", "MEDICAID-OK", "MEDICARE-A", "SELFPAY",
    ]

    selected = rng.sample(patients, 295)  # 295 unique + 5 duplicates appended = 300
    rows = []

    for i, patient in enumerate(selected):
        dob_str = patient.dob.isoformat()
        first_name = patient.first_name
        last_name = patient.last_name

        if i < 5:
            if rng.random() < 0.5:
                first_name = ""
            else:
                last_name = ""

        icd10 = rng.choice(MAPPED_ICD10 + UNMAPPED_ICD10)[0]
        if 5 <= i <= 9:
            icd10 = INVALID_ICD10_CODES[i - 5]

        if 10 <= i <= 14:
            # Wrong date format: MM/DD/YYYY
            dob_str = patient.dob.strftime("%m/%d/%Y")

        last_visit = (patient.dob + timedelta(days=rng.randint(365 * 18, 365 * 80))).isoformat()
        if last_visit > date.today().isoformat():
            last_visit = date.today().isoformat()

        rows.append({
            "patient_id":     f"ECW-{patient.patient_id:05d}",
            "first_name":     first_name,
            "last_name":      last_name,
            "dob":            dob_str,
            "gender":         patient.gender,
            "ssn_last4":      patient.ssn_last4 or "",
            "address":        patient.address,
            "city":           patient.city,
            "state":          patient.state,
            "zip":            patient.postal_code,
            "phone":          patient.phone,
            "pcp_npi":        rng.choice(pcp_npi_pool),
            "insurance_id":   rng.choice(ins_pool),
            "last_visit_date":last_visit,
            "primary_dx_icd10": icd10,
            "tenant_id":      patient.tenant_id,
        })

    # Rows 15-19 are DQ: exact duplicates of rows 0-4 (appended at end)
    for j in range(5):
        rows.append(dict(rows[j]))

    return rows


# ── eClinicalWorks lab CSV ────────────────────────────────────────────────────

def generate_ecw_labs(patients: list[Patient], rng: random.Random) -> list[dict]:
    """
    Generate 500 eClinicalWorks lab result rows.
    Uses eClinicalWorks local codes — exercises terminology normalization.

    Data quality issues injected (20 total):
    - Rows 0-9  : local codes with no LOINC mapping (triggers unmapped log)
    - Rows 10-19: result_value is text when numeric expected (e.g., "See note")
    """
    TEXT_RESULT_VALUES = [
        "See note", "Pending confirmation", "Quantity not sufficient",
        "Cancelled", "Specimen rejected", "Interfering substance",
        "Unable to calculate", "Not applicable", "Result discarded", "Repeat requested",
    ]

    prov_npi_pool = [p[0] for p in PROVIDER_POOL]
    rows = []

    for i in range(500):
        patient = rng.choice(patients[:300])
        collect_date = (date(2024, 1, 1) + timedelta(days=rng.randint(0, 364))).isoformat()
        result_id = f"ECW-LAB-{i+1:06d}"

        if i < 10:
            # DQ: unmapped local code
            code_info = ECW_UNMAPPED_CODES[i]
            test_code = code_info[0]
            test_name = code_info[1]
            result_value = str(round(rng.uniform(1.0, 20.0), 1))
            result_unit = "units"
            ref_low = ""
            ref_high = ""
            abnormal_flag = "U"
        elif 10 <= i <= 19:
            # DQ: text value when numeric expected
            lab = rng.choice(ECW_MAPPED_CODES)
            test_code, test_name, unit, ref_lo, ref_hi, typ_lo, typ_hi = lab
            result_value = TEXT_RESULT_VALUES[i - 10]
            result_unit = unit
            ref_low = str(ref_lo)
            ref_high = str(ref_hi)
            abnormal_flag = ""
        else:
            # Normal: mix of mapped and unmapped codes
            if rng.random() < 0.7:
                # Mapped code
                lab = rng.choice(ECW_MAPPED_CODES)
                test_code, test_name, unit, ref_lo, ref_hi, typ_lo, typ_hi = lab
                val = round(rng.uniform(typ_lo, typ_hi), 1)
                result_value = str(val)
                result_unit = unit
                ref_low = str(ref_lo)
                ref_high = str(ref_hi)
                abnormal_flag = "H" if val > ref_hi else ("L" if val < ref_lo else "N")
            else:
                # Unmapped code
                code_info = rng.choice(ECW_UNMAPPED_CODES)
                test_code = code_info[0]
                test_name = code_info[1]
                val = round(rng.uniform(1.0, 50.0), 1)
                result_value = str(val)
                result_unit = "units"
                ref_low = ""
                ref_high = ""
                abnormal_flag = "U"

        rows.append({
            "result_id":              result_id,
            "patient_id":             f"ECW-{patient.patient_id:05d}",
            "collection_date":        collect_date,
            "test_code":              test_code,
            "test_name":              test_name,
            "result_value":           result_value,
            "result_unit":            result_unit,
            "reference_range_low":    ref_low,
            "reference_range_high":   ref_high,
            "abnormal_flag":          abnormal_flag,
            "ordering_provider_npi":  rng.choice(prov_npi_pool),
            "status":                 "final",
        })

    return rows


# ── DATA_QUALITY_GUIDE.md ─────────────────────────────────────────────────────

DATA_QUALITY_GUIDE = """\
# DATA_QUALITY_GUIDE.md — Synthetic Data Quality Issue Registry

Every data quality issue injected across the synthetic batch files is documented
here. Use this as your QA checklist when running the pipeline: each issue should
produce a specific, auditable outcome in the Silver or Gold layer. Nothing should
be silently dropped.

---

## Summary

| File | Issue Type | Count | Pipeline Component | Expected Outcome |
|---|---|---|---|---|
| hl7_adt_batch.txt | Missing PID-5 (patient name) | 10 | HL7 parser → Bronze | `processing_status=PENDING`; name fields null in hl7_messages |
| hl7_adt_batch.txt | Malformed DOB in PID-7 | 10 | HL7 parser → Bronze | `processing_status=PENDING`; message_datetime null or raw value preserved |
| hl7_adt_batch.txt | Invalid gender code in PID-8 | 10 | HL7 parser → Bronze | `processing_status=PENDING`; gender value preserved as-is |
| hl7_adt_batch.txt | Duplicate ADT^A01 messages | 10 | Bronze dedup / MPI | Duplicate rows in hl7_messages; MPI returns same UMPI for both |
| hl7_adt_batch.txt | Missing MSH-4 (sending facility) | 10 | HL7 parser → Bronze | `processing_status=PENDING`; sending_facility null; tenant fallback |
| hl7_oru_batch.txt | Unmapped local lab codes | 10 | Terminology service | UNMAPPED entry in `terminology_unmapped_codes`; loinc_mapped=False |
| hl7_oru_batch.txt | Out-of-range result values | 10 | Bronze ingest / Silver QA | Value preserved in Bronze and Silver; out-of-range flag for QA scorecard |
| hl7_oru_batch.txt | Missing OBX-5 (observation value) | 10 | ORU parser → Bronze | `processing_status=PENDING`; value fields null in lab_observations |
| fhir_bundle_batch.json | Missing Patient.birthDate | 10 | FHIR ingester → MPI | MPI pass 4 (DOB+name+zip) skipped; NEW_RECORD minted; birthDate null |
| fhir_bundle_batch.json | Invalid LOINC codes in Observation | 10 | Bronze→Silver normalization | loinc_mapped=False; SOURCE_LOINC path still writes code; unmapped log entry |
| fhir_bundle_batch.json | Missing Bundle.meta.tag (tenant) | 10 | FHIR ingester → Bronze | tenant_id=default or null; fhir_resources row still written (PENDING) |
| ecw_patients.csv | Blank first_name or last_name | 5 | CSV ingester → Bronze | Row written; name fields null; MPI may degrade to pass 2 |
| ecw_patients.csv | Invalid ICD-10 codes | 5 | Bronze→Silver (Condition) | ICD-10 written as-is; SNOMED map returns UNMAPPED |
| ecw_patients.csv | DOB in wrong format (MM/DD/YYYY) | 5 | CSV ingester / MPI | DOB parse failure; birth_date null; MPI falls back to lower passes |
| ecw_patients.csv | Fully duplicate rows | 5 | Bronze dedup / MPI | Duplicate rows written; MPI returns same UMPI (DETERMINISTIC match) |
| ecw_labs.csv | Local codes with no LOINC mapping | 10 | Terminology service | UNMAPPED in `terminology_unmapped_codes`; loinc_mapped=False |
| ecw_labs.csv | Text result when numeric expected | 10 | CSV ingester / Silver | value_quantity null; value_string populated; preserved for audit |

**Total injected issues: 130**

---

## Detailed Issue Registry

### hl7_adt_batch.txt

**File:** `data/synthetic/hl7_adt_batch.txt`
**Total messages:** 1000 (500 ADT^A01 + 500 ADT^A03)
**Generator:** `generate_synthetic_data.py` → `generate_hl7_adt()`

#### DQ-ADT-001: Missing PID-5 (patient name)
- **Messages affected:** A01 messages for patients at index 10–19 (20 messages in the file)
- **PID-5 value:** `^^^^^` (empty components)
- **Pipeline component:** HL7 parser (`ingestion/hl7_parser.py`)
- **Expected Bronze outcome:** Row written with `processing_status=PENDING`; `sending_application` and facility captured; name fields null
- **MPI impact:** MPI pass 1 (name+DOB+zip) cannot match on name; pass 2 (SSN4) attempted if available
- **QA check:** `SELECT * FROM dev.fhir_bronze.hl7_messages WHERE message_type = 'ADT^A01' AND sending_application IS NOT NULL` — confirm name-related fields are null for affected rows

#### DQ-ADT-002: Malformed DOB in PID-7
- **Messages affected:** A01 messages for patients at index 20–29
- **PID-7 values injected:** `19991399`, `20001432`, `00000000`, `76/12/04`, `UNKNOWN`, `99999999`, `19850230`, `20240631`, `19800000`, `20191301`
- **Pipeline component:** HL7 timestamp parser (`parse_hl7_timestamp()` in `hl7_parser.py`)
- **Expected Bronze outcome:** Row written; `message_datetime` null or raw value preserved (parser handles gracefully)
- **QA check:** Confirm no exceptions thrown; rows land with PENDING status

#### DQ-ADT-003: Invalid gender code in PID-8
- **Messages affected:** A01 messages for patients at index 30–39
- **PID-8 values injected:** `X`, `?`, `9`, `MALE`, `""`, `FEMALE`, `TRANS`, `NB`, `3`, `U1`
- **Pipeline component:** HL7 parser
- **Expected Bronze outcome:** Row written with PENDING status; gender value preserved verbatim
- **Silver impact:** Gender normalization step should flag unrecognized values

#### DQ-ADT-004: Duplicate ADT^A01 messages
- **Messages affected:** A01 messages for patients at index 490–499 are exact copies of A01 messages for patients at index 0–9 (same MSH-10 message control ID, same PID data, same visit number)
- **Pipeline component:** Bronze ingest → MPI → Silver
- **Expected Bronze outcome:** Both rows written to `hl7_messages` (Bronze is append-only)
- **Expected MPI outcome:** Second resolution returns `match_method=DETERMINISTIC` with same UMPI as first
- **QA check:** `SELECT mrn, COUNT(*) FROM hl7_messages WHERE message_type='ADT^A01' GROUP BY mrn HAVING COUNT(*) > 1`

#### DQ-ADT-005: Missing MSH-4 (sending facility)
- **Messages affected:** A01 messages for patients at index 40–49
- **MSH-4 value:** empty string
- **Pipeline component:** HL7 parser → tenant extraction
- **Expected Bronze outcome:** Row written; `sending_facility` null; tenant resolved from ZTN segment (fallback works) or defaults
- **QA check:** Confirm `sending_facility IS NULL` for these rows; verify tenant_id resolved via ZTN

---

### hl7_oru_batch.txt

**File:** `data/synthetic/hl7_oru_batch.txt`
**Total messages:** 500 ORU^R01
**Generator:** `generate_hl7_oru()`

#### DQ-ORU-001: Unmapped local lab codes
- **Messages affected:** Indices 0–9 (10 messages)
- **Codes used:** ECW-HBA1C-001, ECW-GLUC-002, ECW-CREAT-003, ECW-CHOL-004, ECW-CBC-DIFF, ECW-BMP-007, ECW-TSH-008, ECW-PSA-009, ECW-INR-010, ECW-URIC-011
- **OBX code system:** `L` (local) — not `LN` (LOINC)
- **Pipeline component:** Terminology service (`TerminologyService.map_loinc()`)
- **Expected Silver outcome:** `loinc_mapped=False`; `loinc_map_method=UNMAPPED`; entry in `terminology_unmapped_codes`
- **QA check:** `SELECT source_code, COUNT(*) FROM dev.fhir_silver.terminology_unmapped_codes GROUP BY source_code`

#### DQ-ORU-002: Out-of-physiologically-plausible-range result values
- **Messages affected:** Indices 10–19 (10 messages)
- **Example values:** HbA1c=45.2%, glucose=1520 mg/dL, creatinine=28.7 mg/dL, WBC=95.0 K/uL
- **Pipeline component:** Bronze ingest (value preserved); Silver QA scorecard
- **Expected outcome:** Value written to Bronze and Silver unchanged; data quality scorecard flags for review
- **Note:** Bronze is immutable — values are NEVER corrected or rejected at ingest

#### DQ-ORU-003: Missing OBX-5 (observation value)
- **Messages affected:** Indices 20–29 (10 messages)
- **OBX-5 content:** empty string
- **Pipeline component:** HL7 parser → lab observation extraction
- **Expected Silver outcome:** `value_quantity=NULL`; `value_string=NULL`; row written with PENDING status
- **QA check:** `SELECT COUNT(*) FROM dev.fhir_silver.lab_observations WHERE value_quantity IS NULL AND value_string IS NULL`

---

### fhir_bundle_batch.json

**File:** `data/synthetic/fhir_bundle_batch.json`
**Total bundles:** 500 (JSON array)
**Generator:** `generate_fhir_bundles()`

#### DQ-FHIR-001: Missing Patient.birthDate
- **Bundles affected:** Indices 0–9 (10 bundles)
- **Field removed:** `Patient.birthDate`
- **Pipeline component:** FHIR ingester → MPI resolution
- **Expected MPI outcome:** Pass 4 (DOB+name+zip) cannot execute; falls to NEW_RECORD if no other pass matches
- **Expected Silver outcome:** `birth_date=NULL` in `mpi_patient_index`
- **QA check:** `SELECT COUNT(*) FROM dev.fhir_silver.mpi_patient_index WHERE birth_date IS NULL`

#### DQ-FHIR-002: Invalid LOINC codes in Observation.code
- **Bundles affected:** Indices 10–19 (10 bundles)
- **Codes used:** INVALID-9999-1 through NOLOINC-000001 (10 codes, all with `http://loinc.org` system)
- **Pipeline component:** Bronze→Silver normalization (SOURCE_LOINC path in `normalize_fhir_observation()`)
- **Expected Silver outcome:** `loinc_mapped=True` (code accepted from source); `loinc_map_method=SOURCE_LOINC`; invalid code written to `loinc_code` column; normalization_log entry created
- **Note:** The SOURCE_LOINC path trusts the source system. Post-ingest LOINC validation is needed to catch invalid codes.

#### DQ-FHIR-003: Missing Bundle.meta.tag (tenant cannot be resolved)
- **Bundles affected:** Indices 20–29 (10 bundles)
- **Bundle.meta.tag value:** empty array `[]`
- **Pipeline component:** FHIR ingester tenant extraction
- **Expected Bronze outcome:** `tenant_id` falls back to `default_tenant_id` parameter; row written with PENDING status
- **QA check:** Confirm tenant fallback behavior in fhir_ingester; rows land in `fhir_resources`

---

### ecw_patients.csv

**File:** `data/synthetic/ecw_patients.csv`
**Total rows:** 300 (280 unique + 5 duplicates at end; 5 blank-name rows)
**Generator:** `generate_ecw_patients()`

#### DQ-CSV-PAT-001: Blank first_name or last_name
- **Rows affected:** Indices 0–4 (5 rows)
- **Field value:** empty string `""`
- **Pipeline component:** CSV ingester → MPI
- **Expected MPI outcome:** Name-based passes (3 and 4) degrade; SSN4 or identifier passes attempted
- **QA check:** Filter ecw_patients for blank name fields; confirm MPI still assigns UMPI

#### DQ-CSV-PAT-002: Invalid ICD-10 codes
- **Rows affected:** Indices 5–9 (5 rows)
- **Codes used:** ZZ999, BADCODE, 99999, X00.000000, 123
- **Pipeline component:** Bronze→Silver Condition normalization
- **Expected Silver outcome:** ICD-10 code written as-is; SNOMED map returns UNMAPPED; entry in `terminology_unmapped_codes`

#### DQ-CSV-PAT-003: DOB in wrong format (MM/DD/YYYY)
- **Rows affected:** Indices 10–14 (5 rows)
- **Format:** `MM/DD/YYYY` instead of `YYYY-MM-DD`
- **Pipeline component:** CSV ingester → date parsing → MPI
- **Expected outcome:** DOB parse failure; `birth_date=NULL`; MPI pass 4 skipped; row still written

#### DQ-CSV-PAT-004: Fully duplicate rows
- **Rows affected:** Rows 295–299 are exact copies of rows 0–4
- **Pipeline component:** Bronze dedup → MPI
- **Expected Bronze outcome:** Both rows written (Bronze is append-only)
- **Expected MPI outcome:** Duplicate resolved to same UMPI via DETERMINISTIC match (identifier system + value)

---

### ecw_labs.csv

**File:** `data/synthetic/ecw_labs.csv`
**Total rows:** 500
**Generator:** `generate_ecw_labs()`

#### DQ-CSV-LAB-001: Local codes with no LOINC mapping
- **Rows affected:** Indices 0–9 (10 rows)
- **Codes used:** ECW-HBA1C-001, ECW-GLUC-002, ECW-CREAT-003, ECW-CHOL-004, ECW-CBC-DIFF, ECW-BMP-007, ECW-TSH-008, ECW-PSA-009, ECW-INR-010, ECW-URIC-011
- **Pipeline component:** Terminology service (`map_loinc()`)
- **Expected Silver outcome:** `loinc_mapped=False`; `loinc_map_method=UNMAPPED`; entry in `terminology_unmapped_codes`
- **Action required:** Add these codes to the LOINC lookup table in `TerminologyService`

#### DQ-CSV-LAB-002: Text result_value when numeric expected
- **Rows affected:** Indices 10–19 (10 rows)
- **Values:** "See note", "Pending confirmation", "Quantity not sufficient", etc.
- **Pipeline component:** CSV ingester → lab observation extraction
- **Expected Silver outcome:** `value_quantity=NULL` (parse fails gracefully); `value_string` populated with text
- **QA check:** `SELECT COUNT(*) FROM dev.fhir_silver.lab_observations WHERE value_quantity IS NULL AND value_string IS NOT NULL`

---

## Pipeline QA Checklist

After running the full pipeline against these batch files, verify:

- [ ] `hl7_messages`: 1000 rows (500 A01 + 500 A03); no rows missing
- [ ] `hl7_messages` duplicate A01s: 10 patients with 2 identical message_control_id values
- [ ] `fhir_resources`: 500+ rows (Patient + Encounter + Observation + optional Condition per bundle)
- [ ] `mpi_patient_index`: patient count ≤ 500 (patients shared across ingestion paths resolve to same UMPI)
- [ ] `mpi_identity_crosswalk`: entries for all resolved patients across all sources
- [ ] `terminology_unmapped_codes`: at minimum 20 rows (10 ORU + 10 CSV lab unmapped codes)
- [ ] `lab_observations`: rows with `loinc_mapped=False` for all unmapped codes
- [ ] `lab_observations`: rows with `value_quantity IS NULL` for text-value rows and missing OBX-5 rows
- [ ] `mpi_patient_index`: rows with `birth_date IS NULL` for the 10 bundles with missing birthDate
- [ ] Data quality scorecard: out-of-range lab values flagged for the 10 ORU DQ-ORU-002 messages
"""


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    Faker.seed(SEED)
    rng = random.Random(SEED)
    fake = Faker("en_US")

    print("Generating 500-patient master pool ...")
    patients = build_patient_pool(fake, rng)
    print(f"  Patient pool built: {len(patients)} patients")
    print(f"  Patient 1: {patients[0].first_name} {patients[0].last_name} "
          f"(DOB={patients[0].dob}, tenant={patients[0].tenant_id})")

    # ── hl7_adt_batch.txt ─────────────────────────────────────────────────────
    print("Generating hl7_adt_batch.txt (1000 ADT messages) ...")
    adt_content = generate_hl7_adt(patients, rng)
    adt_path = OUTPUT_DIR / "hl7_adt_batch.txt"
    adt_path.write_text(adt_content, encoding="utf-8")
    msg_count = adt_content.count("MSH|")
    print(f"  Written: {adt_path}  ({msg_count} messages)")

    # ── hl7_oru_batch.txt ─────────────────────────────────────────────────────
    print("Generating hl7_oru_batch.txt (500 ORU messages) ...")
    oru_content = generate_hl7_oru(patients, rng)
    oru_path = OUTPUT_DIR / "hl7_oru_batch.txt"
    oru_path.write_text(oru_content, encoding="utf-8")
    oru_count = oru_content.count("MSH|")
    print(f"  Written: {oru_path}  ({oru_count} messages)")

    # ── fhir_bundle_batch.json ────────────────────────────────────────────────
    print("Generating fhir_bundle_batch.json (500 FHIR R4 Bundles) ...")
    fhir_content = generate_fhir_bundles(patients, rng)
    fhir_path = OUTPUT_DIR / "fhir_bundle_batch.json"
    fhir_path.write_text(fhir_content, encoding="utf-8")
    bundle_count = len(json.loads(fhir_content))
    print(f"  Written: {fhir_path}  ({bundle_count} bundles)")

    # ── ecw_patients.csv ──────────────────────────────────────────────────────
    print("Generating ecw_patients.csv (300 rows) ...")
    ecw_pat_rows = generate_ecw_patients(patients, rng)
    ecw_pat_path = OUTPUT_DIR / "ecw_patients.csv"
    ecw_pat_fields = [
        "patient_id", "first_name", "last_name", "dob", "gender", "ssn_last4",
        "address", "city", "state", "zip", "phone", "pcp_npi",
        "insurance_id", "last_visit_date", "primary_dx_icd10", "tenant_id",
    ]
    with open(ecw_pat_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ecw_pat_fields)
        writer.writeheader()
        writer.writerows(ecw_pat_rows)
    print(f"  Written: {ecw_pat_path}  ({len(ecw_pat_rows)} rows)")

    # ── ecw_labs.csv ──────────────────────────────────────────────────────────
    print("Generating ecw_labs.csv (500 rows) ...")
    ecw_lab_rows = generate_ecw_labs(patients, rng)
    ecw_lab_path = OUTPUT_DIR / "ecw_labs.csv"
    ecw_lab_fields = [
        "result_id", "patient_id", "collection_date", "test_code", "test_name",
        "result_value", "result_unit", "reference_range_low", "reference_range_high",
        "abnormal_flag", "ordering_provider_npi", "status",
    ]
    with open(ecw_lab_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ecw_lab_fields)
        writer.writeheader()
        writer.writerows(ecw_lab_rows)
    print(f"  Written: {ecw_lab_path}  ({len(ecw_lab_rows)} rows)")

    # ── DATA_QUALITY_GUIDE.md ─────────────────────────────────────────────────
    print("Generating DATA_QUALITY_GUIDE.md ...")
    guide_path = OUTPUT_DIR / "DATA_QUALITY_GUIDE.md"
    guide_path.write_text(DATA_QUALITY_GUIDE, encoding="utf-8")
    print(f"  Written: {guide_path}")

    print()
    print("Done. All files written to:", OUTPUT_DIR)
    print()
    print("Data quality issues injected:")
    print("  hl7_adt_batch.txt     : 50 issues (10+10+10+10+10)")
    print("  hl7_oru_batch.txt     : 30 issues (10+10+10)")
    print("  fhir_bundle_batch.json: 30 issues (10+10+10)")
    print("  ecw_patients.csv      : 20 issues (5+5+5+5)")
    print("  ecw_labs.csv          : 20 issues (10+10)")
    print("  Total                 : 150 injected issues")


if __name__ == "__main__":
    main()
