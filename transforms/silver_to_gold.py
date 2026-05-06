"""
transforms/silver_to_gold.py

Silver → Gold analytics layer construction.

This module builds Gold layer records from normalized Silver CDM data.
Three primary outputs:

  1. gold.patient_summary — population health patient profile with risk
     stratification, chronic condition flags, and utilization metrics.

  2. gold.quality_measures — HEDIS/CMS measure calculation for individual
     patients (CDC Diabetes measures implemented as reference examples).

  3. gold.adt_event_feed — denormalized ADT event stream for care
     coordination, including 30-day readmission flagging.

Design principle: Gold is always derived from Silver. This module reads
Silver CDM entities (provided as in-memory structures in this reference
implementation) and produces Gold records. In production, these functions
are called by scheduled Databricks or Snowflake tasks that read from
Silver tables and write incrementally to Gold tables.

Row-level security is enforced at the Gold table level via platform RLS
policies (see schema/gold.sql and docs/tenant-isolation.md). This module
does not implement access control — it produces records that RLS will filter
at query time.
"""

import hashlib
import logging
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

PIPELINE_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# ICD-10 chronic condition code sets
# ---------------------------------------------------------------------------
# These value sets define the ICD-10-CM codes used to flag chronic conditions
# on the patient summary. In production, these are maintained in a value set
# management system (VSAC) and versioned alongside HEDIS measure specifications.

DIABETES_ICD10 = {
    "E10", "E10.9", "E11", "E11.9", "E11.65", "E11.40", "E11.41",
    "E13", "E13.9", "E08", "E08.9",
}

HYPERTENSION_ICD10 = {
    "I10", "I11", "I11.0", "I11.9", "I12", "I12.9", "I13", "I13.0",
    "I13.10", "I13.11", "I13.2",
}

HEART_FAILURE_ICD10 = {
    "I50", "I50.1", "I50.20", "I50.21", "I50.22", "I50.23",
    "I50.30", "I50.31", "I50.32", "I50.33", "I50.40", "I50.41",
    "I50.42", "I50.43", "I50.9",
}

CKD_ICD10 = {
    "N18", "N18.1", "N18.2", "N18.3", "N18.4", "N18.5", "N18.6", "N18.9",
}

COPD_ICD10 = {
    "J44", "J44.0", "J44.1", "J44.9", "J43", "J43.0", "J43.1",
    "J43.2", "J43.9",
}

DEPRESSION_ICD10 = {
    "F32", "F32.0", "F32.1", "F32.2", "F32.3", "F32.4", "F32.5",
    "F32.9", "F33", "F33.0", "F33.1", "F33.2", "F33.3", "F33.9",
}

# ADT event type display labels
ADT_EVENT_LABELS = {
    "A01": "Admission",
    "A02": "Transfer",
    "A03": "Discharge",
    "A04": "Registration",
    "A08": "Update",
    "A11": "Cancel Admission",
    "A13": "Cancel Discharge",
    "A28": "Add Person Information",
    "A31": "Update Person Information",
}


# ---------------------------------------------------------------------------
# Input data classes (Silver CDM representations)
# ---------------------------------------------------------------------------

@dataclass
class SilverPatient:
    umpi: str
    tenant_id: str
    family_name: Optional[str]
    given_name: Optional[str]
    birth_date: Optional[date]
    gender: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]


@dataclass
class SilverEncounter:
    encounter_id: str
    tenant_id: str
    umpi: str
    encounter_class: Optional[str]
    period_start: Optional[datetime]
    period_end: Optional[datetime]
    facility_name: Optional[str]
    facility_npi: Optional[str]
    attending_provider_npi: Optional[str]
    attending_provider_name: Optional[str]
    encounter_status: Optional[str]
    admit_source_code: Optional[str]
    discharge_disposition_code: Optional[str]


@dataclass
class SilverDiagnosis:
    diagnosis_id: str
    tenant_id: str
    umpi: str
    encounter_id: Optional[str]
    icd10_code: Optional[str]
    icd10_display: Optional[str]
    diagnosis_rank: Optional[int]
    clinical_status: Optional[str]
    onset_datetime: Optional[datetime]


@dataclass
class SilverLabObservation:
    observation_id: str
    tenant_id: str
    umpi: str
    encounter_id: Optional[str]
    loinc_code: Optional[str]
    loinc_display: Optional[str]
    value_quantity: Optional[float]
    value_unit: Optional[str]
    interpretation_code: Optional[str]
    effective_datetime: Optional[datetime]
    observation_status: Optional[str]


@dataclass
class SilverADTEvent:
    adt_event_id: str
    tenant_id: str
    umpi: str
    encounter_id: Optional[str]
    event_type: str
    event_datetime: datetime
    facility_name: Optional[str]
    facility_npi: Optional[str]
    current_location: Optional[str]


# ---------------------------------------------------------------------------
# Output data classes (Gold layer records)
# ---------------------------------------------------------------------------

@dataclass
class GoldPatientSummary:
    tenant_id: str
    patient_key: str                    # hashed UMPI — not raw UMPI
    full_name: Optional[str]
    birth_date: Optional[date]
    age_years: Optional[int]
    gender: Optional[str]
    state: Optional[str]
    postal_code: Optional[str]
    attributed_pcp_npi: Optional[str]
    attributed_pcp_name: Optional[str]
    attribution_method: Optional[str]
    charlson_index: int
    elixhauser_index: int
    risk_tier: str
    risk_score_updated_ts: str
    total_encounters_12m: int
    total_ed_visits_12m: int
    total_inpatient_days_12m: int
    last_encounter_date: Optional[date]
    flag_diabetes: bool
    flag_hypertension: bool
    flag_heart_failure: bool
    flag_ckd: bool
    flag_copd: bool
    flag_depression: bool
    as_of_date: date
    refreshed_ts: str


@dataclass
class GoldQualityMeasure:
    measure_id: str
    tenant_id: str
    patient_key: str
    measure_code: str
    measure_name: str
    measure_steward: str
    measurement_year: int
    numerator: Optional[bool]
    denominator: bool
    exclusion: bool
    exclusion_reason: Optional[str]
    evidence_encounter_id: Optional[str]
    evidence_observation_id: Optional[str]
    evidence_date: Optional[date]
    evidence_value: Optional[str]
    calculated_ts: str
    as_of_date: date


@dataclass
class GoldADTEventFeed:
    event_id: str
    tenant_id: str
    patient_key: str
    event_type: str
    event_type_display: str
    event_datetime: datetime
    facility_name: Optional[str]
    current_location: Optional[str]
    primary_diagnosis_icd10: Optional[str]
    primary_diagnosis_display: Optional[str]
    attributed_pcp_npi: Optional[str]
    attributed_pcp_name: Optional[str]
    is_readmission_30d: bool
    prior_discharge_date: Optional[date]
    loaded_ts: str


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def hash_umpi(umpi: str) -> str:
    """
    Produce a stable, non-reversible patient_key from a UMPI.
    Gold layer consumers receive the patient_key, not the raw UMPI,
    to provide an additional layer of de-identification for BI tools
    that may cache query results.

    SHA-256 is used for stability — the same UMPI always produces
    the same patient_key, enabling joins across Gold tables without
    exposing the raw identifier.
    """
    return hashlib.sha256(umpi.encode()).hexdigest()


def calculate_age(birth_date: date, as_of: date) -> int:
    years = as_of.year - birth_date.year
    if (as_of.month, as_of.day) < (birth_date.month, birth_date.day):
        years -= 1
    return years


def assign_risk_tier(charlson_index: int) -> str:
    """
    Map Charlson Comorbidity Index to a risk tier.
    Thresholds based on published mortality risk stratification literature.
    """
    if charlson_index == 0:
        return "LOW"
    elif charlson_index <= 2:
        return "MODERATE"
    elif charlson_index <= 4:
        return "HIGH"
    else:
        return "VERY_HIGH"


def calculate_charlson_index(diagnoses: list[SilverDiagnosis]) -> int:
    """
    Calculate Charlson Comorbidity Index from ICD-10 diagnoses.

    Implements the Quan et al. (2005) ICD-10 coding algorithm for the
    Charlson Comorbidity Index. Each condition carries a weight of 1, 2,
    or 3 points. Score = sum of weights for all conditions present.

    Reference: Quan H, et al. Med Care. 2005;43(11):1130-1139.
    """
    icd10_codes = {d.icd10_code for d in diagnoses if d.icd10_code}

    def has_any(prefixes: set) -> bool:
        return any(
            any(code.startswith(p) for p in prefixes)
            for code in icd10_codes
        )

    score = 0

    # Weight 1 conditions
    if has_any({"I21", "I22", "I25.2"}):                     score += 1  # MI
    if has_any({"I50"}):                                      score += 1  # CHF
    if has_any({"I70", "I71", "I73.9", "I77.1", "I79"}):     score += 1  # PVD
    if has_any({"I60", "I61", "I62", "I63", "I64", "G45", "G46"}): score += 1  # CVD
    if has_any({"F00", "F01", "F02", "F03", "G30"}):         score += 1  # Dementia
    if has_any({"J40", "J41", "J42", "J43", "J44", "J45", "J46", "J47"}): score += 1  # COPD
    if has_any({"M05", "M06", "M32", "M33", "M34", "M35"}):  score += 1  # Rheumatic
    if has_any({"K25", "K26", "K27", "K28"}):                 score += 1  # PUD
    if has_any({"B18", "K70", "K71", "K73", "K74"}):          score += 1  # Liver (mild)
    if has_any({"E10", "E11", "E12", "E13", "E14"}) and not has_any({"E10.2", "E11.2", "E12.2", "E13.2", "E14.2"}):
        score += 1  # DM without complications

    # Weight 2 conditions
    if has_any({"E10.2", "E11.2", "E12.2", "E13.2", "E14.2"}): score += 2  # DM with complications
    if has_any({"G81", "G82", "G83"}):                        score += 2  # Hemiplegia
    if has_any({"N18", "N19", "Z49", "Z94.0", "Z99.2"}):      score += 2  # Renal disease
    if has_any({"C0", "C1", "C2", "C3", "C40", "C41", "C43", "C45", "C46",
                "C47", "C48", "C49", "C5", "C6", "C70", "C71", "C72",
                "C73", "C74", "C75", "C76", "C81", "C82", "C83", "C84",
                "C85", "C88", "C90", "C91", "C92", "C93", "C94", "C95",
                "C96", "C97"}):                                score += 2  # Malignancy
    if has_any({"K72.1", "K72.9", "K76.5", "K76.6", "K76.7"}): score += 3  # Liver (mod-severe)

    # Weight 6 conditions
    if has_any({"C77", "C78", "C79", "C80"}):                 score += 6  # Metastatic tumor
    if has_any({"B20", "B21", "B22", "B23", "B24"}):          score += 6  # HIV/AIDS

    return score


# ---------------------------------------------------------------------------
# Patient Summary builder
# ---------------------------------------------------------------------------

def build_patient_summary(
    patient: SilverPatient,
    encounters: list[SilverEncounter],
    diagnoses: list[SilverDiagnosis],
    as_of_date: Optional[date] = None,
) -> GoldPatientSummary:
    """
    Build a gold.patient_summary record from Silver CDM entities.

    Args:
        patient: The Silver patient record (from master_patient_index).
        encounters: All encounters for this patient from silver.encounters.
        diagnoses: All diagnoses for this patient from silver.diagnoses.
        as_of_date: Snapshot date for the summary. Defaults to today.

    Returns:
        GoldPatientSummary ready for upsert into gold.patient_summary.
    """
    now = datetime.now(timezone.utc)
    as_of = as_of_date or now.date()
    patient_key = hash_umpi(patient.umpi)

    # Age
    age = calculate_age(patient.birth_date, as_of) if patient.birth_date else None

    # 12-month utilization window
    window_start = datetime.combine(as_of - timedelta(days=365), datetime.min.time()).replace(tzinfo=timezone.utc)

    recent_encounters = [
        e for e in encounters
        if e.period_start and e.period_start >= window_start
    ]

    total_encounters_12m = len(recent_encounters)

    ed_encounters = [
        e for e in recent_encounters
        if e.encounter_class in ("EMER", "E", "EM")
    ]
    total_ed_visits_12m = len(ed_encounters)

    inpatient_encounters = [
        e for e in recent_encounters
        if e.encounter_class in ("IMP", "I", "ACUTE")
    ]
    total_inpatient_days_12m = sum(
        (e.period_end - e.period_start).days
        for e in inpatient_encounters
        if e.period_start and e.period_end
    )

    last_encounter_date = None
    if encounters:
        latest = max(
            (e.period_start for e in encounters if e.period_start),
            default=None
        )
        last_encounter_date = latest.date() if latest else None

    # Chronic condition flags
    icd10_codes = {d.icd10_code for d in diagnoses if d.icd10_code}

    def has_condition(code_set: set) -> bool:
        return any(
            any(code.startswith(prefix) for prefix in code_set)
            for code in icd10_codes
        )

    flag_diabetes = has_condition(DIABETES_ICD10)
    flag_hypertension = has_condition(HYPERTENSION_ICD10)
    flag_heart_failure = has_condition(HEART_FAILURE_ICD10)
    flag_ckd = has_condition(CKD_ICD10)
    flag_copd = has_condition(COPD_ICD10)
    flag_depression = has_condition(DEPRESSION_ICD10)

    # Risk stratification via Charlson Index
    charlson = calculate_charlson_index(diagnoses)
    risk_tier = assign_risk_tier(charlson)

    # Provider attribution: most frequent attending provider in last 12 months
    # Simple frequency-based attribution (encounter count method)
    provider_counts: dict[tuple, int] = {}
    for e in recent_encounters:
        if e.attending_provider_npi and e.attending_provider_name:
            key = (e.attending_provider_npi, e.attending_provider_name)
            provider_counts[key] = provider_counts.get(key, 0) + 1

    attributed_pcp_npi = None
    attributed_pcp_name = None
    attribution_method = None
    if provider_counts:
        top_provider = max(provider_counts, key=lambda k: provider_counts[k])
        attributed_pcp_npi, attributed_pcp_name = top_provider
        attribution_method = "ENCOUNTER_FREQUENCY"

    return GoldPatientSummary(
        tenant_id=patient.tenant_id,
        patient_key=patient_key,
        full_name=f"{patient.given_name or ''} {patient.family_name or ''}".strip() or None,
        birth_date=patient.birth_date,
        age_years=age,
        gender=patient.gender,
        state=patient.state,
        postal_code=patient.postal_code,
        attributed_pcp_npi=attributed_pcp_npi,
        attributed_pcp_name=attributed_pcp_name,
        attribution_method=attribution_method,
        charlson_index=charlson,
        elixhauser_index=0,        # Elixhauser implementation is an extension point
        risk_tier=risk_tier,
        risk_score_updated_ts=now.isoformat(),
        total_encounters_12m=total_encounters_12m,
        total_ed_visits_12m=total_ed_visits_12m,
        total_inpatient_days_12m=total_inpatient_days_12m,
        last_encounter_date=last_encounter_date,
        flag_diabetes=flag_diabetes,
        flag_hypertension=flag_hypertension,
        flag_heart_failure=flag_heart_failure,
        flag_ckd=flag_ckd,
        flag_copd=flag_copd,
        flag_depression=flag_depression,
        as_of_date=as_of,
        refreshed_ts=now.isoformat(),
    )


# ---------------------------------------------------------------------------
# Quality measure calculator
# ---------------------------------------------------------------------------

def calculate_cdc_hba1c_control(
    patient: SilverPatient,
    diagnoses: list[SilverDiagnosis],
    observations: list[SilverLabObservation],
    measurement_year: int,
) -> Optional[GoldQualityMeasure]:
    """
    HEDIS CDC (Comprehensive Diabetes Care): HbA1c Control (<8.0%).

    Denominator: Patients 18-75 with diabetes diagnosis in measurement year.
    Numerator: Most recent HbA1c in measurement year < 8.0%.
    Exclusion: Patients with gestational diabetes only (not implemented here).

    Reference: NCQA HEDIS 2024 Technical Specifications, CDC measure.
    """
    now = datetime.now(timezone.utc)
    patient_key = hash_umpi(patient.umpi)
    measure_year_start = datetime(measurement_year, 1, 1)
    measure_year_end = datetime(measurement_year, 12, 31, 23, 59, 59)

    # Denominator: age 18-75 + diabetes diagnosis
    age = calculate_age(patient.birth_date, date(measurement_year, 12, 31)) if patient.birth_date else None
    if age is None or not (18 <= age <= 75):
        return None  # Outside eligible age range — not in denominator

    diabetes_dx = [
        d for d in diagnoses
        if d.icd10_code and any(d.icd10_code.startswith(p) for p in DIABETES_ICD10)
    ]
    if not diabetes_dx:
        return None  # No diabetes diagnosis — not in denominator

    # Numerator: most recent HbA1c in measurement year
    HGBA1C_LOINC = "4548-4"
    hba1c_obs = [
        o for o in observations
        if o.loinc_code == HGBA1C_LOINC
        and o.effective_datetime
        and measure_year_start <= o.effective_datetime <= measure_year_end
        and o.observation_status == "final"
        and o.value_quantity is not None
    ]

    evidence_obs_id = None
    evidence_date = None
    evidence_value = None
    numerator = None

    if hba1c_obs:
        most_recent = max(hba1c_obs, key=lambda o: o.effective_datetime or datetime.min)
        val = most_recent.value_quantity
        if val is not None:
            evidence_obs_id = most_recent.observation_id
            evidence_date = most_recent.effective_datetime.date() if most_recent.effective_datetime else None
            evidence_value = f"{val:.1f}%"
            numerator = val < 8.0
    # If no HbA1c in measurement year: numerator=None (not met, no evidence)

    return GoldQualityMeasure(
        measure_id=str(uuid.uuid4()),
        tenant_id=patient.tenant_id,
        patient_key=patient_key,
        measure_code="CDC_HBA1C_CONTROL_LESS_8",
        measure_name="Comprehensive Diabetes Care: HbA1c Control (<8.0%)",
        measure_steward="NCQA",
        measurement_year=measurement_year,
        numerator=numerator,
        denominator=True,
        exclusion=False,
        exclusion_reason=None,
        evidence_encounter_id=None,
        evidence_observation_id=evidence_obs_id,
        evidence_date=evidence_date,
        evidence_value=evidence_value,
        calculated_ts=now.isoformat(),
        as_of_date=date.today(),
    )


# ---------------------------------------------------------------------------
# ADT event feed builder
# ---------------------------------------------------------------------------

def build_adt_event_feed(
    patient: SilverPatient,
    adt_events: list[SilverADTEvent],
    diagnoses: list[SilverDiagnosis],
    attributed_pcp_npi: Optional[str] = None,
    attributed_pcp_name: Optional[str] = None,
) -> list[GoldADTEventFeed]:
    """
    Build gold.adt_event_feed records from Silver ADT events.
    Includes 30-day readmission flagging.

    Readmission logic: An admission event (A01) is flagged as a readmission
    if there was a prior discharge event (A03) for the same patient within
    the preceding 30 days.
    """
    now = datetime.now(timezone.utc)
    patient_key = hash_umpi(patient.umpi)

    # Build encounter → primary diagnosis lookup
    encounter_primary_dx: dict[str, SilverDiagnosis] = {}
    for dx in diagnoses:
        if dx.encounter_id and dx.diagnosis_rank == 1 and dx.clinical_status == "active":
            encounter_primary_dx[dx.encounter_id] = dx

    # Sort events by datetime for readmission window calculation
    sorted_events = sorted(adt_events, key=lambda e: e.event_datetime)

    # Track discharge dates for readmission detection
    prior_discharges: list[datetime] = []
    gold_records = []

    for event in sorted_events:
        # Check for 30-day readmission
        is_readmission = False
        prior_discharge_date = None

        if event.event_type == "A01":  # Admission
            for discharge_ts in reversed(prior_discharges):
                delta = event.event_datetime - discharge_ts
                if 0 < delta.total_seconds() <= 30 * 86400:
                    is_readmission = True
                    prior_discharge_date = discharge_ts.date()
                    break

        if event.event_type == "A03":  # Discharge
            prior_discharges.append(event.event_datetime)

        # Primary diagnosis for this encounter
        primary_dx = encounter_primary_dx.get(event.encounter_id) if event.encounter_id else None

        gold_records.append(GoldADTEventFeed(
            event_id=str(uuid.uuid4()),
            tenant_id=event.tenant_id,
            patient_key=patient_key,
            event_type=event.event_type,
            event_type_display=ADT_EVENT_LABELS.get(event.event_type, event.event_type),
            event_datetime=event.event_datetime,
            facility_name=event.facility_name,
            current_location=event.current_location,
            primary_diagnosis_icd10=primary_dx.icd10_code if primary_dx else None,
            primary_diagnosis_display=primary_dx.icd10_display if primary_dx else None,
            attributed_pcp_npi=attributed_pcp_npi,
            attributed_pcp_name=attributed_pcp_name,
            is_readmission_30d=is_readmission,
            prior_discharge_date=prior_discharge_date,
            loaded_ts=now.isoformat(),
        ))

    return gold_records


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import os

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # Build a synthetic Silver patient record matching the synthetic FHIR bundle
    patient = SilverPatient(
        umpi=str(uuid.uuid4()),
        tenant_id="INTEGRIS_BAPTIST",
        family_name="Ramirez",
        given_name="Carlos",
        birth_date=date(1976, 12, 4),
        gender="male",
        state="OK",
        postal_code="73102",
    )

    now = datetime.now(timezone.utc)

    encounters = [
        SilverEncounter(
            encounter_id=str(uuid.uuid4()),
            tenant_id="INTEGRIS_BAPTIST",
            umpi=patient.umpi,
            encounter_class="IMP",
            period_start=datetime(2024, 3, 15, 8, 20),
            period_end=datetime(2024, 3, 18, 14, 0),
            facility_name="INTEGRIS Baptist Medical Center",
            facility_npi="NPI-0987654321",
            attending_provider_npi="NPI-0987654321",
            attending_provider_name="Dr. Anita Patel",
            encounter_status="finished",
            admit_source_code="emd",
            discharge_disposition_code="01",
        )
    ]

    diagnoses = [
        SilverDiagnosis(
            diagnosis_id=str(uuid.uuid4()),
            tenant_id="INTEGRIS_BAPTIST",
            umpi=patient.umpi,
            encounter_id=encounters[0].encounter_id,
            icd10_code="I21.9",
            icd10_display="Acute myocardial infarction, unspecified",
            diagnosis_rank=1,
            clinical_status="active",
            onset_datetime=datetime(2024, 3, 15, 8, 0),
        ),
        SilverDiagnosis(
            diagnosis_id=str(uuid.uuid4()),
            tenant_id="INTEGRIS_BAPTIST",
            umpi=patient.umpi,
            encounter_id=encounters[0].encounter_id,
            icd10_code="E11.9",
            icd10_display="Type 2 diabetes mellitus without complications",
            diagnosis_rank=2,
            clinical_status="active",
            onset_datetime=None,
        ),
        SilverDiagnosis(
            diagnosis_id=str(uuid.uuid4()),
            tenant_id="INTEGRIS_BAPTIST",
            umpi=patient.umpi,
            encounter_id=encounters[0].encounter_id,
            icd10_code="I10",
            icd10_display="Essential (primary) hypertension",
            diagnosis_rank=3,
            clinical_status="active",
            onset_datetime=None,
        ),
    ]

    observations = [
        SilverLabObservation(
            observation_id=str(uuid.uuid4()),
            tenant_id="INTEGRIS_BAPTIST",
            umpi=patient.umpi,
            encounter_id=encounters[0].encounter_id,
            loinc_code="4548-4",
            loinc_display="Hemoglobin A1c/Hemoglobin.total in Blood",
            value_quantity=8.2,
            value_unit="%",
            interpretation_code="H",
            effective_datetime=datetime(2024, 3, 15, 14, 30),
            observation_status="final",
        )
    ]

    adt_events = [
        SilverADTEvent(
            adt_event_id=str(uuid.uuid4()),
            tenant_id="INTEGRIS_BAPTIST",
            umpi=patient.umpi,
            encounter_id=encounters[0].encounter_id,
            event_type="A01",
            event_datetime=datetime(2024, 3, 15, 8, 20),
            facility_name="INTEGRIS Baptist Medical Center",
            facility_npi="NPI-0987654321",
            current_location="3 NORTH^312^A",
        ),
        SilverADTEvent(
            adt_event_id=str(uuid.uuid4()),
            tenant_id="INTEGRIS_BAPTIST",
            umpi=patient.umpi,
            encounter_id=encounters[0].encounter_id,
            event_type="A03",
            event_datetime=datetime(2024, 3, 18, 14, 0),
            facility_name="INTEGRIS Baptist Medical Center",
            facility_npi="NPI-0987654321",
            current_location=None,
        ),
    ]

    # Build Gold records
    summary = build_patient_summary(patient, encounters, diagnoses)
    print("\n=== Patient Summary ===")
    print(f"  patient_key:        {summary.patient_key[:16]}...")
    print(f"  full_name:          {summary.full_name}")
    print(f"  age:                {summary.age_years}")
    print(f"  charlson_index:     {summary.charlson_index}")
    print(f"  risk_tier:          {summary.risk_tier}")
    print(f"  flag_diabetes:      {summary.flag_diabetes}")
    print(f"  flag_hypertension:  {summary.flag_hypertension}")
    print(f"  flag_heart_failure: {summary.flag_heart_failure}")
    print(f"  encounters_12m:     {summary.total_encounters_12m}")
    print(f"  attributed_pcp:     {summary.attributed_pcp_name}")

    measure = calculate_cdc_hba1c_control(patient, diagnoses, observations, 2024)
    print("\n=== Quality Measure: CDC HbA1c Control ===")
    if measure:
        print(f"  denominator:        {measure.denominator}")
        print(f"  numerator:          {measure.numerator}")
        print(f"  evidence_value:     {measure.evidence_value}")
        print(f"  evidence_date:      {measure.evidence_date}")
        print(f"  (HbA1c 8.2% → not in control → numerator=False)")
    else:
        print("  Patient not in denominator.")

    adt_feed = build_adt_event_feed(patient, adt_events, diagnoses)
    print(f"\n=== ADT Event Feed ({len(adt_feed)} events) ===")
    for ev in adt_feed:
        print(f"  {ev.event_type} ({ev.event_type_display:<12}) {ev.event_datetime.strftime('%Y-%m-%d %H:%M')}  readmission={ev.is_readmission_30d}")
