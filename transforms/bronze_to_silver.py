"""
transforms/bronze_to_silver.py

Bronze → Silver normalization pipeline.

This module transforms raw clinical records from the Bronze layer into
normalized Silver records aligned to the canonical CDM.

Processing order (enforced):
  1. Identity resolution — assign UMPI before touching clinical data
  2. Semantic normalization — map local codes to LOINC, SNOMED-CT, RxNorm
  3. Schema alignment — populate Silver CDM tables
  4. Normalization logging — write to silver.normalization_log

Design principle: Terminology mapping is deterministic lookup, not inference.
Every mapping is traceable to an authoritative source (LOINC release, NLM
RxNorm file, SNOMED-CT release). The normalization_log records every mapping
applied so Silver values can be audited back to Bronze source codes.

This reference implementation uses a static in-memory terminology table.
In production, replace the TerminologyService with a call to a hosted
terminology server (NLM VSAC API, Apelon TDE, or a locally-loaded FHIR
TerminologyServer endpoint).
"""

import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

PIPELINE_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Terminology service (static reference implementation)
# ---------------------------------------------------------------------------

class TerminologyService:
    """
    Maps local/source codes to standardized terminology systems.

    This implementation uses hard-coded lookup tables for the most common
    clinical concepts. In production, replace with calls to:
      - NLM FHIR Terminology Server: https://tx.fhir.org/r4/
      - VSAC API for value set membership
      - Locally-loaded LOINC/RxNorm/SNOMED-CT release files in the warehouse

    The key design constraint: mappings are table lookups, not model inferences.
    If a source code is not in the lookup table, the result is an explicit
    UNMAPPED status — not a guess.
    """

    # LOINC mappings: local display text → (loinc_code, canonical_display)
    # Source: LOINC release 2.76 (https://loinc.org)
    _LOINC_MAP: dict[str, tuple[str, str]] = {
        # HbA1c variants — the canonical multi-system HIE problem
        "hba1c":                        ("4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood"),
        "hgba1c":                       ("4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood"),
        "hemoglobin a1c":               ("4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood"),
        "a1c":                          ("4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood"),
        "glycated hemoglobin":          ("4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood"),
        "glycohemoglobin":              ("4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood"),
        # Glucose
        "glucose":                      ("2345-7", "Glucose [Mass/volume] in Serum or Plasma"),
        "blood glucose":                ("2345-7", "Glucose [Mass/volume] in Serum or Plasma"),
        "fasting glucose":              ("1558-6", "Fasting glucose [Mass/volume] in Serum or Plasma"),
        # Creatinine / Renal
        "creatinine":                   ("2160-0", "Creatinine [Mass/volume] in Serum or Plasma"),
        "serum creatinine":             ("2160-0", "Creatinine [Mass/volume] in Serum or Plasma"),
        "egfr":                         ("62238-1", "Glomerular filtration rate/1.73 sq M.predicted [Volume Rate/Area] in Serum, Plasma or Blood by Creatinine-based formula (CKD-EPI)"),
        # Lipids
        "ldl":                          ("2089-1", "Cholesterol in LDL [Mass/volume] in Serum or Plasma"),
        "ldl cholesterol":              ("2089-1", "Cholesterol in LDL [Mass/volume] in Serum or Plasma"),
        "hdl":                          ("2085-9", "Cholesterol in HDL [Mass/volume] in Serum or Plasma"),
        "total cholesterol":            ("2093-3", "Cholesterol [Mass/volume] in Serum or Plasma"),
        "triglycerides":                ("2571-8", "Triglyceride [Mass/volume] in Serum or Plasma"),
        # CBC
        "wbc":                          ("6690-2", "Leukocytes [#/volume] in Blood by Automated count"),
        "white blood cell count":       ("6690-2", "Leukocytes [#/volume] in Blood by Automated count"),
        "hemoglobin":                   ("718-7",  "Hemoglobin [Mass/volume] in Blood"),
        "hgb":                          ("718-7",  "Hemoglobin [Mass/volume] in Blood"),
        "platelet count":               ("777-3",  "Platelets [#/volume] in Blood by Automated count"),
        # Vitals
        "systolic blood pressure":      ("8480-6", "Systolic blood pressure"),
        "diastolic blood pressure":     ("8462-4", "Diastolic blood pressure"),
        "heart rate":                   ("8867-4", "Heart rate"),
        "bmi":                          ("39156-5","Body mass index (BMI) [Ratio]"),
        "body weight":                  ("29463-7","Body weight"),
        "body height":                  ("8302-2", "Body height"),
    }

    # RxNorm mappings: local drug name (lowercase) → (rxnorm_code, canonical_display)
    # Source: NLM RxNorm (https://www.nlm.nih.gov/research/umls/rxnorm/)
    _RXNORM_MAP: dict[str, tuple[str, str]] = {
        "metformin":            ("860975",  "metformin hydrochloride 500 MG Oral Tablet"),
        "metformin 500mg":      ("860975",  "metformin hydrochloride 500 MG Oral Tablet"),
        "metformin 1000mg":     ("861007",  "metformin hydrochloride 1000 MG Oral Tablet"),
        "lisinopril":           ("314076",  "lisinopril 10 MG Oral Tablet"),
        "lisinopril 10mg":      ("314076",  "lisinopril 10 MG Oral Tablet"),
        "atorvastatin":         ("617310",  "atorvastatin 10 MG Oral Tablet"),
        "atorvastatin 40mg":    ("617314",  "atorvastatin 40 MG Oral Tablet"),
        "amlodipine":           ("197361",  "amlodipine 5 MG Oral Tablet"),
        "aspirin":              ("1191",    "aspirin"),
        "aspirin 81mg":         ("243670",  "aspirin 81 MG Oral Tablet"),
        "insulin glargine":     ("274783",  "insulin glargine"),
        "furosemide":           ("202991",  "furosemide 40 MG Oral Tablet"),
        "omeprazole":           ("7646",    "omeprazole"),
        "levothyroxine":        ("10582",   "levothyroxine"),
    }

    # SNOMED-CT mappings: ICD-10 code → (snomed_code, snomed_display)
    # Source: NLM ICD-10-CM to SNOMED CT Map
    _ICD10_TO_SNOMED: dict[str, tuple[str, str]] = {
        "I21.9":  ("57054005",  "Acute myocardial infarction"),
        "I10":    ("38341003",  "Hypertensive disorder, systemic arterial"),
        "E11.9":  ("44054006",  "Diabetes mellitus type 2"),
        "E11.65": ("44054006",  "Diabetes mellitus type 2"),
        "N18.3":  ("700379002", "Chronic kidney disease stage 3"),
        "N18.4":  ("700378005", "Chronic kidney disease stage 4"),
        "J44.1":  ("195951007", "Acute exacerbation of chronic obstructive airways disease"),
        "I50.9":  ("84114007",  "Heart failure"),
        "F32.9":  ("35489007",  "Depressive disorder"),
        "Z87.891":("160303001", "Family history of tobacco use"),
    }

    def map_loinc(self, source_display: str) -> Optional[tuple[str, str]]:
        """
        Map a local lab display string to LOINC.
        Returns (loinc_code, loinc_display) or None if unmapped.
        """
        if not source_display:
            return None
        normalized = source_display.strip().lower()
        return self._LOINC_MAP.get(normalized)

    def map_rxnorm(self, source_drug_name: str) -> Optional[tuple[str, str]]:
        """Map a local drug name to RxNorm."""
        if not source_drug_name:
            return None
        normalized = source_drug_name.strip().lower()
        return self._RXNORM_MAP.get(normalized)

    def map_snomed_from_icd10(self, icd10_code: str) -> Optional[tuple[str, str]]:
        """Map an ICD-10-CM code to SNOMED-CT."""
        if not icd10_code:
            return None
        return self._ICD10_TO_SNOMED.get(icd10_code.strip())


# ---------------------------------------------------------------------------
# FHIR Observation → Silver lab record
# ---------------------------------------------------------------------------

@dataclass
class SilverLabRecord:
    observation_id: str
    tenant_id: str
    umpi: str
    encounter_id: Optional[str]
    loinc_code: Optional[str]
    loinc_display: Optional[str]
    source_code: Optional[str]
    source_code_system: Optional[str]
    source_display: Optional[str]
    observation_status: Optional[str]
    value_quantity: Optional[float]
    value_unit: Optional[str]
    value_string: Optional[str]
    interpretation_code: Optional[str]
    interpretation_display: Optional[str]
    reference_range_low: Optional[float]
    reference_range_high: Optional[float]
    reference_range_unit: Optional[str]
    effective_datetime: Optional[str]
    issued_datetime: Optional[str]
    loinc_mapped: bool = False
    loinc_map_method: str = "UNMAPPED"
    source_table: str = "bronze.fhir_resources"
    source_id: Optional[str] = None
    created_ts: str = ""


def normalize_fhir_observation(
    resource: dict,
    tenant_id: str,
    umpi: str,
    source_id: str,
    terminology: TerminologyService,
    encounter_silver_id: Optional[str] = None,
) -> tuple[SilverLabRecord, list[dict]]:
    """
    Normalize a FHIR R4 Observation resource to a SilverLabRecord.

    Returns:
        (SilverLabRecord, normalization_log_entries)

    The normalization_log_entries list contains dicts ready for insert
    into silver.normalization_log.
    """
    norm_log = []
    obs_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    # Extract coding
    codings = resource.get("code", {}).get("coding", [])
    source_code = None
    source_code_system = None
    source_display = resource.get("code", {}).get("text")   # free-text display from source

    loinc_code = None
    loinc_display = None
    loinc_mapped = False
    loinc_map_method = "UNMAPPED"

    for coding in codings:
        system = coding.get("system", "")
        code = coding.get("code")
        display = coding.get("display")

        if "loinc.org" in system and code:
            # Source already sent a LOINC code — accept it directly
            loinc_code = code
            loinc_display = display
            loinc_mapped = True
            loinc_map_method = "SOURCE_LOINC"
            norm_log.append(_norm_log_entry(
                tenant_id=tenant_id,
                source_table="bronze.fhir_resources",
                source_id=source_id,
                target_table="silver.lab_observations",
                target_id=obs_id,
                mapping_type="LOINC_MAP",
                source_value=code,
                source_system=system,
                mapped_value=code,
                mapped_system="http://loinc.org",
                confidence=1.0,
                method="SOURCE_LOINC",
            ))
        else:
            source_code = code
            source_code_system = system
            if not source_display:
                source_display = display

    # If no LOINC from source, attempt local terminology lookup
    if not loinc_mapped:
        mapped = terminology.map_loinc(source_display)
        if mapped:
            loinc_code, loinc_display = mapped
            loinc_mapped = True
            loinc_map_method = "TERMINOLOGY_SERVICE"
            norm_log.append(_norm_log_entry(
                tenant_id=tenant_id,
                source_table="bronze.fhir_resources",
                source_id=source_id,
                target_table="silver.lab_observations",
                target_id=obs_id,
                mapping_type="LOINC_MAP",
                source_value=source_display,
                source_system=source_code_system or "LOCAL",
                mapped_value=loinc_code,
                mapped_system="http://loinc.org",
                confidence=1.0,
                method="TERMINOLOGY_SERVICE",
            ))
        else:
            logger.warning(
                "LOINC unmapped: source_display='%s' source_code='%s' tenant=%s",
                source_display, source_code, tenant_id,
            )
            norm_log.append(_norm_log_entry(
                tenant_id=tenant_id,
                source_table="bronze.fhir_resources",
                source_id=source_id,
                target_table="silver.lab_observations",
                target_id=obs_id,
                mapping_type="LOINC_MAP",
                source_value=source_display,
                source_system=source_code_system or "LOCAL",
                mapped_value=None,
                mapped_system=None,
                confidence=0.0,
                method="UNMAPPED",
            ))

    # Extract value
    value_quantity = None
    value_unit = None
    value_string = None
    value_qty = resource.get("valueQuantity", {})
    if value_qty:
        value_quantity = value_qty.get("value")
        value_unit = value_qty.get("code") or value_qty.get("unit")
    elif resource.get("valueString"):
        value_string = resource.get("valueString")

    # Reference range
    ref_ranges = resource.get("referenceRange", [])
    ref_low = ref_high = ref_unit = None
    if ref_ranges:
        rr = ref_ranges[0]
        ref_low = rr.get("low", {}).get("value")
        ref_high = rr.get("high", {}).get("value")
        ref_unit = rr.get("low", {}).get("unit") or rr.get("high", {}).get("unit")

    # Interpretation
    interp_code = interp_display = None
    interpretations = resource.get("interpretation", [])
    if interpretations:
        interp_codings = interpretations[0].get("coding", [])
        if interp_codings:
            interp_code = interp_codings[0].get("code")
            interp_display = interp_codings[0].get("display")

    record = SilverLabRecord(
        observation_id=obs_id,
        tenant_id=tenant_id,
        umpi=umpi,
        encounter_id=encounter_silver_id,
        loinc_code=loinc_code,
        loinc_display=loinc_display,
        source_code=source_code,
        source_code_system=source_code_system,
        source_display=source_display,
        observation_status=resource.get("status"),
        value_quantity=float(value_quantity) if value_quantity is not None else None,
        value_unit=value_unit,
        value_string=value_string,
        interpretation_code=interp_code,
        interpretation_display=interp_display,
        reference_range_low=float(ref_low) if ref_low is not None else None,
        reference_range_high=float(ref_high) if ref_high is not None else None,
        reference_range_unit=ref_unit,
        effective_datetime=resource.get("effectiveDateTime"),
        issued_datetime=resource.get("issued"),
        loinc_mapped=loinc_mapped,
        loinc_map_method=loinc_map_method,
        source_table="bronze.fhir_resources",
        source_id=source_id,
        created_ts=now,
    )

    return record, norm_log


def _norm_log_entry(
    tenant_id: str,
    source_table: str,
    source_id: str,
    target_table: str,
    target_id: str,
    mapping_type: str,
    source_value: Optional[str],
    source_system: Optional[str],
    mapped_value: Optional[str],
    mapped_system: Optional[str],
    confidence: float,
    method: str,
) -> dict:
    return {
        "log_id": str(uuid.uuid4()),
        "tenant_id": tenant_id,
        "source_table": source_table,
        "source_id": source_id,
        "target_table": target_table,
        "target_id": target_id,
        "mapping_type": mapping_type,
        "source_value": source_value,
        "source_system": source_system,
        "mapped_value": mapped_value,
        "mapped_system": mapped_system,
        "mapping_confidence": confidence,
        "mapping_method": method,
        "pipeline_version": PIPELINE_VERSION,
        "processed_ts": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    sample_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "synthetic", "fhir_bundle_sample.json"
    )

    with open(sample_path) as f:
        bundle = json.load(f)

    terminology = TerminologyService()
    synthetic_umpi = str(uuid.uuid4())

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") != "Observation":
            continue

        record, norm_log = normalize_fhir_observation(
            resource=resource,
            tenant_id="INTEGRIS_BAPTIST",
            umpi=synthetic_umpi,
            source_id="demo-bronze-id-001",
            terminology=terminology,
        )

        print(f"\nSilver lab record:")
        print(f"  loinc_code:       {record.loinc_code}")
        print(f"  loinc_display:    {record.loinc_display}")
        print(f"  source_display:   {record.source_display}")
        print(f"  loinc_mapped:     {record.loinc_mapped}")
        print(f"  loinc_map_method: {record.loinc_map_method}")
        print(f"  value:            {record.value_quantity} {record.value_unit}")
        print(f"  interpretation:   {record.interpretation_code}")
        print(f"  norm_log_entries: {len(norm_log)}")
