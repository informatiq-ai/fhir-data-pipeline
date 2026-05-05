"""
transforms/identity_resolution.py

Deterministic Master Patient Index (MPI) resolution for the Silver layer.

This module assigns a Universal Master Patient Index (UMPI) to every patient
record before clinical normalization begins. Identity resolution is the first
step in Bronze → Silver processing because every Silver entity (encounter,
diagnosis, lab) is keyed to a UMPI, not a local MRN.

Why identity resolution comes first:
  A lab result from Hospital A and a diagnosis from Clinic B are only
  meaningful together if you know they belong to the same patient. Running
  clinical normalization before resolving identity produces a Silver layer
  where the same patient appears as multiple records — one per tenant MRN.
  That defeats the purpose of a state-scale HIE.

Matching strategy (deterministic only in this reference implementation):
  1. Exact SSN-4 + DOB + last name
  2. Exact MRN + facility NPI
  3. Exact DOB + last name + first name + zip
  4. No match → create new UMPI

Production note:
  A production HIE implementation would layer probabilistic matching
  (Fellegi-Sunter or a referential service like Verato) on top of
  deterministic matches. This module provides the deterministic foundation
  and the interface contract that a probabilistic layer would fulfill.

  The match_method field on silver.master_patient_index distinguishes
  DETERMINISTIC from PROBABILISTIC from MANUAL resolutions, enabling
  audit and downstream confidence filtering.
"""

import hashlib
import logging
import uuid
from dataclasses import dataclass
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PatientIdentity:
    """
    Normalized patient identity attributes extracted from a source record.
    This is the input to the MPI matching process.
    """
    # Source provenance
    tenant_id: str
    source_table: str           # bronze.hl7_messages or bronze.fhir_resources
    source_id: str              # bronze record PK

    # Source identifiers
    source_mrn: Optional[str]
    source_facility_npi: Optional[str]
    source_identifier_system: Optional[str]

    # Demographics (normalized to match Silver canonical form)
    family_name: Optional[str]
    given_name: Optional[str]
    birth_date: Optional[date]
    gender: Optional[str]
    postal_code: Optional[str]
    ssn_last4: Optional[str]    # last 4 only; never accept full SSN


@dataclass
class MPIResolutionResult:
    """Result of MPI lookup for a single patient."""
    umpi: str
    match_method: str           # DETERMINISTIC, NEW_RECORD, MANUAL
    match_confidence: float     # 1.0 = deterministic; 0.0 = no match (new record)
    is_new_record: bool         # True if a new UMPI was minted
    matched_on: list[str]       # which fields drove the match


# ---------------------------------------------------------------------------
# In-memory MPI index (reference implementation)
# ---------------------------------------------------------------------------
# In production, this is backed by silver.master_patient_index and
# silver.patient_identifiers tables in the data warehouse.
# This in-memory implementation supports local dev and unit testing.

class MPIIndex:
    """
    In-memory Master Patient Index for local development and testing.

    Maintains three deterministic lookup indexes:
      1. mrn_npi_index: (mrn, facility_npi) → umpi
      2. ssn4_dob_name_index: (ssn_last4, dob, family_name_upper) → umpi
      3. dob_name_zip_index: (dob, family_name_upper, given_name_upper, postal_code) → umpi

    Thread safety: Not thread-safe. Use a single instance per pipeline worker.
    """

    def __init__(self):
        self._umpi_records: dict[str, dict] = {}           # umpi → patient record
        self._mrn_npi_index: dict[tuple, str] = {}         # (mrn, npi) → umpi
        self._ssn4_dob_name_index: dict[tuple, str] = {}   # (ssn4, dob, name) → umpi
        self._dob_name_zip_index: dict[tuple, str] = {}    # (dob, fname, gname, zip) → umpi
        self._identifier_index: dict[tuple, str] = {}      # (system, value) → umpi

    def _normalize_name(self, name: Optional[str]) -> Optional[str]:
        """Uppercase, strip, collapse whitespace."""
        if not name:
            return None
        return " ".join(name.upper().split())

    def resolve(self, identity: PatientIdentity) -> MPIResolutionResult:
        """
        Attempt to find an existing UMPI for this patient.
        Falls through the matching hierarchy; mints a new UMPI on no match.
        """

        # Pass 1: Exact MRN + facility NPI
        if identity.source_mrn and identity.source_facility_npi:
            key = (identity.source_mrn.strip(), identity.source_facility_npi.strip())
            if key in self._mrn_npi_index:
                umpi = self._mrn_npi_index[key]
                logger.debug("MPI match (MRN+NPI): umpi=%s", umpi)
                return MPIResolutionResult(
                    umpi=umpi,
                    match_method="DETERMINISTIC",
                    match_confidence=1.0,
                    is_new_record=False,
                    matched_on=["source_mrn", "source_facility_npi"],
                )

        # Pass 2: Identifier system + value (e.g., FHIR identifier.system + value)
        if identity.source_identifier_system and identity.source_mrn:
            key = (identity.source_identifier_system.strip(), identity.source_mrn.strip())
            if key in self._identifier_index:
                umpi = self._identifier_index[key]
                logger.debug("MPI match (identifier system+value): umpi=%s", umpi)
                return MPIResolutionResult(
                    umpi=umpi,
                    match_method="DETERMINISTIC",
                    match_confidence=1.0,
                    is_new_record=False,
                    matched_on=["identifier_system", "identifier_value"],
                )

        # Pass 3: SSN-4 + DOB + family name
        if identity.ssn_last4 and identity.birth_date and identity.family_name:
            key = (
                identity.ssn_last4.strip(),
                str(identity.birth_date),
                self._normalize_name(identity.family_name),
            )
            if key in self._ssn4_dob_name_index:
                umpi = self._ssn4_dob_name_index[key]
                logger.debug("MPI match (SSN4+DOB+name): umpi=%s", umpi)
                return MPIResolutionResult(
                    umpi=umpi,
                    match_method="DETERMINISTIC",
                    match_confidence=1.0,
                    is_new_record=False,
                    matched_on=["ssn_last4", "birth_date", "family_name"],
                )

        # Pass 4: DOB + full name + postal code
        if identity.birth_date and identity.family_name and identity.given_name and identity.postal_code:
            key = (
                str(identity.birth_date),
                self._normalize_name(identity.family_name),
                self._normalize_name(identity.given_name),
                identity.postal_code.strip(),
            )
            if key in self._dob_name_zip_index:
                umpi = self._dob_name_zip_index[key]
                logger.debug("MPI match (DOB+name+zip): umpi=%s", umpi)
                return MPIResolutionResult(
                    umpi=umpi,
                    match_method="DETERMINISTIC",
                    match_confidence=0.95,
                    is_new_record=False,
                    matched_on=["birth_date", "family_name", "given_name", "postal_code"],
                )

        # No match — mint a new UMPI
        new_umpi = self._mint_umpi(identity)
        logger.debug("MPI new record minted: umpi=%s tenant=%s", new_umpi, identity.tenant_id)
        return MPIResolutionResult(
            umpi=new_umpi,
            match_method="NEW_RECORD",
            match_confidence=0.0,
            is_new_record=True,
            matched_on=[],
        )

    def _mint_umpi(self, identity: PatientIdentity) -> str:
        """
        Create a new UMPI, register the patient in all applicable indexes,
        and return the UMPI string.
        """
        new_umpi = str(uuid.uuid4())

        # Register in all applicable indexes
        if identity.source_mrn and identity.source_facility_npi:
            key = (identity.source_mrn.strip(), identity.source_facility_npi.strip())
            self._mrn_npi_index[key] = new_umpi

        if identity.source_identifier_system and identity.source_mrn:
            key = (identity.source_identifier_system.strip(), identity.source_mrn.strip())
            self._identifier_index[key] = new_umpi

        if identity.ssn_last4 and identity.birth_date and identity.family_name:
            key = (
                identity.ssn_last4.strip(),
                str(identity.birth_date),
                self._normalize_name(identity.family_name),
            )
            self._ssn4_dob_name_index[key] = new_umpi

        if identity.birth_date and identity.family_name and identity.given_name and identity.postal_code:
            key = (
                str(identity.birth_date),
                self._normalize_name(identity.family_name),
                self._normalize_name(identity.given_name),
                identity.postal_code.strip(),
            )
            self._dob_name_zip_index[key] = new_umpi

        # Store the patient record
        self._umpi_records[new_umpi] = {
            "umpi": new_umpi,
            "tenant_id": identity.tenant_id,
            "family_name": identity.family_name,
            "given_name": identity.given_name,
            "birth_date": str(identity.birth_date) if identity.birth_date else None,
            "gender": identity.gender,
            "postal_code": identity.postal_code,
            "ssn_last4": identity.ssn_last4,
        }

        return new_umpi

    def get_patient(self, umpi: str) -> Optional[dict]:
        """Retrieve the stored patient record for a UMPI."""
        return self._umpi_records.get(umpi)

    @property
    def patient_count(self) -> int:
        return len(self._umpi_records)


# ---------------------------------------------------------------------------
# FHIR Patient → PatientIdentity extractor
# ---------------------------------------------------------------------------

def fhir_patient_to_identity(
    resource: dict,
    tenant_id: str,
    source_table: str,
    source_id: str,
    facility_npi: Optional[str] = None,
) -> PatientIdentity:
    """
    Extract PatientIdentity from a FHIR R4 Patient resource dict.
    """
    # Name
    names = resource.get("name", [])
    official_name = next((n for n in names if n.get("use") == "official"), names[0] if names else {})
    family_name = official_name.get("family")
    given_names = official_name.get("given", [])
    given_name = given_names[0] if given_names else None

    # Birth date
    birth_date_str = resource.get("birthDate")
    birth_date = None
    if birth_date_str:
        try:
            birth_date = date.fromisoformat(birth_date_str)
        except ValueError:
            logger.warning("Could not parse FHIR birthDate: %s", birth_date_str)

    # Gender
    gender = resource.get("gender")

    # Address
    addresses = resource.get("address", [])
    home_address = next((a for a in addresses if a.get("use") == "home"), addresses[0] if addresses else {})
    postal_code = home_address.get("postalCode")

    # Identifiers
    identifiers = resource.get("identifier", [])
    source_mrn = None
    source_identifier_system = None
    ssn_last4 = None

    for ident in identifiers:
        id_type_codings = ident.get("type", {}).get("coding", [])
        id_type_code = next((c.get("code") for c in id_type_codings), None)

        if id_type_code == "MR" and not source_mrn:
            source_mrn = ident.get("value")
            source_identifier_system = ident.get("system")

        if id_type_code == "SS":
            raw_ssn = ident.get("value", "")
            if raw_ssn:
                # Never store full SSN — take last 4 only
                ssn_last4 = raw_ssn.replace("-", "").replace(" ", "")[-4:]

    return PatientIdentity(
        tenant_id=tenant_id,
        source_table=source_table,
        source_id=source_id,
        source_mrn=source_mrn,
        source_facility_npi=facility_npi,
        source_identifier_system=source_identifier_system,
        family_name=family_name,
        given_name=given_name,
        birth_date=birth_date,
        gender=gender,
        postal_code=postal_code,
        ssn_last4=ssn_last4,
    )


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json
    import os

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s %(message)s")

    sample_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "synthetic", "fhir_bundle_sample.json"
    )

    with open(sample_path) as f:
        bundle = json.load(f)

    mpi = MPIIndex()

    for entry in bundle.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") != "Patient":
            continue

        identity = fhir_patient_to_identity(
            resource=resource,
            tenant_id="INTEGRIS_BAPTIST",
            source_table="bronze.fhir_resources",
            source_id="demo-source-id-001",
        )

        result = mpi.resolve(identity)
        print(f"\nResolution result:")
        print(f"  umpi:             {result.umpi}")
        print(f"  match_method:     {result.match_method}")
        print(f"  match_confidence: {result.match_confidence}")
        print(f"  is_new_record:    {result.is_new_record}")
        print(f"  matched_on:       {result.matched_on}")

        # Resolve again — should match on identifier
        result2 = mpi.resolve(identity)
        print(f"\nSecond resolution (should match):")
        print(f"  umpi:             {result2.umpi}")
        print(f"  match_method:     {result2.match_method}")
        print(f"  is_new_record:    {result2.is_new_record}")
        assert result.umpi == result2.umpi, "UMPI mismatch on second resolution!"
        print(f"  ✓ UMPI consistent across resolutions")

    print(f"\nTotal patients in MPI: {mpi.patient_count}")
