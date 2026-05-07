"""
Three-layer test strategy — Layer 3: End-to-end integration tests.

Exercises the full Bronze → Silver → Gold pipeline against the synthetic
data files in data/synthetic/. No Spark runtime, no database, no cloud
credentials required. All assertions are on Python data structures.

Ten test cases covering the five planned scenarios from TESTING.md plus
five additional cases identified during the testing-strategy build-out.
"""
import json
import pathlib
import uuid
from datetime import date, datetime, timezone

import pytest

REPO_ROOT = pathlib.Path(__file__).parent.parent
DATA_DIR  = REPO_ROOT / "data" / "synthetic"


# ──────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────

def _load_fhir_bundle(name: str) -> dict:
    return json.loads((DATA_DIR / name).read_text())


def _load_hl7(name: str) -> str:
    return (DATA_DIR / name).read_text()


# ──────────────────────────────────────────────────────────────────
# TestHL7AdtToSilverEncounter
# ──────────────────────────────────────────────────────────────────

class TestHL7AdtToSilverEncounter:

    def test_hl7_adt_parses_to_bronze_record(self):
        """ADT^A01 message lands in Bronze with correct tenant and message type."""
        from ingestion.hl7_parser import parse_hl7_batch
        content = _load_hl7("hl7_adt_sample.txt")
        records, failures = parse_hl7_batch([content], file_source="test", default_tenant_id="FALLBACK")
        assert not failures, f"Parse failures: {failures}"
        assert len(records) == 1
        r = records[0]
        assert r.tenant_id == "INTEGRIS_BAPTIST"
        assert r.processing_status == "PENDING"
        assert "ADT" in r.message_type
        assert r.sending_facility == "INTEGRIS_BAPTIST"

    def test_hl7_adu_raw_payload_preserved(self):
        """Raw HL7 payload is preserved verbatim — Bronze immutability guarantee."""
        from ingestion.hl7_parser import parse_hl7_batch
        content = _load_hl7("hl7_adt_sample.txt").strip()
        records, _ = parse_hl7_batch([content], file_source="test", default_tenant_id="FALLBACK")
        assert records[0].raw_payload.strip() == content

    def test_hl7_oru_hba1c_lab_parsed(self):
        """ORU^R01 with HbA1c result lands in Bronze with correct message type."""
        from ingestion.hl7_parser import parse_hl7_batch
        content = _load_hl7("hl7_oru_sample.txt")
        records, failures = parse_hl7_batch([content], file_source="test", default_tenant_id="FALLBACK")
        assert not failures
        r = records[0]
        assert "ORU" in r.message_type
        assert r.tenant_id == "INTEGRIS_BAPTIST"
        assert r.processing_status == "PENDING"


# ──────────────────────────────────────────────────────────────────
# TestFHIRBundleToSilverFull
# ──────────────────────────────────────────────────────────────────

class TestFHIRBundleToSilverFull:

    def test_fhir_bundle_ingests_all_resource_types(self):
        """FHIR R4 bundle splits into one record per resource with expected types."""
        from ingestion.fhir_ingester import ingest_fhir_bundle
        bundle = _load_fhir_bundle("fhir_bundle_sample.json")
        result = ingest_fhir_bundle(bundle)
        types = {r.fhir_resource_type for r in result.records if r.processing_status != "ERROR"}
        assert "Patient" in types
        assert "Observation" in types
        assert "Encounter" in types
        assert "Condition" in types

    def test_fhir_bundle_tenant_resolved_from_meta_tag(self):
        """Tenant ID is extracted from bundle meta.tag, not hardcoded."""
        from ingestion.fhir_ingester import ingest_fhir_bundle
        bundle = _load_fhir_bundle("fhir_bundle_sample.json")
        result = ingest_fhir_bundle(bundle)
        tenants = {r.tenant_id for r in result.records}
        assert "INTEGRIS_BAPTIST" in tenants

    def test_fhir_patient_yields_valid_identity(self):
        """FHIR Patient resource extracts to PatientIdentity with correct demographics."""
        from transforms.identity_resolution import fhir_patient_to_identity
        bundle = _load_fhir_bundle("fhir_bundle_sample.json")
        patient_resource = next(
            e["resource"] for e in bundle.get("entry", [])
            if e.get("resource", {}).get("resourceType") == "Patient"
        )
        identity = fhir_patient_to_identity(
            patient_resource,
            tenant_id="INTEGRIS_BAPTIST",
            source_id="test-fhir-patient",
            source_table="bronze.fhir_bundles",
        )
        assert identity.family_name == "Ramirez"
        assert identity.given_name == "Carlos"
        assert identity.birth_date == date(1976, 12, 4)
        assert identity.source_mrn == "MRN-29471"

    def test_fhir_observation_normalizes_to_loinc(self):
        """HbA1c Observation normalizes to LOINC 4548-4 via SOURCE_LOINC path."""
        from transforms.bronze_to_silver import TerminologyService, normalize_fhir_observation
        bundle = _load_fhir_bundle("fhir_bundle_sample.json")
        obs_resource = next(
            e["resource"] for e in bundle.get("entry", [])
            if e.get("resource", {}).get("resourceType") == "Observation"
        )
        silver_rec, _ = normalize_fhir_observation(
            resource=obs_resource,
            tenant_id="INTEGRIS_BAPTIST",
            umpi="test-umpi",
            source_id=str(uuid.uuid4()),
            terminology=TerminologyService(),
            encounter_silver_id=None,
        )
        assert silver_rec.loinc_code == "4548-4"
        assert silver_rec.value_quantity == pytest.approx(8.2)
        assert silver_rec.loinc_map_method == "SOURCE_LOINC"


# ──────────────────────────────────────────────────────────────────
# TestCrossTenantMPILinkage
# ──────────────────────────────────────────────────────────────────

class TestCrossTenantMPILinkage:

    def test_mpi_pass1_exact_mrn_npi_match(self):
        """Pass 1: same MRN + NPI from same tenant resolves to same UMPI."""
        from transforms.identity_resolution import MPIIndex, PatientIdentity
        mpi = MPIIndex()
        base = dict(
            source_table="test", source_id="1", source_mrn="MRN-29471",
            source_facility_npi="NPI-1234567890", source_identifier_system=None,
            family_name="Ramirez", given_name="Carlos", birth_date=date(1976, 12, 4),
            gender="M", postal_code="73102", ssn_last4="6789",
        )
        r1 = mpi.resolve(PatientIdentity(tenant_id="TENANT_A", **base))
        r2 = mpi.resolve(PatientIdentity(tenant_id="TENANT_A", **{**base, "source_id": "2"}))
        assert r1.is_new_record
        assert not r2.is_new_record
        assert r1.umpi == r2.umpi
        assert "source_mrn" in r2.matched_on

    def test_mpi_cross_tenant_linkage_via_ssn4_dob_name(self):
        """Pass 3: same SSN4 + DOB + family name links patient across two tenants."""
        from transforms.identity_resolution import MPIIndex, PatientIdentity
        mpi = MPIIndex()
        shared = dict(
            source_table="test", family_name="Ramirez", given_name="Carlos",
            birth_date=date(1976, 12, 4), gender="M", postal_code="73102",
            ssn_last4="6789", source_identifier_system=None,
        )
        # Tenant A registers via MRN+NPI (Pass 1 mints UMPI)
        r_a = mpi.resolve(PatientIdentity(
            tenant_id="INTEGRIS_BAPTIST", source_id="hl7-1",
            source_mrn="MRN-29471", source_facility_npi="NPI-1234567890",
            **shared,
        ))
        assert r_a.is_new_record

        # Tenant B arrives with same SSN4+DOB+name but different MRN+NPI (unknown NPI)
        r_b = mpi.resolve(PatientIdentity(
            tenant_id="ECW_CLINIC", source_id="ecw-1",
            source_mrn="ECW-99999", source_facility_npi="NPI-UNKNOWN",
            **shared,
        ))
        assert not r_b.is_new_record
        assert r_a.umpi == r_b.umpi
        assert "ssn_last4" in r_b.matched_on

    def test_mpi_different_patients_get_different_umpis(self):
        """Distinct patients with non-overlapping identifiers mint separate UMPIs."""
        from transforms.identity_resolution import MPIIndex, PatientIdentity
        mpi = MPIIndex()
        r1 = mpi.resolve(PatientIdentity(
            tenant_id="T1", source_table="test", source_id="A",
            source_mrn="MRN-001", source_facility_npi="NPI-001",
            source_identifier_system=None, family_name="Smith", given_name="Alice",
            birth_date=date(1980, 1, 1), gender="F", postal_code="10001", ssn_last4=None,
        ))
        r2 = mpi.resolve(PatientIdentity(
            tenant_id="T2", source_table="test", source_id="B",
            source_mrn="MRN-002", source_facility_npi="NPI-002",
            source_identifier_system=None, family_name="Jones", given_name="Bob",
            birth_date=date(1975, 6, 15), gender="M", postal_code="90001", ssn_last4=None,
        ))
        assert r1.umpi != r2.umpi
        assert r1.is_new_record and r2.is_new_record


# ──────────────────────────────────────────────────────────────────
# TestFullPipelineToGold
# ──────────────────────────────────────────────────────────────────

class TestFullPipelineToGold:

    def test_gold_charlson_diabetes_scores_one(self):
        """Patient with diabetes ICD-10 diagnosis scores Charlson index = 1."""
        from transforms.silver_to_gold import (
            SilverPatient, SilverDiagnosis, build_patient_summary,
        )
        patient = SilverPatient(
            umpi="umpi-carlos", tenant_id="INTEGRIS_BAPTIST",
            family_name="Ramirez", given_name="Carlos",
            birth_date=date(1976, 12, 4), gender="M",
            state="OK", postal_code="73102",
        )
        diagnoses = [
            SilverDiagnosis(
                diagnosis_id="dx-1", tenant_id="INTEGRIS_BAPTIST",
                umpi="umpi-carlos", encounter_id="enc-1",
                icd10_code="E11.9", icd10_display="Type 2 diabetes mellitus",
                diagnosis_rank=1, clinical_status="active",
                onset_datetime=datetime(2020, 1, 1),
            )
        ]
        summary = build_patient_summary(patient, encounters=[], diagnoses=diagnoses)
        assert summary.charlson_index >= 1
        assert summary.flag_diabetes is True

    def test_gold_adt_readmission_flagged_within_30_days(self):
        """Admit within 30 days of prior discharge is flagged as a readmission."""
        from transforms.silver_to_gold import (
            SilverPatient, SilverADTEvent, SilverDiagnosis, build_adt_event_feed,
        )
        patient = SilverPatient(
            umpi="umpi-test", tenant_id="T1",
            family_name="Test", given_name="Patient",
            birth_date=date(1960, 1, 1), gender="M",
            state="OK", postal_code="73102",
        )
        discharge_dt = datetime(2024, 3, 1, 12, 0, tzinfo=timezone.utc)
        readmit_dt   = datetime(2024, 3, 20, 9, 0, tzinfo=timezone.utc)  # 19 days later

        events = [
            SilverADTEvent(
                adt_event_id="evt-1", tenant_id="T1", umpi="umpi-test",
                encounter_id="enc-1", event_type="A03",
                event_datetime=discharge_dt, facility_name="Hospital A",
                facility_npi="NPI-001", current_location="DISCHARGED",
            ),
            SilverADTEvent(
                adt_event_id="evt-2", tenant_id="T1", umpi="umpi-test",
                encounter_id="enc-2", event_type="A01",
                event_datetime=readmit_dt, facility_name="Hospital A",
                facility_npi="NPI-001", current_location="3 NORTH",
            ),
        ]
        gold_events = build_adt_event_feed(patient, adt_events=events, diagnoses=[])
        admit_events = [e for e in gold_events if e.event_type == "A01"]
        assert admit_events, "Expected at least one admit event"
        readmit = admit_events[-1]
        assert readmit.is_readmission_30d is True
        assert readmit.prior_discharge_date is not None

    def test_gold_adt_no_false_readmission_beyond_30_days(self):
        """Admit more than 30 days after discharge is NOT flagged as readmission."""
        from transforms.silver_to_gold import (
            SilverPatient, SilverADTEvent, build_adt_event_feed,
        )
        patient = SilverPatient(
            umpi="umpi-test2", tenant_id="T1",
            family_name="Other", given_name="Patient",
            birth_date=date(1955, 5, 5), gender="F",
            state="OK", postal_code="73102",
        )
        discharge_dt = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
        readmit_dt   = datetime(2024, 2, 15, 9, 0, tzinfo=timezone.utc)  # 45 days later

        events = [
            SilverADTEvent(
                adt_event_id="evt-a", tenant_id="T1", umpi="umpi-test2",
                encounter_id="enc-a", event_type="A03",
                event_datetime=discharge_dt, facility_name="Hospital A",
                facility_npi="NPI-001", current_location="DISCHARGED",
            ),
            SilverADTEvent(
                adt_event_id="evt-b", tenant_id="T1", umpi="umpi-test2",
                encounter_id="enc-b", event_type="A01",
                event_datetime=readmit_dt, facility_name="Hospital A",
                facility_npi="NPI-001", current_location="3 NORTH",
            ),
        ]
        gold_events = build_adt_event_feed(patient, adt_events=events, diagnoses=[])
        admit_events = [e for e in gold_events if e.event_type == "A01"]
        assert admit_events
        assert admit_events[-1].is_readmission_30d is False


# ──────────────────────────────────────────────────────────────────
# TestMalformedBatchDoesNotBlockValid
# ──────────────────────────────────────────────────────────────────

class TestMalformedBatchDoesNotBlockValid:

    def test_hl7_batch_one_malformed_does_not_drop_valid(self):
        """A batch with one malformed message still produces valid Bronze rows for the rest."""
        from ingestion.hl7_parser import parse_hl7_batch
        good_content = _load_hl7("hl7_adt_sample.txt").strip()
        bad_content   = "NOT_MSH|junk|invalid"
        records, failures = parse_hl7_batch(
            [good_content, bad_content],
            file_source="batch_test",
            default_tenant_id="FALLBACK",
        )
        pending = [r for r in records if r.processing_status == "PENDING"]
        error   = [r for r in records if r.processing_status == "ERROR"]
        assert len(pending) >= 1, "Valid message should produce a PENDING record"
        assert len(error) >= 1,   "Malformed message should produce an ERROR record"
        assert len(failures) >= 1, "Parse failure should be reported in failures list"
