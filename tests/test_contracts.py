"""
Three-layer test strategy — Layer 1: Contract tests.

TestPatientIdentityContract  — API shape for identity resolution module
TestDDLSchemaContracts       — Notebook StructType constants vs DDL source of truth

Schema tests use pytest.skip() when a notebook has not yet been refactored to
expose module-level StructType constants. As each notebook is refactored the
corresponding test automatically activates without code changes here.
"""
import dataclasses
import importlib.util
import warnings
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

REPO_ROOT = Path(__file__).parent.parent
NOTEBOOKS = REPO_ROOT / "databricks" / "notebooks"


# ──────────────────────────────────────────────────────────────────
# Notebook loader
# ──────────────────────────────────────────────────────────────────

def _load_notebook(name: str):
    """Import a Databricks notebook as a module, injecting Spark/dbutils mocks.

    FileNotFoundError / OSError from notebook-level open() calls are caught;
    schema constants must be defined before those calls in refactored notebooks.
    Any other exception is surfaced as a warning so collection still succeeds.
    """
    path = NOTEBOOKS / name
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    module.spark = MagicMock()
    module.dbutils = MagicMock()
    module.display = MagicMock()  # Databricks built-in, not in stdlib
    try:
        spec.loader.exec_module(module)
    except (FileNotFoundError, OSError):
        pass
    except Exception as exc:
        warnings.warn(
            f"_load_notebook({name!r}) raised {type(exc).__name__}: {exc}",
            stacklevel=2,
        )
    return module


def _require_schema(module, constant: str) -> StructType:
    """Return a notebook schema constant or skip the test if not yet defined."""
    if not hasattr(module, constant):
        pytest.skip(
            f"{module.__name__} does not yet expose {constant} — "
            "refactor notebook to add module-level StructType constant"
        )
    return getattr(module, constant)


def _schema_diff(actual: StructType, expected: StructType) -> str:
    """Human-readable diff between two StructType objects."""
    actual_map = {f.name: f for f in actual}
    expected_map = {f.name: f for f in expected}
    lines = []
    for name, ef in expected_map.items():
        if name not in actual_map:
            lines.append(f"  MISSING  {name}")
        else:
            af = actual_map[name]
            if af.dataType != ef.dataType or af.nullable != ef.nullable:
                lines.append(
                    f"  MISMATCH {name}: "
                    f"got ({type(af.dataType).__name__}, nullable={af.nullable}), "
                    f"expected ({type(ef.dataType).__name__}, nullable={ef.nullable})"
                )
    for name in actual_map:
        if name not in expected_map:
            lines.append(f"  EXTRA    {name}")
    return "\n".join(lines) or "schemas differ (field order mismatch?)"


# ──────────────────────────────────────────────────────────────────
# Notebooks loaded once for the module (safe — mocks injected first)
# ──────────────────────────────────────────────────────────────────

_nb01 = _load_notebook("01_ingest_hl7.py")
_nb02 = _load_notebook("02_ingest_fhir.py")
_nb03 = _load_notebook("03_bronze_to_silver.py")
_nb04 = _load_notebook("04_silver_to_gold.py")
_nb05 = _load_notebook("05_ingest_csv.py")


# ──────────────────────────────────────────────────────────────────
# Expected schemas (source of truth: databricks/fhir_pipeline_ddl.sql)
# ──────────────────────────────────────────────────────────────────

EXPECTED_INGEST_HL7_MESSAGES = StructType([
    StructField("message_id",        StringType(),    False),
    StructField("raw_payload",       StringType(),    False),
    StructField("message_type",      StringType(),    True),
    StructField("message_event",     StringType(),    True),
    StructField("tenant_id",         StringType(),    False),
    StructField("source_system",     StringType(),    True),
    StructField("source_facility",   StringType(),    True),
    StructField("received_at",       TimestampType(), False),
    StructField("validation_status", StringType(),    True),
    StructField("pipeline_run_id",   StringType(),    True),
])

EXPECTED_INGEST_FHIR_BUNDLES = StructType([
    StructField("bundle_id",         StringType(),             False),
    StructField("raw_payload",       StringType(),             False),
    StructField("bundle_type",       StringType(),             True),
    StructField("resource_types",    ArrayType(StringType()),  True),
    StructField("tenant_id",         StringType(),             False),
    StructField("source_system",     StringType(),             True),
    StructField("received_at",       TimestampType(),          False),
    StructField("validation_status", StringType(),             True),
    StructField("pipeline_run_id",   StringType(),             True),
])

EXPECTED_INGEST_CSV_BATCHES = StructType([
    StructField("batch_id",          StringType(),    False),
    StructField("raw_payload",       StringType(),    False),
    StructField("source_system",     StringType(),    True),
    StructField("batch_frequency",   StringType(),    True),
    StructField("file_name",         StringType(),    True),
    StructField("file_size_bytes",   LongType(),      True),
    StructField("row_count",         LongType(),      True),
    StructField("tenant_id",         StringType(),    False),
    StructField("received_at",       TimestampType(), False),
    StructField("validation_status", StringType(),    True),
    StructField("pipeline_run_id",   StringType(),    True),
])

EXPECTED_AUDIT_INGEST_LOG = StructType([
    StructField("log_id",            StringType(),    False),
    StructField("pipeline_run_id",   StringType(),    False),
    StructField("ingestion_path",    StringType(),    True),
    StructField("source_table",      StringType(),    True),
    StructField("record_count",      LongType(),      True),
    StructField("pass_count",        LongType(),      True),
    StructField("error_count",       LongType(),      True),
    StructField("tenant_id",         StringType(),    True),
    StructField("run_started_at",    TimestampType(), True),
    StructField("run_completed_at",  TimestampType(), True),
    StructField("logged_at",         TimestampType(), False),
])

EXPECTED_AUDIT_VALIDATION_ERRORS = StructType([
    StructField("error_id",          StringType(),    False),
    StructField("pipeline_run_id",   StringType(),    True),
    StructField("ingestion_path",    StringType(),    True),
    StructField("source_record_id",  StringType(),    True),
    StructField("error_code",        StringType(),    True),
    StructField("error_message",     StringType(),    True),
    StructField("raw_payload",       StringType(),    True),
    StructField("tenant_id",         StringType(),    True),
    StructField("requires_review",   BooleanType(),   True),
    StructField("reviewed_at",       TimestampType(), True),
    StructField("reviewed_by",       StringType(),    True),
    StructField("review_outcome",    StringType(),    True),
    StructField("created_at",        TimestampType(), False),
])

EXPECTED_MPI_PATIENT_INDEX = StructType([
    StructField("umpi",                StringType(),            False),
    StructField("resolution_method",   StringType(),            True),
    StructField("first_resolved_at",   TimestampType(),         True),
    StructField("last_updated_at",     TimestampType(),         True),
    StructField("linked_record_count", LongType(),              True),
    StructField("tenant_ids",          ArrayType(StringType()), True),
    StructField("is_merged",           BooleanType(),           True),
    StructField("merged_into_umpi",    StringType(),            True),
])

EXPECTED_MPI_IDENTITY_CROSSWALK = StructType([
    StructField("crosswalk_id",      StringType(),    False),
    StructField("umpi",              StringType(),    False),
    StructField("source_mrn",        StringType(),    False),
    StructField("tenant_id",         StringType(),    False),
    StructField("source_system",     StringType(),    True),
    StructField("facility_id",       StringType(),    True),
    StructField("match_confidence",  DoubleType(),    True),
    StructField("created_at",        TimestampType(), False),
    StructField("updated_at",        TimestampType(), True),
])

EXPECTED_CLINICAL_PATIENTS = StructType([
    StructField("patient_id",         StringType(),    False),
    StructField("umpi",               StringType(),    False),
    StructField("first_name",         StringType(),    True),
    StructField("last_name",          StringType(),    True),
    StructField("date_of_birth",      DateType(),      True),
    StructField("gender",             StringType(),    True),
    StructField("race",               StringType(),    True),
    StructField("ethnicity",          StringType(),    True),
    StructField("preferred_language", StringType(),    True),
    StructField("address_line1",      StringType(),    True),
    StructField("address_line2",      StringType(),    True),
    StructField("city",               StringType(),    True),
    StructField("state",              StringType(),    True),
    StructField("zip",                StringType(),    True),
    StructField("phone",              StringType(),    True),
    StructField("email",              StringType(),    True),
    StructField("tenant_id",          StringType(),    False),
    StructField("source_system",      StringType(),    True),
    StructField("source_record_id",   StringType(),    True),
    StructField("created_at",         TimestampType(), False),
    StructField("updated_at",         TimestampType(), True),
])

EXPECTED_CLINICAL_OBSERVATIONS = StructType([
    StructField("observation_id",        StringType(),    False),
    StructField("umpi",                  StringType(),    False),
    StructField("encounter_id",          StringType(),    True),
    StructField("loinc_code",            StringType(),    False),
    StructField("loinc_display",         StringType(),    True),
    StructField("value_quantity",        DoubleType(),    True),
    StructField("value_unit",            StringType(),    True),
    StructField("value_string",          StringType(),    True),
    StructField("value_codeable_code",   StringType(),    True),
    StructField("value_codeable_system", StringType(),    True),
    StructField("reference_range_low",   DoubleType(),    True),
    StructField("reference_range_high",  DoubleType(),    True),
    StructField("interpretation",        StringType(),    True),
    StructField("observation_datetime",  TimestampType(), True),
    StructField("status",                StringType(),    True),
    StructField("tenant_id",             StringType(),    False),
    StructField("source_system",         StringType(),    True),
    StructField("source_code",           StringType(),    True),
    StructField("source_record_id",      StringType(),    True),
    StructField("created_at",            TimestampType(), False),
    StructField("updated_at",            TimestampType(), True),
])

EXPECTED_TERMINOLOGY_UNMAPPED_CODES = StructType([
    StructField("unmapped_id",       StringType(),    False),
    StructField("source_code",       StringType(),    False),
    StructField("source_display",    StringType(),    True),
    StructField("target_system",     StringType(),    False),
    StructField("source_system",     StringType(),    True),
    StructField("record_type",       StringType(),    True),
    StructField("source_record_id",  StringType(),    True),
    StructField("tenant_id",         StringType(),    False),
    StructField("pipeline_run_id",   StringType(),    True),
    StructField("logged_at",         TimestampType(), False),
    StructField("resolved",          BooleanType(),   True),
    StructField("resolved_at",       TimestampType(), True),
    StructField("resolved_by",       StringType(),    True),
    StructField("resolved_mapping",  StringType(),    True),
    StructField("resolution_notes",  StringType(),    True),
])

EXPECTED_ANALYTICS_PATIENT_SUMMARY = StructType([
    StructField("patient_key",             StringType(),                       False),
    StructField("charlson_index",          IntegerType(),                      True),
    StructField("elixhauser_index",        IntegerType(),                      True),
    StructField("chronic_condition_flags", MapType(StringType(), BooleanType()),True),
    StructField("pcp_npi",                 StringType(),                       True),
    StructField("pcp_attribution_method",  StringType(),                       True),
    StructField("last_encounter_date",     DateType(),                         True),
    StructField("total_encounter_count",   LongType(),                         True),
    StructField("tenant_id",               StringType(),                       False),
    StructField("pipeline_run_id",         StringType(),                       True),
    StructField("generated_at",            TimestampType(),                    False),
])

EXPECTED_ANALYTICS_QUALITY_MEASURES = StructType([
    StructField("measure_id",               StringType(),    False),
    StructField("patient_key",              StringType(),    False),
    StructField("measure_name",             StringType(),    False),
    StructField("measure_code",             StringType(),    True),
    StructField("in_denominator",           BooleanType(),   True),
    StructField("in_numerator",             BooleanType(),   True),
    StructField("excluded",                 BooleanType(),   True),
    StructField("hba1c_value",              DoubleType(),    True),
    StructField("measurement_period_start", DateType(),      True),
    StructField("measurement_period_end",   DateType(),      True),
    StructField("tenant_id",               StringType(),    False),
    StructField("pipeline_run_id",          StringType(),    True),
    StructField("generated_at",             TimestampType(), False),
])

EXPECTED_ANALYTICS_ADT_EVENTS = StructType([
    StructField("event_id",                   StringType(),    False),
    StructField("patient_key",                StringType(),    False),
    StructField("event_type",                 StringType(),    True),
    StructField("event_subtype",              StringType(),    True),
    StructField("facility_id",                StringType(),    True),
    StructField("event_datetime",             TimestampType(), False),
    StructField("readmission_30day",          BooleanType(),   True),
    StructField("prior_discharge_datetime",   TimestampType(), True),
    StructField("days_since_prior_discharge", DoubleType(),    True),
    StructField("tenant_id",                  StringType(),    False),
    StructField("pipeline_run_id",            StringType(),    True),
    StructField("generated_at",               TimestampType(), False),
])

EXPECTED_EXPORT_USCDI_V3_PATIENT = StructType([
    StructField("export_id",        StringType(),            False),
    StructField("patient_key",      StringType(),            False),
    StructField("uscdi_version",    StringType(),            True),
    StructField("export_payload",   StringType(),            False),
    StructField("export_datetime",  TimestampType(),         False),
    StructField("qhin_ready",       BooleanType(),           True),
    StructField("missing_elements", ArrayType(StringType()), True),
    StructField("tenant_id",        StringType(),            False),
    StructField("pipeline_run_id",  StringType(),            True),
])


# ──────────────────────────────────────────────────────────────────
# TestPatientIdentityContract
# ──────────────────────────────────────────────────────────────────

class TestPatientIdentityContract:

    def test_patient_identity_required_fields(self):
        from transforms.identity_resolution import PatientIdentity
        actual = {f.name for f in dataclasses.fields(PatientIdentity)}
        expected = {
            "tenant_id", "source_table", "source_id",
            "source_mrn", "source_facility_npi",
            "source_identifier_system",
            "family_name", "given_name",
            "birth_date", "gender",
            "postal_code", "ssn_last4",
        }
        assert actual == expected

    def test_patient_identity_rejects_unknown_kwargs(self):
        from transforms.identity_resolution import PatientIdentity
        with pytest.raises(TypeError):
            PatientIdentity(source_identifier_value="X")  # field removed in bug fix

    def test_mpi_resolve_returns_expected_fields(self):
        from transforms.identity_resolution import MPIIndex, PatientIdentity
        mpi = MPIIndex()
        identity = PatientIdentity(
            tenant_id="T1",
            source_table="test",
            source_id="ID-1",
            source_mrn="MRN-001",
            source_facility_npi="1234567890",
            source_identifier_system=None,
            family_name="Smith",
            given_name="Alice",
            birth_date=date(1980, 1, 1),
            gender="F",
            postal_code="73102",
            ssn_last4=None,
        )
        result = mpi.resolve(identity)
        for field in ("umpi", "match_method", "match_confidence", "is_new_record", "matched_on"):
            assert hasattr(result, field), f"MPIResolutionResult missing field: {field}"


# ──────────────────────────────────────────────────────────────────
# TestDDLSchemaContracts
# Schema tests skip until each notebook exposes its module-level constant.
# ──────────────────────────────────────────────────────────────────

class TestDDLSchemaContracts:

    # -- NB01 schemas ---------------------------------------------------

    def test_ingest_hl7_messages_schema(self):
        schema = _require_schema(_nb01, "INGEST_HL7_MESSAGES_SCHEMA")
        assert schema == EXPECTED_INGEST_HL7_MESSAGES, _schema_diff(schema, EXPECTED_INGEST_HL7_MESSAGES)

    def test_audit_ingest_log_schema(self):
        schema = _require_schema(_nb01, "AUDIT_INGEST_LOG_SCHEMA")
        assert schema == EXPECTED_AUDIT_INGEST_LOG, _schema_diff(schema, EXPECTED_AUDIT_INGEST_LOG)

    def test_audit_validation_errors_schema(self):
        schema = _require_schema(_nb01, "AUDIT_VALIDATION_ERRORS_SCHEMA")
        assert schema == EXPECTED_AUDIT_VALIDATION_ERRORS, _schema_diff(schema, EXPECTED_AUDIT_VALIDATION_ERRORS)

    # -- NB02 schemas ---------------------------------------------------

    def test_ingest_fhir_bundles_schema(self):
        schema = _require_schema(_nb02, "INGEST_FHIR_BUNDLES_SCHEMA")
        assert schema == EXPECTED_INGEST_FHIR_BUNDLES, _schema_diff(schema, EXPECTED_INGEST_FHIR_BUNDLES)

    # -- NB03 schemas ---------------------------------------------------

    def test_mpi_patient_index_schema(self):
        schema = _require_schema(_nb03, "MPI_PATIENT_INDEX_SCHEMA")
        assert schema == EXPECTED_MPI_PATIENT_INDEX, _schema_diff(schema, EXPECTED_MPI_PATIENT_INDEX)

    def test_mpi_identity_crosswalk_schema(self):
        schema = _require_schema(_nb03, "MPI_IDENTITY_CROSSWALK_SCHEMA")
        assert schema == EXPECTED_MPI_IDENTITY_CROSSWALK, _schema_diff(schema, EXPECTED_MPI_IDENTITY_CROSSWALK)

    def test_clinical_patients_schema(self):
        schema = _require_schema(_nb03, "CLINICAL_PATIENTS_SCHEMA")
        assert schema == EXPECTED_CLINICAL_PATIENTS, _schema_diff(schema, EXPECTED_CLINICAL_PATIENTS)

    def test_clinical_observations_schema(self):
        schema = _require_schema(_nb03, "CLINICAL_OBSERVATIONS_SCHEMA")
        assert schema == EXPECTED_CLINICAL_OBSERVATIONS, _schema_diff(schema, EXPECTED_CLINICAL_OBSERVATIONS)

    def test_terminology_unmapped_codes_schema(self):
        schema = _require_schema(_nb03, "TERMINOLOGY_UNMAPPED_CODES_SCHEMA")
        assert schema == EXPECTED_TERMINOLOGY_UNMAPPED_CODES, _schema_diff(schema, EXPECTED_TERMINOLOGY_UNMAPPED_CODES)

    # -- NB04 schemas ---------------------------------------------------

    def test_analytics_patient_summary_schema(self):
        schema = _require_schema(_nb04, "ANALYTICS_PATIENT_SUMMARY_SCHEMA")
        assert schema == EXPECTED_ANALYTICS_PATIENT_SUMMARY, _schema_diff(schema, EXPECTED_ANALYTICS_PATIENT_SUMMARY)

    def test_analytics_quality_measures_schema(self):
        schema = _require_schema(_nb04, "ANALYTICS_QUALITY_MEASURES_SCHEMA")
        assert schema == EXPECTED_ANALYTICS_QUALITY_MEASURES, _schema_diff(schema, EXPECTED_ANALYTICS_QUALITY_MEASURES)

    def test_analytics_adt_events_schema(self):
        schema = _require_schema(_nb04, "ANALYTICS_ADT_EVENTS_SCHEMA")
        assert schema == EXPECTED_ANALYTICS_ADT_EVENTS, _schema_diff(schema, EXPECTED_ANALYTICS_ADT_EVENTS)

    def test_export_uscdi_v3_patient_schema(self):
        schema = _require_schema(_nb04, "EXPORT_USCDI_V3_PATIENT_SCHEMA")
        assert schema == EXPECTED_EXPORT_USCDI_V3_PATIENT, _schema_diff(schema, EXPECTED_EXPORT_USCDI_V3_PATIENT)

    # -- NB05 schemas ---------------------------------------------------

    def test_ingest_csv_batches_schema(self):
        """CSV_BATCHES_SCHEMA in 05_ingest_csv.py must match ingest_csv_batches DDL."""
        schema = _require_schema(_nb05, "CSV_BATCHES_SCHEMA")
        assert schema == EXPECTED_INGEST_CSV_BATCHES, _schema_diff(schema, EXPECTED_INGEST_CSV_BATCHES)

    def test_audit_validation_errors_schema_csv(self):
        """VALIDATION_SCHEMA in 05_ingest_csv.py must match audit_validation_errors DDL."""
        schema = _require_schema(_nb05, "VALIDATION_SCHEMA")
        assert schema == EXPECTED_AUDIT_VALIDATION_ERRORS, _schema_diff(schema, EXPECTED_AUDIT_VALIDATION_ERRORS)

    def test_csv_error_codes_defined(self):
        """All CSV error code constants must be module-level strings in 05_ingest_csv.py.
        FHIR_MISSING_TENANT is verified in 02_ingest_fhir.py where it is used inline."""
        for constant in (
            "CSV_MISSING_REQUIRED_FIELD",
            "CSV_DUPLICATE_RECORD",
            "CSV_MALFORMED_DOB",
            "CSV_NON_NUMERIC_RESULT",
        ):
            assert hasattr(_nb05, constant), (
                f"05_ingest_csv.py is missing module-level constant: {constant}"
            )
            assert isinstance(getattr(_nb05, constant), str), (
                f"{constant} must be a string, got {type(getattr(_nb05, constant))}"
            )
            assert getattr(_nb05, constant) == constant, (
                f"{constant} value must equal its own name, got {getattr(_nb05, constant)!r}"
            )
        # Verify FHIR_MISSING_TENANT is used as a literal string in 02_ingest_fhir.py
        import inspect
        nb02_src = inspect.getsource(_nb02._detect_fhir_validation_issues) if hasattr(_nb02, "_detect_fhir_validation_issues") else ""
        assert "FHIR_MISSING_TENANT" in nb02_src or "FHIR_MISSING_TENANT" in str(
            [v for v in vars(_nb02).values() if isinstance(v, str)]
        ), "FHIR_MISSING_TENANT must appear in 02_ingest_fhir.py"
