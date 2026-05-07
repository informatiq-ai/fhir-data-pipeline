"""
Three-layer test strategy — Layer 2: Notebook import smoke tests.

Verifies that all four Databricks notebooks can be loaded as Python modules
without raising unexpected exceptions. Catches regressions where a syntax
error or top-level name error would prevent the module from loading.

Spark and dbutils are replaced with MagicMock. FileNotFoundError / OSError
from notebook-level open() calls are absorbed (schema constants must be
defined before those calls — enforced by the refactoring in prior commits).
"""
import importlib.util
import warnings
from pathlib import Path
from unittest.mock import MagicMock

import pytest

NOTEBOOKS = Path(__file__).parent.parent / "databricks" / "notebooks"


def _load(name: str):
    path = NOTEBOOKS / name
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    module.spark = MagicMock()
    module.dbutils = MagicMock()
    module.display = MagicMock()
    try:
        spec.loader.exec_module(module)
    except (FileNotFoundError, OSError):
        pass
    return module


class TestNotebookImports:

    def test_nb01_ingest_hl7_loads(self):
        mod = _load("01_ingest_hl7.py")
        assert hasattr(mod, "INGEST_HL7_MESSAGES_SCHEMA")
        assert hasattr(mod, "AUDIT_INGEST_LOG_SCHEMA")
        assert hasattr(mod, "AUDIT_VALIDATION_ERRORS_SCHEMA")

    def test_nb02_ingest_fhir_loads(self):
        mod = _load("02_ingest_fhir.py")
        assert hasattr(mod, "INGEST_FHIR_BUNDLES_SCHEMA")

    def test_nb03_bronze_to_silver_loads(self):
        mod = _load("03_bronze_to_silver.py")
        assert hasattr(mod, "MPI_PATIENT_INDEX_SCHEMA")
        assert hasattr(mod, "CLINICAL_PATIENTS_SCHEMA")
        assert hasattr(mod, "CLINICAL_OBSERVATIONS_SCHEMA")
        assert not hasattr(mod, "ECW_PATIENTS_FILE"), "ECW_PATIENTS_FILE must not exist in nb03 (moved to nb06)"
        assert not hasattr(mod, "ECW_LABS_FILE"), "ECW_LABS_FILE must not exist in nb03 (moved to nb06)"
        assert not hasattr(mod, "ECW_IDENTIFIER_SYSTEM"), "ECW_IDENTIFIER_SYSTEM must not exist in nb03 (moved to nb06)"

    def test_nb04_silver_to_gold_loads(self):
        mod = _load("04_silver_to_gold.py")
        assert hasattr(mod, "ANALYTICS_PATIENT_SUMMARY_SCHEMA")
        assert hasattr(mod, "ANALYTICS_ADT_EVENTS_SCHEMA")
        assert hasattr(mod, "EXPORT_USCDI_V3_PATIENT_SCHEMA")

    def test_nb05_ingest_csv_loads(self):
        mod = _load("05_ingest_csv.py")
        assert hasattr(mod, "CSV_BATCHES_SCHEMA")
        assert hasattr(mod, "VALIDATION_SCHEMA")
        assert hasattr(mod, "AUDIT_INGEST_LOG_SCHEMA")

    def test_nb06_bronze_to_silver_csv_loads(self):
        mod = _load("06_bronze_to_silver_csv.py")
        assert hasattr(mod, "MPI_PATIENT_INDEX_SCHEMA")
        assert hasattr(mod, "MPI_IDENTITY_CROSSWALK_SCHEMA")
        assert hasattr(mod, "CLINICAL_PATIENTS_SCHEMA")
        assert hasattr(mod, "CLINICAL_OBSERVATIONS_SCHEMA")
        assert hasattr(mod, "CLINICAL_CONDITIONS_SCHEMA")
        assert hasattr(mod, "TERMINOLOGY_UNMAPPED_CODES_SCHEMA")
        assert hasattr(mod, "AUDIT_VALIDATION_ERRORS_SCHEMA")
        assert hasattr(mod, "AUDIT_INGEST_LOG_SCHEMA")
