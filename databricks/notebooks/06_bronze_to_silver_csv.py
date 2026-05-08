# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 — Bronze → Silver: CSV Batch Path
# MAGIC
# MAGIC **Purpose:** Read eClinicalWorks CSV batch rows from `ingest_csv_batches`, run MPI
# MAGIC resolution and terminology normalization, and write normalized records to Silver CDM
# MAGIC tables. This is the Silver-layer task for Job 2 (CSV Batch Pipeline).
# MAGIC All MPI matching logic is delegated entirely to `transforms/identity_resolution.py`.
# MAGIC
# MAGIC **Reads from:** `dev.fhir_bronze.ingest_csv_batches` (filtered by upstream_pipeline_run_id)
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_silver.mpi_patient_index`
# MAGIC - `dev.fhir_silver.mpi_identity_crosswalk`
# MAGIC - `dev.fhir_silver.clinical_patients`
# MAGIC - `dev.fhir_silver.clinical_conditions`
# MAGIC - `dev.fhir_silver.clinical_observations`
# MAGIC - `dev.fhir_silver.terminology_unmapped_codes`
# MAGIC - `dev.fhir_bronze.audit_ingest_log`
# MAGIC - `dev.fhir_bronze.audit_validation_errors`
# MAGIC
# MAGIC **Run order:** Task 2 of Job 2 (CSV Batch Pipeline). Run after `05_ingest_csv.py`,
# MAGIC before `07_silver_to_gold_csv.py`. This notebook appends only — Silver tables are
# MAGIC never truncated here.

# COMMAND ----------

import csv
import io
import sys
import os
import uuid
import re as _re
from datetime import datetime, timezone, date

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

dbutils.widgets.text("tenant_id", "INTEGRIS_BAPTIST", "Tenant ID")
TENANT_ID          = dbutils.widgets.get("tenant_id")
SRC_CATALOG        = "dev"
SRC_SCHEMA         = "fhir_bronze"
TGT_CATALOG        = "dev"
TGT_SCHEMA         = "fhir_silver"

BRONZE_CSV_TABLE        = f"{SRC_CATALOG}.{SRC_SCHEMA}.ingest_csv_batches"
BRONZE_AUDIT_TABLE      = f"{SRC_CATALOG}.{SRC_SCHEMA}.audit_ingest_log"
BRONZE_VALIDATION_TABLE = f"{SRC_CATALOG}.{SRC_SCHEMA}.audit_validation_errors"
NOTEBOOK_NAME           = "06_bronze_to_silver_csv"

TBL_MPI_PATIENTS         = f"{TGT_CATALOG}.{TGT_SCHEMA}.mpi_patient_index"
TBL_MPI_XWALK            = f"{TGT_CATALOG}.{TGT_SCHEMA}.mpi_identity_crosswalk"
TBL_CLINICAL_PATIENTS    = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_patients"
TBL_CLINICAL_CONDITIONS  = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_conditions"
TBL_CLINICAL_OBS         = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_observations"
TBL_UNMAPPED             = f"{TGT_CATALOG}.{TGT_SCHEMA}.terminology_unmapped_codes"

ECW_IDENTIFIER_SYSTEM = "urn:system:eclinicalworks"

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"tenant_id       : {TENANT_ID}")
print(f"source          : {BRONZE_CSV_TABLE}")
print(f"target catalog  : {TGT_CATALOG}.{TGT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget: upstream pipeline_run_id
# MAGIC
# MAGIC When run from the Databricks job, this widget receives the `pipeline_run_id`
# MAGIC from notebook 05 so only that run's Bronze rows are processed.
# MAGIC Leave blank to process all CSV batch rows.

# COMMAND ----------

dbutils.widgets.text(
    "upstream_pipeline_run_id",
    "",
    "Pipeline Run ID from notebook 05 (blank = all CSV batch rows)",
)
upstream_run_id = dbutils.widgets.get("upstream_pipeline_run_id").strip()

if upstream_run_id:
    csv_filter = f"pipeline_run_id = '{upstream_run_id}'"
    print(f"Filtering by pipeline_run_id: {upstream_run_id}")
else:
    csv_filter = "1=1"
    print("No upstream run ID — processing all CSV batch rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve repo root and import transform modules

# COMMAND ----------

_nb_path = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)
REPO_ROOT = "/Workspace" + _nb_path.rsplit("/databricks/notebooks", 1)[0]
print(f"REPO_ROOT: {REPO_ROOT}")

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from transforms.identity_resolution import (
    MPIIndex,
    PatientIdentity,
    fhir_patient_to_identity,
)
from transforms.bronze_to_silver import (
    TerminologyService,
    normalize_fhir_observation,
)

mpi        = MPIIndex()
terminology = TerminologyService()

print("Transform modules imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema constants
# MAGIC
# MAGIC All module-level StructType constants are defined here — before any Spark writes —
# MAGIC so that `test_contracts.py` and `test_notebook_imports.py` can import this notebook
# MAGIC as a module and assert on them without triggering Spark execution.
# MAGIC
# MAGIC Schema names match `databricks/fhir_pipeline_ddl.sql` exactly.
# MAGIC Do not add or rename columns here — the DDL is the single source of truth.

# COMMAND ----------

from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DoubleType, LongType,
    StringType, StructField, StructType, TimestampType,
)

MPI_PATIENT_INDEX_SCHEMA = StructType([
    StructField("umpi",                StringType(),            False),  # NOT NULL
    StructField("resolution_method",   StringType(),            True),
    StructField("first_resolved_at",   TimestampType(),         True),
    StructField("last_updated_at",     TimestampType(),         True),
    StructField("linked_record_count", LongType(),              True),
    StructField("tenant_ids",          ArrayType(StringType()), True),
    StructField("is_merged",           BooleanType(),           True),
    StructField("merged_into_umpi",    StringType(),            True),
])

MPI_IDENTITY_CROSSWALK_SCHEMA = StructType([
    StructField("crosswalk_id",     StringType(),    False),  # NOT NULL
    StructField("umpi",             StringType(),    False),  # NOT NULL
    StructField("source_mrn",       StringType(),    False),  # NOT NULL
    StructField("tenant_id",        StringType(),    False),  # NOT NULL
    StructField("source_system",    StringType(),    True),
    StructField("facility_id",      StringType(),    True),
    StructField("match_confidence", DoubleType(),    True),
    StructField("created_at",       TimestampType(), False),  # NOT NULL
    StructField("updated_at",       TimestampType(), True),
])

CLINICAL_PATIENTS_SCHEMA = StructType([
    StructField("patient_id",         StringType(),    False),  # NOT NULL
    StructField("umpi",               StringType(),    False),  # NOT NULL
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
    StructField("tenant_id",          StringType(),    False),  # NOT NULL
    StructField("source_system",      StringType(),    True),
    StructField("source_record_id",   StringType(),    True),
    StructField("created_at",         TimestampType(), False),  # NOT NULL
    StructField("updated_at",         TimestampType(), True),
])

CLINICAL_OBSERVATIONS_SCHEMA = StructType([
    StructField("observation_id",        StringType(),    False),  # NOT NULL
    StructField("umpi",                  StringType(),    False),  # NOT NULL
    StructField("encounter_id",          StringType(),    True),
    StructField("loinc_code",            StringType(),    False),  # NOT NULL
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
    StructField("tenant_id",             StringType(),    False),  # NOT NULL
    StructField("source_system",         StringType(),    True),
    StructField("source_code",           StringType(),    True),
    StructField("source_record_id",      StringType(),    True),
    StructField("created_at",            TimestampType(), False),  # NOT NULL
    StructField("updated_at",            TimestampType(), True),
])

CLINICAL_CONDITIONS_SCHEMA = StructType([
    StructField("condition_id",         StringType(),    False),  # NOT NULL
    StructField("umpi",                 StringType(),    False),  # NOT NULL
    StructField("encounter_id",         StringType(),    True),
    StructField("icd10_code",           StringType(),    False),  # NOT NULL
    StructField("icd10_display",        StringType(),    True),
    StructField("condition_category",   StringType(),    True),
    StructField("onset_datetime",       TimestampType(), True),
    StructField("abatement_datetime",   TimestampType(), True),
    StructField("clinical_status",      StringType(),    True),
    StructField("verification_status",  StringType(),    True),
    StructField("tenant_id",            StringType(),    False),  # NOT NULL
    StructField("source_system",        StringType(),    True),
    StructField("source_code",          StringType(),    True),
    StructField("source_record_id",     StringType(),    True),
    StructField("created_at",           TimestampType(), False),  # NOT NULL
    StructField("updated_at",           TimestampType(), True),
])

TERMINOLOGY_UNMAPPED_CODES_SCHEMA = StructType([
    StructField("unmapped_id",       StringType(),    False),  # NOT NULL
    StructField("source_code",       StringType(),    False),  # NOT NULL
    StructField("source_display",    StringType(),    True),
    StructField("target_system",     StringType(),    False),  # NOT NULL
    StructField("source_system",     StringType(),    True),
    StructField("record_type",       StringType(),    True),
    StructField("source_record_id",  StringType(),    True),
    StructField("tenant_id",         StringType(),    False),  # NOT NULL
    StructField("pipeline_run_id",   StringType(),    True),
    StructField("logged_at",         TimestampType(), False),  # NOT NULL
    StructField("resolved",          BooleanType(),   True),
    StructField("resolved_at",       TimestampType(), True),
    StructField("resolved_by",       StringType(),    True),
    StructField("resolved_mapping",  StringType(),    True),
    StructField("resolution_notes",  StringType(),    True),
])

AUDIT_VALIDATION_ERRORS_SCHEMA = StructType([
    StructField("error_id",          StringType(),    False),  # NOT NULL
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
    StructField("created_at",        TimestampType(), False),  # NOT NULL
])

AUDIT_INGEST_LOG_SCHEMA = StructType([
    StructField("log_id",            StringType(),    False),  # NOT NULL
    StructField("pipeline_run_id",   StringType(),    False),  # NOT NULL
    StructField("ingestion_path",    StringType(),    True),
    StructField("source_table",      StringType(),    True),
    StructField("record_count",      LongType(),      True),
    StructField("pass_count",        LongType(),      True),
    StructField("error_count",       LongType(),      True),
    StructField("tenant_id",         StringType(),    True),
    StructField("run_started_at",    TimestampType(), True),
    StructField("run_completed_at",  TimestampType(), True),
    StructField("logged_at",         TimestampType(), False),  # NOT NULL
])

print("Schema constants defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed MPIIndex from existing Silver records (idempotency)
# MAGIC
# MAGIC Restores MPI state from `clinical_patients` (passes 3/4) and
# MAGIC `mpi_identity_crosswalk` (pass 1: MRN+NPI) so that CSV patients who were
# MAGIC previously resolved by the FHIR/HL7 path are correctly linked rather than
# MAGIC minted as new UMPIs.

# COMMAND ----------

existing_patients = spark.sql(f"""
    SELECT umpi, last_name, first_name, date_of_birth, zip
    FROM {TBL_CLINICAL_PATIENTS}
""").collect()

existing_xwalk = spark.sql(f"""
    SELECT umpi, source_mrn, facility_id
    FROM {TBL_MPI_XWALK}
""").collect()

for r in existing_patients:
    umpi = r["umpi"]
    birth_str = r["date_of_birth"].isoformat() if r["date_of_birth"] else None
    mpi._umpi_records[umpi] = {
        "umpi":        umpi,
        "tenant_id":   None,
        "family_name": r["last_name"],
        "given_name":  r["first_name"],
        "birth_date":  birth_str,
        "gender":      None,
        "postal_code": r["zip"],
        "ssn_last4":   None,
    }
    if birth_str and r["last_name"] and r["first_name"] and r["zip"]:
        key = (
            birth_str,
            mpi._normalize_name(r["last_name"]),
            mpi._normalize_name(r["first_name"]),
            r["zip"],
        )
        mpi._dob_name_zip_index[key] = umpi

for r in existing_xwalk:
    umpi = r["umpi"]
    if r["source_mrn"] and r["facility_id"]:
        mpi._mrn_npi_index[(r["source_mrn"], r["facility_id"])] = umpi

print(f"MPIIndex seeded from existing Silver records:")
print(f"  clinical_patients rows : {len(existing_patients)}")
print(f"  crosswalk rows         : {len(existing_xwalk)}")
print(f"  _umpi_records          : {len(mpi._umpi_records)}")
print(f"  _dob_name_zip_index    : {len(mpi._dob_name_zip_index)}")
print(f"  _mrn_npi_index         : {len(mpi._mrn_npi_index)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read from ingest_csv_batches
# MAGIC
# MAGIC Fetches two rows for this run:
# MAGIC - `source_system = 'eClinicalWorks'` — ecw_patients content in `raw_payload`
# MAGIC - `source_system = 'ECW_LABS'`       — ecw_labs content in `raw_payload`
# MAGIC
# MAGIC Each `raw_payload` is parsed with `csv.DictReader(io.StringIO(raw_payload))`.

# COMMAND ----------

csv_batch_rows = spark.sql(f"""
    SELECT batch_id, source_system, raw_payload, pipeline_run_id AS bronze_run_id
    FROM {BRONZE_CSV_TABLE}
    WHERE {csv_filter}
""").collect()

patients_batch = next(
    (r for r in csv_batch_rows if r["source_system"] == "eClinicalWorks"), None
)
labs_batch = next(
    (r for r in csv_batch_rows if r["source_system"] == "ECW_LABS"), None
)

print(f"Batch rows fetched: {len(csv_batch_rows)}")
print(f"  eClinicalWorks (patients) : {'found' if patients_batch else 'not found'}")
print(f"  ECW_LABS (labs)           : {'found' if labs_batch else 'not found'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper functions

# COMMAND ----------

def _parse_iso_dob(dob_str):
    """Accept YYYY-MM-DD only. Returns (date_obj_or_None, is_malformed: bool)."""
    if not dob_str:
        return None, False
    if _re.fullmatch(r"\d{4}-\d{2}-\d{2}", dob_str.strip()):
        try:
            return date.fromisoformat(dob_str.strip()), False
        except ValueError:
            return None, True
    return None, True


def _to_float(s):
    """Parse float from string; return None on failure."""
    if s is None:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def _unmapped_row(source_code, source_display, target_system, source_system,
                  record_type, source_record_id, run_id, now_ts):
    """Build a terminology_unmapped_codes row matching the DDL schema exactly."""
    return {
        "unmapped_id":       str(uuid.uuid4()),
        "source_code":       source_code or "UNKNOWN",   # NOT NULL — fallback if absent
        "source_display":    source_display,
        "target_system":     target_system,               # NOT NULL
        "source_system":     source_system,
        "record_type":       record_type,
        "source_record_id":  source_record_id,
        "tenant_id":         TENANT_ID,                   # NOT NULL
        "pipeline_run_id":   run_id,
        "logged_at":         now_ts,                      # NOT NULL
        "resolved":          False,
        "resolved_at":       None,
        "resolved_by":       None,
        "resolved_mapping":  None,
        "resolution_notes":  None,
    }


def _csv_validation_error(code, field, detail, row_id, raw_val, run_id):
    return {
        "error_id":         str(uuid.uuid4()),
        "pipeline_run_id":  run_id,
        "ingestion_path":   "csv",
        "source_record_id": row_id,
        "error_code":       code,
        "error_message":    f"{field}: {detail}",
        "raw_payload":      str(raw_val) if raw_val is not None else None,
        "tenant_id":        TENANT_ID,
        "requires_review":  True,
        "reviewed_at":      None,
        "reviewed_by":      None,
        "review_outcome":   None,
        "created_at":       datetime.now(timezone.utc).replace(tzinfo=None),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step CSV-1 — Process patients batch (eClinicalWorks)
# MAGIC
# MAGIC DQ issues detected and logged to `audit_validation_errors`:
# MAGIC - `CSV_BLANK_NAME`    — first_name or last_name is empty
# MAGIC - `CSV_MALFORMED_DOB` — DOB not in YYYY-MM-DD format; date_of_birth → NULL
# MAGIC - `CSV_DUPLICATE_ROW` — same patient_id seen more than once
# MAGIC - `CSV_INVALID_ICD10` — primary_dx_icd10 produces no SNOMED mapping

# COMMAND ----------

csv_mpi_rows        = []    # → mpi_patient_index (new UMPIs only)
csv_xwalk_rows      = []    # → mpi_identity_crosswalk
csv_patient_rows    = []    # → clinical_patients
csv_condition_rows  = []    # → clinical_conditions (primary_dx_icd10 per patient)
csv_validation_errs = []    # → audit_validation_errors
ecw_patient_umpi_map = {}   # ECW patient_id → umpi (for lab linkage)
seen_patient_ids    = {}    # duplicate detection

csv_patients_started = datetime.now(timezone.utc).replace(tzinfo=None)

if patients_batch:
    raw_payload = patients_batch["raw_payload"]
    reader = csv.DictReader(io.StringIO(raw_payload))
    for row_idx, row in enumerate(reader):
        now_ts_csv = datetime.now(timezone.utc).replace(tzinfo=None)
        patient_id = row["patient_id"].strip()
        first_name = row["first_name"].strip()
        last_name  = row["last_name"].strip()
        raw_dob    = row["dob"].strip()
        gender     = row["gender"].strip()
        ssn_last4  = row["ssn_last4"].strip() or None
        zip_code   = row["zip"].strip() or None
        pcp_npi    = row["pcp_npi"].strip() or None
        primary_dx = row["primary_dx_icd10"].strip() or None

        dob_date, dob_malformed = _parse_iso_dob(raw_dob)

        # DQ: blank name
        if not first_name:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_BLANK_NAME", "first_name",
                "first_name is blank; MPI name-based passes may degrade",
                patient_id, first_name, pipeline_run_id,
            ))
        if not last_name:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_BLANK_NAME", "last_name",
                "last_name is blank; MPI name-based passes may degrade",
                patient_id, last_name, pipeline_run_id,
            ))

        # DQ: malformed DOB
        if dob_malformed:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_MALFORMED_DOB", "dob",
                "DOB is not in ISO 8601 format (expected YYYY-MM-DD); date_of_birth set to NULL",
                patient_id, raw_dob, pipeline_run_id,
            ))

        # DQ: duplicate row
        if patient_id in seen_patient_ids:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_DUPLICATE_ROW", "patient_id",
                f"Duplicate patient_id first seen at row {seen_patient_ids[patient_id]}; "
                f"MPI will return same UMPI (DETERMINISTIC match)",
                patient_id, patient_id, pipeline_run_id,
            ))
        else:
            seen_patient_ids[patient_id] = row_idx

        # MPI resolution
        identity = PatientIdentity(
            source_id=patient_id,
            source_table=BRONZE_CSV_TABLE,
            tenant_id=TENANT_ID,
            family_name=last_name or None,
            given_name=first_name or None,
            birth_date=dob_date,
            gender=gender or None,
            postal_code=zip_code,
            ssn_last4=ssn_last4,
            source_mrn=patient_id,
            source_facility_npi=pcp_npi,
            source_identifier_system=ECW_IDENTIFIER_SYSTEM,
        )
        result = mpi.resolve(identity)
        ecw_patient_umpi_map[patient_id] = result.umpi

        # mpi_patient_index: new patients only
        if result.is_new_record:
            csv_mpi_rows.append({
                "umpi":                result.umpi,
                "resolution_method":   result.match_method,
                "first_resolved_at":   now_ts_csv,
                "last_updated_at":     now_ts_csv,
                "linked_record_count": 1,
                "tenant_ids":          [TENANT_ID],
                "is_merged":           False,
                "merged_into_umpi":    None,
            })

        # mpi_identity_crosswalk: source_mrn NOT NULL — patient_id always present
        csv_xwalk_rows.append({
            "crosswalk_id":     str(uuid.uuid4()),
            "umpi":             result.umpi,
            "source_mrn":       patient_id,
            "tenant_id":        TENANT_ID,
            "source_system":    "eClinicalWorks",
            "facility_id":      pcp_npi,
            "match_confidence": result.match_confidence,
            "created_at":       now_ts_csv,
            "updated_at":       None,
        })

        # DQ: invalid ICD-10
        if primary_dx and terminology.map_snomed_from_icd10(primary_dx) is None:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_INVALID_ICD10", "primary_dx_icd10",
                f"ICD-10 code '{primary_dx}' has no SNOMED mapping; written as-is",
                patient_id, primary_dx, pipeline_run_id,
            ))

        # clinical_conditions: primary diagnosis — written as-is regardless of SNOMED mapping
        if primary_dx:
            csv_condition_rows.append({
                "condition_id":         str(uuid.uuid4()),
                "umpi":                 result.umpi,
                "encounter_id":         None,
                "icd10_code":           primary_dx,
                "icd10_display":        None,
                "condition_category":   "primary",
                "onset_datetime":       None,
                "abatement_datetime":   None,
                "clinical_status":      "active",
                "verification_status":  "confirmed",
                "tenant_id":            TENANT_ID,
                "source_system":        "eClinicalWorks",
                "source_code":          primary_dx,
                "source_record_id":     patient_id,
                "created_at":           now_ts_csv,
                "updated_at":           None,
            })

        # clinical_patients: DDL-aligned (21 columns)
        csv_patient_rows.append({
            "patient_id":         str(uuid.uuid4()),
            "umpi":               result.umpi,
            "first_name":         first_name or None,
            "last_name":          last_name or None,
            "date_of_birth":      dob_date,
            "gender":             gender or None,
            "race":               None,
            "ethnicity":          None,
            "preferred_language": None,
            "address_line1":      row["address"].strip() or None,
            "address_line2":      None,
            "city":               row["city"].strip() or None,
            "state":              row["state"].strip() or None,
            "zip":                zip_code,
            "phone":              row["phone"].strip() or None,
            "email":              None,
            "tenant_id":          TENANT_ID,
            "source_system":      "eClinicalWorks",
            "source_record_id":   patient_id,
            "created_at":         now_ts_csv,
            "updated_at":         None,
        })

csv_patients_completed = datetime.now(timezone.utc).replace(tzinfo=None)

print(f"ECW patients processed  : {len(csv_patient_rows):,} rows")
print(f"New UMPIs minted        : {len(csv_mpi_rows)}")
print(f"Crosswalk entries       : {len(csv_xwalk_rows)}")
print(f"Validation issues       : {len(csv_validation_errs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step CSV-2 — Write ECW patient rows to Silver

# COMMAND ----------

now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

if csv_patient_rows:
    cp_csv_df = spark.createDataFrame(csv_patient_rows, schema=CLINICAL_PATIENTS_SCHEMA)
    cp_csv_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_PATIENTS)
    print(f"Wrote {cp_csv_df.count():,} ECW patient row(s) to {TBL_CLINICAL_PATIENTS}")
else:
    print("No ECW patient rows — clinical_patients not written")

if csv_mpi_rows:
    mpi_csv_df = spark.createDataFrame(csv_mpi_rows, schema=MPI_PATIENT_INDEX_SCHEMA)
    mpi_csv_df.write.format("delta").mode("append").insertInto(TBL_MPI_PATIENTS)
    print(f"Wrote {len(csv_mpi_rows)} new UMPI(s) to {TBL_MPI_PATIENTS}")
else:
    print("All ECW patients matched existing UMPIs — mpi_patient_index not written")

if csv_xwalk_rows:
    xwalk_csv_df = spark.createDataFrame(csv_xwalk_rows, schema=MPI_IDENTITY_CROSSWALK_SCHEMA)
    xwalk_csv_df.write.format("delta").mode("append").insertInto(TBL_MPI_XWALK)
    print(f"Wrote {len(csv_xwalk_rows)} crosswalk row(s) to {TBL_MPI_XWALK}")
else:
    print("No ECW crosswalk rows — mpi_identity_crosswalk not written")

if csv_condition_rows:
    cond_csv_df = spark.createDataFrame(csv_condition_rows, schema=CLINICAL_CONDITIONS_SCHEMA)
    cond_csv_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_CONDITIONS)
    print(f"Wrote {len(csv_condition_rows)} ECW condition row(s) to {TBL_CLINICAL_CONDITIONS}")
else:
    print("No ECW condition rows — clinical_conditions not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step CSV-3 — Process labs batch (ECW_LABS)
# MAGIC
# MAGIC DQ issues detected and logged to `audit_validation_errors`:
# MAGIC - `CSV_UNMAPPED_LAB_CODE`  — test_code has no LOINC mapping
# MAGIC - `CSV_TEXT_RESULT_VALUE`  — result_value cannot be parsed as float

# COMMAND ----------

csv_obs_rows      = []   # → clinical_observations (mapped only)
csv_unmapped_rows = []   # → terminology_unmapped_codes
csv_lab_val_errs  = []   # collected here, appended to csv_validation_errs below

csv_labs_started = datetime.now(timezone.utc).replace(tzinfo=None)

if labs_batch:
    raw_payload = labs_batch["raw_payload"]
    reader = csv.DictReader(io.StringIO(raw_payload))
    for row in reader:
        now_ts_lab  = datetime.now(timezone.utc).replace(tzinfo=None)
        result_id   = row["result_id"].strip()
        ecw_pat_id  = row["patient_id"].strip()
        test_code   = row["test_code"].strip()
        test_name   = row["test_name"].strip()
        raw_value   = row["result_value"].strip()
        result_unit = row["result_unit"].strip() or None
        ref_low     = row["reference_range_low"].strip() or None
        ref_high    = row["reference_range_high"].strip() or None
        abnormal    = row["abnormal_flag"].strip() or None
        collect_date = row["collection_date"].strip() or None
        prov_npi    = row["ordering_provider_npi"].strip() or None
        status      = row["status"].strip() or None

        umpi = ecw_patient_umpi_map.get(ecw_pat_id, "UNKNOWN")

        # LOINC normalization: try test_code, fallback to test_name
        loinc_result = terminology.map_loinc(test_code)
        map_method   = "SOURCE_LOINC"
        if loinc_result is None:
            loinc_result = terminology.map_loinc(test_name)
            map_method   = "TERMINOLOGY_SERVICE" if loinc_result else "UNMAPPED"

        loinc_code    = loinc_result[0] if loinc_result else None
        loinc_display = loinc_result[1] if loinc_result else None

        # Parse numeric result value
        value_quantity = None
        is_text_value  = False
        try:
            value_quantity = float(raw_value)
        except (ValueError, TypeError):
            is_text_value = True

        if is_text_value and raw_value:
            csv_lab_val_errs.append(_csv_validation_error(
                "CSV_TEXT_RESULT_VALUE", "result_value",
                f"result_value '{raw_value}' cannot be parsed as float; value_quantity=NULL",
                result_id, raw_value, pipeline_run_id,
            ))

        # observation_datetime from collection_date (date → midnight timestamp)
        obs_dt = None
        if collect_date:
            try:
                obs_dt = datetime.combine(date.fromisoformat(collect_date), datetime.min.time())
            except (ValueError, TypeError):
                obs_dt = None

        if loinc_code:
            csv_obs_rows.append({
                "observation_id":        str(uuid.uuid4()),
                "umpi":                  umpi,
                "encounter_id":          None,
                "loinc_code":            loinc_code,
                "loinc_display":         loinc_display,
                "value_quantity":        value_quantity,
                "value_unit":            result_unit,
                "value_string":          raw_value if is_text_value else None,
                "value_codeable_code":   None,
                "value_codeable_system": None,
                "reference_range_low":   _to_float(ref_low),
                "reference_range_high":  _to_float(ref_high),
                "interpretation":        abnormal,
                "observation_datetime":  obs_dt,
                "status":                status,
                "tenant_id":             TENANT_ID,
                "source_system":         "eClinicalWorks",
                "source_code":           test_code,
                "source_record_id":      result_id,
                "created_at":            now_ts_lab,
                "updated_at":            None,
            })
        else:
            csv_lab_val_errs.append(_csv_validation_error(
                "CSV_UNMAPPED_LAB_CODE", "test_code",
                f"test_code '{test_code}' (test_name='{test_name}') has no LOINC mapping; "
                f"entry written to terminology_unmapped_codes",
                result_id, test_code, pipeline_run_id,
            ))
            csv_unmapped_rows.append(_unmapped_row(
                source_code=test_code,
                source_display=test_name,
                target_system="LOINC",
                source_system="eClinicalWorks",
                record_type="observation",
                source_record_id=result_id,
                run_id=pipeline_run_id,
                now_ts=now_ts_lab,
            ))

csv_labs_completed = datetime.now(timezone.utc).replace(tzinfo=None)
csv_validation_errs.extend(csv_lab_val_errs)

print(f"ECW lab rows processed  : {len(csv_obs_rows) + len(csv_unmapped_rows):,}")
print(f"LOINC-mapped            : {len(csv_obs_rows):,}")
print(f"LOINC-unmapped          : {len(csv_unmapped_rows):,}")
print(f"Text result values      : {sum(1 for r in csv_obs_rows if r['value_quantity'] is None):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step CSV-4 — Write ECW lab rows to Silver

# COMMAND ----------

if csv_obs_rows:
    obs_csv_df = spark.createDataFrame(csv_obs_rows, schema=CLINICAL_OBSERVATIONS_SCHEMA)
    obs_csv_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_OBS)
    print(f"Wrote {obs_csv_df.count():,} ECW observation row(s) to {TBL_CLINICAL_OBS}")
else:
    print("No ECW lab rows mapped to LOINC — clinical_observations not written")

if csv_unmapped_rows:
    unmapped_csv_df = spark.createDataFrame(csv_unmapped_rows, schema=TERMINOLOGY_UNMAPPED_CODES_SCHEMA)
    unmapped_csv_df.write.format("delta").mode("append").insertInto(TBL_UNMAPPED)
    print(f"Wrote {len(csv_unmapped_rows)} ECW unmapped code(s) to {TBL_UNMAPPED}")
else:
    print("All ECW lab codes mapped to LOINC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step CSV-5 — Write validation errors and audit log

# COMMAND ----------

if csv_validation_errs:
    val_df_csv = spark.createDataFrame(csv_validation_errs, schema=AUDIT_VALIDATION_ERRORS_SCHEMA)
    val_df_csv.write.format("delta").mode("append").insertInto(BRONZE_VALIDATION_TABLE)
    print(f"Wrote {val_df_csv.count():,} CSV validation error(s) to {BRONZE_VALIDATION_TABLE}")
else:
    print("No CSV validation errors")

audit_entries = []

if patients_batch:
    audit_entries.append({
        "log_id":           str(uuid.uuid4()),
        "pipeline_run_id":  pipeline_run_id,
        "ingestion_path":   "csv",
        "source_table":     TBL_CLINICAL_PATIENTS,
        "record_count":     len(csv_patient_rows),
        "pass_count":       len(csv_patient_rows),
        "error_count":      0,
        "tenant_id":        TENANT_ID,
        "run_started_at":   csv_patients_started,
        "run_completed_at": csv_patients_completed,
        "logged_at":        datetime.now(timezone.utc).replace(tzinfo=None),
    })
    audit_entries.append({
        "log_id":           str(uuid.uuid4()),
        "pipeline_run_id":  pipeline_run_id,
        "ingestion_path":   "csv",
        "source_table":     TBL_CLINICAL_CONDITIONS,
        "record_count":     len(csv_condition_rows),
        "pass_count":       len(csv_condition_rows),
        "error_count":      0,
        "tenant_id":        TENANT_ID,
        "run_started_at":   csv_patients_started,
        "run_completed_at": csv_patients_completed,
        "logged_at":        datetime.now(timezone.utc).replace(tzinfo=None),
    })

if labs_batch:
    audit_entries.append({
        "log_id":           str(uuid.uuid4()),
        "pipeline_run_id":  pipeline_run_id,
        "ingestion_path":   "csv",
        "source_table":     TBL_CLINICAL_OBS,
        "record_count":     len(csv_obs_rows) + len(csv_unmapped_rows),
        "pass_count":       len(csv_obs_rows),
        "error_count":      len(csv_unmapped_rows),
        "tenant_id":        TENANT_ID,
        "run_started_at":   csv_labs_started,
        "run_completed_at": csv_labs_completed,
        "logged_at":        datetime.now(timezone.utc).replace(tzinfo=None),
    })

if audit_entries:
    audit_df_csv = spark.createDataFrame(audit_entries, schema=AUDIT_INGEST_LOG_SCHEMA)
    audit_df_csv.write.format("delta").mode("append").insertInto(BRONZE_AUDIT_TABLE)
    print(f"Wrote {len(audit_entries)} audit log entry/entries to {BRONZE_AUDIT_TABLE}")
else:
    print("No batch rows found — audit log not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_mpi      = len(csv_mpi_rows)
total_xwalk    = len(csv_xwalk_rows)
total_patients = len(csv_patient_rows)
total_cond     = len(csv_condition_rows)
total_obs      = len(csv_obs_rows)
total_unmapped = len(csv_unmapped_rows)
total_val_errs = len(csv_validation_errs)

print("=" * 60)
print(f"Bronze → Silver CSV complete  |  pipeline_run_id: {pipeline_run_id}")
print("=" * 60)
print(f"  mpi_patient_index       : {total_mpi} new UMPI(s)")
print(f"  mpi_identity_crosswalk  : {total_xwalk} source link(s)")
print(f"  clinical_patients       : {total_patients} row(s)")
print(f"  clinical_conditions     : {total_cond} row(s)")
print(f"  clinical_observations   : {total_obs} row(s) (mapped only)")
print(f"  terminology_unmapped    : {total_unmapped} code(s)")
print(f"  validation_errors       : {total_val_errs} CSV DQ issue(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — preview Silver rows

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        cp.umpi,
        cp.last_name,
        cp.first_name,
        cp.date_of_birth,
        co.loinc_code,
        co.loinc_display,
        co.value_quantity,
        co.value_unit
    FROM {TBL_CLINICAL_PATIENTS} cp
    LEFT JOIN {TBL_CLINICAL_OBS} co ON cp.umpi = co.umpi
    WHERE cp.created_at >= '{now_ts}'
    ORDER BY co.loinc_code
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return pipeline_run_id to orchestrator

# COMMAND ----------

dbutils.notebook.exit(pipeline_run_id)
