# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Bronze → Silver Normalization
# MAGIC
# MAGIC **Purpose:** Read FHIR R4 Bundles from Bronze, split into resources, run identity
# MAGIC resolution and terminology normalization, and write normalized records to Silver CDM
# MAGIC tables. Also processes eClinicalWorks CSV flat files via the CSV ingestion path.
# MAGIC All MPI matching logic is delegated entirely to `transforms/identity_resolution.py`.
# MAGIC
# MAGIC **Processing order (enforced):**
# MAGIC 1. Seed in-memory MPIIndex from existing Silver records (idempotency)
# MAGIC 2. FHIR Patient resources → `mpi_patient_index` + `mpi_identity_crosswalk` + `clinical_patients`
# MAGIC 3. FHIR Observation resources → LOINC normalization → `clinical_observations` + `terminology_unmapped_codes`
# MAGIC 4. ECW CSV patients → `mpi_patient_index` + `mpi_identity_crosswalk` + `clinical_patients`
# MAGIC 5. ECW CSV labs → LOINC normalization → `clinical_observations` + `terminology_unmapped_codes`
# MAGIC
# MAGIC Unmapped codes land in `terminology_unmapped_codes` — nothing is dropped silently.
# MAGIC `clinical_observations.loinc_code` is NOT NULL; unmapped observations are skipped
# MAGIC from that table and captured in `terminology_unmapped_codes` for human review.
# MAGIC
# MAGIC **Reads from:** `dev.fhir_bronze.ingest_fhir_bundles`
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_silver.mpi_patient_index`
# MAGIC - `dev.fhir_silver.mpi_identity_crosswalk`
# MAGIC - `dev.fhir_silver.clinical_patients`
# MAGIC - `dev.fhir_silver.clinical_observations`
# MAGIC - `dev.fhir_silver.terminology_unmapped_codes`
# MAGIC - `dev.fhir_bronze.audit_ingest_log`
# MAGIC - `dev.fhir_bronze.audit_validation_errors`
# MAGIC
# MAGIC **Run order:** Notebook 03 of 04. Run after `01_ingest_hl7.py` and
# MAGIC `02_ingest_fhir.py`, before `04_silver_to_gold.py`.

# COMMAND ----------

import sys
import os
import csv
import uuid
import json
import re as _re
from datetime import datetime, timezone, date

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

TENANT_ID          = "INTEGRIS_BAPTIST"
SRC_CATALOG        = "dev"
SRC_SCHEMA         = "fhir_bronze"
TGT_CATALOG        = "dev"
TGT_SCHEMA         = "fhir_silver"

BRONZE_FHIR_TABLE  = f"{SRC_CATALOG}.{SRC_SCHEMA}.ingest_fhir_bundles"
NOTEBOOK_NAME      = "03_bronze_to_silver"
PIPELINE_VERSION   = "1.0.0"

TBL_MPI_PATIENTS      = f"{TGT_CATALOG}.{TGT_SCHEMA}.mpi_patient_index"
TBL_MPI_XWALK         = f"{TGT_CATALOG}.{TGT_SCHEMA}.mpi_identity_crosswalk"
TBL_CLINICAL_PATIENTS = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_patients"
TBL_CLINICAL_OBS      = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_observations"
TBL_UNMAPPED          = f"{TGT_CATALOG}.{TGT_SCHEMA}.terminology_unmapped_codes"

BRONZE_AUDIT_TABLE      = f"{SRC_CATALOG}.{SRC_SCHEMA}.audit_ingest_log"
BRONZE_VALIDATION_TABLE = f"{SRC_CATALOG}.{SRC_SCHEMA}.audit_validation_errors"

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"source          : {BRONZE_FHIR_TABLE}")
print(f"target catalog  : {TGT_CATALOG}.{TGT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget: upstream pipeline_run_id
# MAGIC
# MAGIC When run from the Databricks job, this widget receives the `pipeline_run_id`
# MAGIC from notebook 02 so only that run's Bronze rows are processed.
# MAGIC Leave blank to process all Bronze bundles.

# COMMAND ----------

dbutils.widgets.text(
    "upstream_pipeline_run_id",
    "",
    "Pipeline Run ID from notebook 02 (blank = all Bronze bundles)",
)
upstream_run_id = dbutils.widgets.get("upstream_pipeline_run_id").strip()

if upstream_run_id:
    bronze_filter = f"pipeline_run_id = '{upstream_run_id}'"
    print(f"Filtering Bronze by pipeline_run_id: {upstream_run_id}")
else:
    bronze_filter = "1=1"
    print("No upstream run ID — processing all Bronze FHIR bundles")

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
# MAGIC ## Create Silver catalog, schema, and target tables (idempotent DDL)
# MAGIC
# MAGIC Schema matches `databricks/fhir_pipeline_ddl.sql` exactly.
# MAGIC Do not add or rename columns here — the DDL file is the single source of truth.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TGT_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TGT_CATALOG}.{TGT_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_MPI_PATIENTS} (
        umpi                  STRING    NOT NULL  COMMENT 'Universal Master Patient Index — surrogate key assigned by MPI',
        resolution_method     STRING              COMMENT 'pass1_exact | pass2_ssn4_dob | pass3_name_dob | pass4_manual',
        first_resolved_at     TIMESTAMP           COMMENT 'When this UMPI was first created',
        last_updated_at       TIMESTAMP           COMMENT 'When linkage last changed',
        linked_record_count   BIGINT              COMMENT 'Number of source records linked to this UMPI',
        tenant_ids            ARRAY<STRING>       COMMENT 'All tenants contributing records to this UMPI',
        is_merged             BOOLEAN             COMMENT 'True if this UMPI was merged from a prior duplicate',
        merged_into_umpi      STRING              COMMENT 'If merged, the surviving UMPI'
    )
    USING DELTA
    COMMENT 'Master patient index. UMPI is the Silver surrogate key for all clinical tables.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_MPI_XWALK} (
        crosswalk_id      STRING    NOT NULL  COMMENT 'UUID',
        umpi              STRING    NOT NULL  COMMENT 'FK to mpi_patient_index',
        source_mrn        STRING    NOT NULL  COMMENT 'Medical record number at source facility',
        tenant_id         STRING    NOT NULL,
        source_system     STRING              COMMENT 'Epic | eClinicalWorks | Athena | etc.',
        facility_id       STRING              COMMENT 'NPI or internal facility code',
        match_confidence  DOUBLE              COMMENT '0.0–1.0 deterministic confidence score',
        created_at        TIMESTAMP NOT NULL,
        updated_at        TIMESTAMP
    )
    USING DELTA
    COMMENT 'Source MRN to UMPI crosswalk. Append-only — identity lineage must never be deleted.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_CLINICAL_PATIENTS} (
        patient_id         STRING    NOT NULL  COMMENT 'UUID (Silver internal)',
        umpi               STRING    NOT NULL  COMMENT 'FK to mpi_patient_index',
        first_name         STRING,
        last_name          STRING,
        date_of_birth      DATE,
        gender             STRING              COMMENT 'SNOMED-CT coded preferred',
        race               STRING              COMMENT 'OMB category',
        ethnicity          STRING              COMMENT 'OMB category',
        preferred_language STRING              COMMENT 'ISO 639-1 code',
        address_line1      STRING,
        address_line2      STRING,
        city               STRING,
        state              STRING              COMMENT '2-letter USPS abbreviation',
        zip                STRING,
        phone              STRING,
        email              STRING,
        tenant_id          STRING    NOT NULL,
        source_system      STRING,
        source_record_id   STRING              COMMENT 'FK back to Bronze source',
        created_at         TIMESTAMP NOT NULL,
        updated_at         TIMESTAMP
    )
    USING DELTA
    COMMENT 'Canonical patient demographics. One row per tenant_id + umpi combination.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_CLINICAL_OBS} (
        observation_id        STRING    NOT NULL  COMMENT 'UUID (Silver internal)',
        umpi                  STRING    NOT NULL,
        encounter_id          STRING              COMMENT 'FK to clinical_encounters (nullable for ambulatory)',
        loinc_code            STRING    NOT NULL  COMMENT 'Normalized LOINC code — never inferred',
        loinc_display         STRING              COMMENT 'LOINC long common name',
        value_quantity        DOUBLE              COMMENT 'Numeric result value',
        value_unit            STRING              COMMENT 'UCUM unit (e.g. %)',
        value_string          STRING              COMMENT 'Text result (when non-numeric)',
        value_codeable_code   STRING              COMMENT 'Coded result code',
        value_codeable_system STRING              COMMENT 'Coded result code system',
        reference_range_low   DOUBLE,
        reference_range_high  DOUBLE,
        interpretation        STRING              COMMENT 'H | L | N | A (HL7 ObsInterpretation)',
        observation_datetime  TIMESTAMP,
        status                STRING              COMMENT 'registered | preliminary | final | amended',
        tenant_id             STRING    NOT NULL,
        source_system         STRING,
        source_code           STRING              COMMENT 'Original source code before normalization',
        source_record_id      STRING,
        created_at            TIMESTAMP NOT NULL,
        updated_at            TIMESTAMP
    )
    USING DELTA
    COMMENT 'LOINC-normalized observations. Unmapped codes written to terminology_unmapped_codes — no silent drops.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_UNMAPPED} (
        unmapped_id      STRING    NOT NULL  COMMENT 'UUID',
        source_code      STRING    NOT NULL  COMMENT 'Original untranslated code',
        source_display   STRING              COMMENT 'Original display text',
        target_system    STRING    NOT NULL  COMMENT 'LOINC | SNOMED-CT | RxNorm | ICD-10',
        source_system    STRING              COMMENT 'eClinicalWorks | Epic | Athena | etc.',
        record_type      STRING              COMMENT 'observation | condition | medication | procedure | immunization',
        source_record_id STRING              COMMENT 'FK to clinical_* table record that triggered this',
        tenant_id        STRING    NOT NULL,
        pipeline_run_id  STRING,
        logged_at        TIMESTAMP NOT NULL,
        resolved         BOOLEAN,
        resolved_at      TIMESTAMP,
        resolved_by      STRING,
        resolved_mapping STRING              COMMENT 'The mapping applied at resolution',
        resolution_notes STRING
    )
    USING DELTA
    COMMENT 'Explicit unmapped terminology audit log. Every UNMAPPED code is written here. No silent drops ever.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_AUDIT_TABLE} (
        log_id            STRING    NOT NULL,
        pipeline_run_id   STRING    NOT NULL,
        ingestion_path    STRING,
        source_table      STRING,
        record_count      BIGINT,
        pass_count        BIGINT,
        error_count       BIGINT,
        tenant_id         STRING,
        run_started_at    TIMESTAMP,
        run_completed_at  TIMESTAMP,
        logged_at         TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Unified ingest audit trail across HL7, FHIR, and CSV paths.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_VALIDATION_TABLE} (
        error_id          STRING    NOT NULL,
        pipeline_run_id   STRING,
        ingestion_path    STRING,
        source_record_id  STRING,
        error_code        STRING,
        error_message     STRING,
        raw_payload       STRING,
        tenant_id         STRING,
        requires_review   BOOLEAN,
        reviewed_at       TIMESTAMP,
        reviewed_by       STRING,
        review_outcome    STRING,
        created_at        TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Validation failures requiring human review. Records here are blocked from Silver.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print("Silver tables verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Truncate stale data (development only)
# MAGIC
# MAGIC Comment out in production — Silver tables accumulate across runs and the MPI
# MAGIC seeding step handles idempotency.

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {TBL_MPI_PATIENTS}")
spark.sql(f"TRUNCATE TABLE {TBL_MPI_XWALK}")
spark.sql(f"TRUNCATE TABLE {TBL_CLINICAL_PATIENTS}")
spark.sql(f"TRUNCATE TABLE {TBL_CLINICAL_OBS}")

print("Silver tables truncated (development mode)")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {TGT_CATALOG}.{TGT_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed MPIIndex from existing Silver records (idempotency)
# MAGIC
# MAGIC Restores MPI state from `clinical_patients` (passes 3/4) and
# MAGIC `mpi_identity_crosswalk` (pass 1: MRN+NPI).
# MAGIC SSN4-based pass 3 seeding is skipped — `ssn_last4` is not stored in the DDL
# MAGIC Silver schema and is not available for cross-run restoration.

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
    # Pass 4: DOB + full name + postal code
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
    # Pass 1: MRN + facility NPI
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
# MAGIC ## Read Bronze FHIR bundles and split into resources
# MAGIC
# MAGIC `ingest_fhir_bundles` stores one row per Bundle with the full JSON in `raw_payload`.
# MAGIC Each bundle is parsed and its entries are split into per-resource dicts grouped by
# MAGIC resourceType for ordered processing.

# COMMAND ----------

bundle_rows = spark.sql(f"""
    SELECT bundle_id, tenant_id, raw_payload, pipeline_run_id AS bronze_run_id
    FROM {BRONZE_FHIR_TABLE}
    WHERE {bronze_filter}
""").collect()

# Group resources by type across all bundles
by_type = {}   # resourceType → list of {resource, bundle_id, tenant_id}
for row in bundle_rows:
    bundle_dict = json.loads(row["raw_payload"])
    for entry in bundle_dict.get("entry", []):
        resource = entry.get("resource", {})
        rt = resource.get("resourceType")
        if rt:
            by_type.setdefault(rt, []).append({
                "resource":  resource,
                "bundle_id": row["bundle_id"],
                "tenant_id": row["tenant_id"],
            })

print(f"Bronze bundles loaded: {len(bundle_rows)}")
for rt, items in sorted(by_type.items()):
    print(f"  {rt:<20} {len(items)} resource(s)")

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


def _parse_obs_datetime(s):
    """Convert ISO 8601 string to naive UTC datetime for TimestampType."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


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


def _csv_validation_error(code, field, detail, source_file, row_id, raw_val, run_id):
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
# MAGIC ## Pass 1 — Identity Resolution (FHIR Patient resources)
# MAGIC
# MAGIC Every Patient resource is resolved through `MPIIndex.resolve()`.
# MAGIC - `mpi_patient_index`: one row per NEW UMPI (UMPI registry metadata only)
# MAGIC - `mpi_identity_crosswalk`: one row per source → UMPI link
# MAGIC - `clinical_patients`: one row per patient per source (demographics)

# COMMAND ----------

now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

fhir_mpi_rows      = []   # → mpi_patient_index (new UMPIs only)
fhir_xwalk_rows    = []   # → mpi_identity_crosswalk
fhir_patient_rows  = []   # → clinical_patients
patient_umpi_map   = {}   # fhir resource id → umpi (for Observation linking)

for rec in by_type.get("Patient", []):
    resource    = rec["resource"]
    bundle_id   = rec["bundle_id"]
    fhir_id     = resource.get("id", str(uuid.uuid4()))
    source_rec_id = f"{bundle_id}/{fhir_id}"

    identity = fhir_patient_to_identity(
        resource=resource,
        tenant_id=TENANT_ID,
        source_table=BRONZE_FHIR_TABLE,
        source_id=source_rec_id,
    )
    result = mpi.resolve(identity)

    patient_umpi_map[fhir_id]               = result.umpi
    patient_umpi_map[f"urn:uuid:{fhir_id}"] = result.umpi

    print(f"  Patient {fhir_id}  →  umpi={result.umpi}  "
          f"method={result.match_method}  new={result.is_new_record}")

    # mpi_patient_index: DDL stores only UMPI registry metadata (no demographics)
    if result.is_new_record:
        fhir_mpi_rows.append({
            "umpi":                result.umpi,
            "resolution_method":   result.match_method,
            "first_resolved_at":   now_ts,
            "last_updated_at":     now_ts,
            "linked_record_count": 1,
            "tenant_ids":          [TENANT_ID],
            "is_merged":           False,
            "merged_into_umpi":    None,
        })

    # mpi_identity_crosswalk: source_mrn NOT NULL — use fhir_id as fallback
    fhir_xwalk_rows.append({
        "crosswalk_id":    str(uuid.uuid4()),
        "umpi":            result.umpi,
        "source_mrn":      identity.source_mrn or fhir_id,
        "tenant_id":       TENANT_ID,
        "source_system":   "FHIR_R4",
        "facility_id":     identity.source_facility_npi,
        "match_confidence": result.match_confidence,
        "created_at":      now_ts,
        "updated_at":      None,
    })

    # clinical_patients: demographics from the FHIR resource
    patient_rec = mpi.get_patient(result.umpi) or {}
    birth_date_obj, _ = _parse_iso_dob(patient_rec.get("birth_date"))
    addr_list  = resource.get("address", [])
    addr       = addr_list[0] if addr_list else {}
    name_list  = resource.get("name", [])
    name       = name_list[0] if name_list else {}
    telecom    = resource.get("telecom", [])
    phone      = next((t.get("value") for t in telecom if t.get("system") == "phone"), None)
    email      = next((t.get("value") for t in telecom if t.get("system") == "email"), None)
    addr_lines = addr.get("line", [])

    fhir_patient_rows.append({
        "patient_id":         str(uuid.uuid4()),
        "umpi":               result.umpi,
        "first_name":         name.get("given", [None])[0] if name.get("given") else patient_rec.get("given_name"),
        "last_name":          name.get("family") or patient_rec.get("family_name"),
        "date_of_birth":      birth_date_obj,
        "gender":             resource.get("gender") or patient_rec.get("gender"),
        "race":               None,
        "ethnicity":          None,
        "preferred_language": None,
        "address_line1":      addr_lines[0] if addr_lines else None,
        "address_line2":      addr_lines[1] if len(addr_lines) > 1 else None,
        "city":               addr.get("city"),
        "state":              addr.get("state"),
        "zip":                addr.get("postalCode") or patient_rec.get("postal_code"),
        "phone":              phone,
        "email":              email,
        "tenant_id":          TENANT_ID,
        "source_system":      "FHIR_R4",
        "source_record_id":   source_rec_id,
        "created_at":         now_ts,
        "updated_at":         None,
    })

print(f"\nFHIR patients resolved : {len(fhir_xwalk_rows)}")
print(f"New UMPIs minted       : {len(fhir_mpi_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write mpi_patient_index — FHIR new patients

# COMMAND ----------

from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DoubleType, LongType,
    StringType, StructField, StructType, TimestampType,
)

# Module-level constants — consumed by tests/test_contracts.py for DDL alignment checks.
# Names match databricks/fhir_pipeline_ddl.sql table names.  Do not rename.

INGEST_CSV_BATCHES_SCHEMA = StructType([
    StructField("batch_id",          StringType(),    False),  # NOT NULL
    StructField("raw_payload",       StringType(),    False),  # NOT NULL
    StructField("source_system",     StringType(),    True),
    StructField("batch_frequency",   StringType(),    True),
    StructField("file_name",         StringType(),    True),
    StructField("file_size_bytes",   LongType(),      True),
    StructField("row_count",         LongType(),      True),
    StructField("tenant_id",         StringType(),    False),  # NOT NULL
    StructField("received_at",       TimestampType(), False),  # NOT NULL
    StructField("validation_status", StringType(),    True),
    StructField("pipeline_run_id",   StringType(),    True),
])

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

if fhir_mpi_rows:
    mpi_df = spark.createDataFrame(fhir_mpi_rows, schema=MPI_PATIENT_INDEX_SCHEMA)
    mpi_df.write.format("delta").mode("append").insertInto(TBL_MPI_PATIENTS)
    print(f"Wrote {len(fhir_mpi_rows)} new UMPI row(s) to {TBL_MPI_PATIENTS}")
else:
    print("All FHIR patients matched existing UMPIs — mpi_patient_index not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write mpi_identity_crosswalk — FHIR source links

# COMMAND ----------

if fhir_xwalk_rows:
    xwalk_df = spark.createDataFrame(fhir_xwalk_rows, schema=MPI_IDENTITY_CROSSWALK_SCHEMA)
    xwalk_df.write.format("delta").mode("append").insertInto(TBL_MPI_XWALK)
    print(f"Wrote {len(fhir_xwalk_rows)} crosswalk row(s) to {TBL_MPI_XWALK}")
else:
    print("No FHIR Patient resources — crosswalk not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write clinical_patients — FHIR demographics

# COMMAND ----------

if fhir_patient_rows:
    cp_df = spark.createDataFrame(fhir_patient_rows, schema=CLINICAL_PATIENTS_SCHEMA)
    cp_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_PATIENTS)
    print(f"Wrote {len(fhir_patient_rows)} FHIR patient row(s) to {TBL_CLINICAL_PATIENTS}")
else:
    print("No FHIR Patient resources — clinical_patients not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 2 — FHIR Observation → LOINC normalization → clinical_observations
# MAGIC
# MAGIC `clinical_observations.loinc_code` is NOT NULL. Unmapped observations are
# MAGIC skipped from this table — they land in `terminology_unmapped_codes` only.
# MAGIC Nothing is dropped silently.

# COMMAND ----------

fhir_obs_rows     = []   # → clinical_observations (mapped only)
fhir_unmapped_rows = []  # → terminology_unmapped_codes

for rec in by_type.get("Observation", []):
    resource    = rec["resource"]
    bundle_id   = rec["bundle_id"]
    fhir_id     = resource.get("id", str(uuid.uuid4()))
    source_rec_id = f"{bundle_id}/{fhir_id}"

    subject_ref = resource.get("subject", {}).get("reference", "")
    fhir_pat_id = subject_ref.split("/")[-1].replace("urn:uuid:", "")
    umpi = patient_umpi_map.get(subject_ref) or patient_umpi_map.get(fhir_pat_id)

    if not umpi:
        print(f"  WARN: No UMPI for Observation subject '{subject_ref}' — skipping")
        continue

    # normalize_fhir_observation returns (SilverLabRecord, norm_log_entries)
    silver_rec, _ = normalize_fhir_observation(
        resource=resource,
        tenant_id=TENANT_ID,
        umpi=umpi,
        source_id=source_rec_id,
        terminology=terminology,
        encounter_silver_id=None,
    )

    if silver_rec.loinc_code:
        # Mapped — write to clinical_observations
        fhir_obs_rows.append({
            "observation_id":        silver_rec.observation_id,
            "umpi":                  umpi,
            "encounter_id":          None,
            "loinc_code":            silver_rec.loinc_code,
            "loinc_display":         silver_rec.loinc_display,
            "value_quantity":        silver_rec.value_quantity,
            "value_unit":            silver_rec.value_unit,
            "value_string":          silver_rec.value_string,
            "value_codeable_code":   None,
            "value_codeable_system": None,
            "reference_range_low":   silver_rec.reference_range_low,
            "reference_range_high":  silver_rec.reference_range_high,
            "interpretation":        silver_rec.interpretation_code,
            "observation_datetime":  _parse_obs_datetime(silver_rec.effective_datetime),
            "status":                silver_rec.observation_status,
            "tenant_id":             TENANT_ID,
            "source_system":         "FHIR_R4",
            "source_code":           silver_rec.source_code,
            "source_record_id":      source_rec_id,
            "created_at":            now_ts,
            "updated_at":            None,
        })
    else:
        # Unmapped — skip clinical_observations, write to terminology_unmapped_codes
        fhir_unmapped_rows.append(_unmapped_row(
            source_code=silver_rec.source_code,
            source_display=silver_rec.source_display,
            target_system="LOINC",
            source_system="FHIR_R4",
            record_type="observation",
            source_record_id=source_rec_id,
            run_id=pipeline_run_id,
            now_ts=now_ts,
        ))

    print(f"  Observation {fhir_id}  →  loinc={silver_rec.loinc_code}  "
          f"method={silver_rec.loinc_map_method}  value={silver_rec.value_quantity}")

print(f"\nFHIR observations built : {len(fhir_obs_rows)} mapped, "
      f"{len(fhir_unmapped_rows)} unmapped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write clinical_observations — FHIR mapped

# COMMAND ----------

if fhir_obs_rows:
    obs_df = spark.createDataFrame(fhir_obs_rows, schema=CLINICAL_OBSERVATIONS_SCHEMA)
    obs_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_OBS)
    print(f"Wrote {len(fhir_obs_rows)} FHIR observation row(s) to {TBL_CLINICAL_OBS}")
else:
    print("No FHIR Observation resources mapped — clinical_observations not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write terminology_unmapped_codes — FHIR unmapped

# COMMAND ----------

if fhir_unmapped_rows:
    unmapped_df = spark.createDataFrame(fhir_unmapped_rows, schema=TERMINOLOGY_UNMAPPED_CODES_SCHEMA)
    unmapped_df.write.format("delta").mode("append").insertInto(TBL_UNMAPPED)
    print(f"Wrote {len(fhir_unmapped_rows)} FHIR unmapped code(s) to {TBL_UNMAPPED}")
else:
    print("All FHIR observations mapped successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV Ingestion Path — eClinicalWorks Patients and Labs
# MAGIC
# MAGIC DQ issues detected and logged to `audit_validation_errors`:
# MAGIC - `CSV_BLANK_NAME`         — first_name or last_name is empty
# MAGIC - `CSV_MALFORMED_DOB`      — DOB not in YYYY-MM-DD format; date_of_birth → NULL
# MAGIC - `CSV_DUPLICATE_ROW`      — same patient_id seen more than once
# MAGIC - `CSV_INVALID_ICD10`      — primary_dx_icd10 produces no SNOMED mapping
# MAGIC - `CSV_UNMAPPED_LAB_CODE`  — test_code has no LOINC mapping
# MAGIC - `CSV_TEXT_RESULT_VALUE`  — result_value cannot be parsed as float

# COMMAND ----------

ECW_PATIENTS_FILE     = f"{REPO_ROOT}/data/synthetic/ecw_patients.csv"
ECW_LABS_FILE         = f"{REPO_ROOT}/data/synthetic/ecw_labs.csv"
ECW_IDENTIFIER_SYSTEM = "urn:system:eclinicalworks"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step CSV-1 — Process ecw_patients.csv

# COMMAND ----------

csv_mpi_rows      = []    # → mpi_patient_index (new UMPIs only)
csv_xwalk_rows    = []    # → mpi_identity_crosswalk
csv_patient_rows  = []    # → clinical_patients
csv_validation_errs = []  # → audit_validation_errors
ecw_patient_umpi_map = {} # ECW patient_id → umpi (for lab linkage)
seen_patient_ids  = {}    # duplicate detection

csv_patients_started = datetime.now(timezone.utc).replace(tzinfo=None)

with open(ECW_PATIENTS_FILE, newline="", encoding="utf-8") as fh:
    reader = csv.DictReader(fh)
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
                ECW_PATIENTS_FILE, patient_id, first_name, pipeline_run_id,
            ))
        if not last_name:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_BLANK_NAME", "last_name",
                "last_name is blank; MPI name-based passes may degrade",
                ECW_PATIENTS_FILE, patient_id, last_name, pipeline_run_id,
            ))

        # DQ: malformed DOB
        if dob_malformed:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_MALFORMED_DOB", "dob",
                "DOB is not in ISO 8601 format (expected YYYY-MM-DD); date_of_birth set to NULL",
                ECW_PATIENTS_FILE, patient_id, raw_dob, pipeline_run_id,
            ))

        # DQ: duplicate row
        if patient_id in seen_patient_ids:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_DUPLICATE_ROW", "patient_id",
                f"Duplicate patient_id first seen at row {seen_patient_ids[patient_id]}; "
                f"MPI will return same UMPI (DETERMINISTIC match)",
                ECW_PATIENTS_FILE, patient_id, patient_id, pipeline_run_id,
            ))
        else:
            seen_patient_ids[patient_id] = row_idx

        # MPI resolution
        identity = PatientIdentity(
            source_id=patient_id,
            source_table=ECW_PATIENTS_FILE,
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
            "crosswalk_id":    str(uuid.uuid4()),
            "umpi":            result.umpi,
            "source_mrn":      patient_id,
            "tenant_id":       TENANT_ID,
            "source_system":   "eClinicalWorks",
            "facility_id":     pcp_npi,
            "match_confidence": result.match_confidence,
            "created_at":      now_ts_csv,
            "updated_at":      None,
        })

        # DQ: invalid ICD-10
        if primary_dx and terminology.map_snomed_from_icd10(primary_dx) is None:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_INVALID_ICD10", "primary_dx_icd10",
                f"ICD-10 code '{primary_dx}' has no SNOMED mapping; written as-is",
                ECW_PATIENTS_FILE, patient_id, primary_dx, pipeline_run_id,
            ))

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

print(f"ECW patients processed : {len(csv_patient_rows):,} rows")
print(f"New UMPIs minted        : {len(csv_mpi_rows)}")
print(f"Crosswalk entries       : {len(csv_xwalk_rows)}")
print(f"Validation issues       : {len(csv_validation_errs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step CSV-2 — Write ECW patient rows to Silver

# COMMAND ----------

if csv_patient_rows:
    cp_csv_df = spark.createDataFrame(csv_patient_rows, schema=CLINICAL_PATIENTS_SCHEMA)
    cp_csv_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_PATIENTS)
    print(f"Wrote {cp_csv_df.count():,} ECW patient row(s) to {TBL_CLINICAL_PATIENTS}")

if csv_mpi_rows:
    mpi_csv_df = spark.createDataFrame(csv_mpi_rows, schema=MPI_PATIENT_INDEX_SCHEMA)
    mpi_csv_df.write.format("delta").mode("append").insertInto(TBL_MPI_PATIENTS)
    print(f"Wrote {len(csv_mpi_rows)} new UMPI(s) to {TBL_MPI_PATIENTS}")

if csv_xwalk_rows:
    xwalk_csv_df = spark.createDataFrame(csv_xwalk_rows, schema=MPI_IDENTITY_CROSSWALK_SCHEMA)
    xwalk_csv_df.write.format("delta").mode("append").insertInto(TBL_MPI_XWALK)
    print(f"Wrote {len(csv_xwalk_rows)} crosswalk row(s) to {TBL_MPI_XWALK}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step CSV-3 — Process ecw_labs.csv

# COMMAND ----------

csv_obs_rows     = []   # → clinical_observations (mapped only)
csv_unmapped_rows = []  # → terminology_unmapped_codes
csv_lab_val_errs  = []  # collected here, appended to csv_validation_errs below

csv_labs_started = datetime.now(timezone.utc).replace(tzinfo=None)

with open(ECW_LABS_FILE, newline="", encoding="utf-8") as fh:
    reader = csv.DictReader(fh)
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
                ECW_LABS_FILE, result_id, raw_value, pipeline_run_id,
            ))

        # observation_datetime from collection_date (date → midnight timestamp)
        obs_dt = None
        if collect_date:
            try:
                obs_dt = datetime.combine(date.fromisoformat(collect_date), datetime.min.time())
            except (ValueError, TypeError):
                obs_dt = None

        if loinc_code:
            # Mapped — write to clinical_observations
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
            # Unmapped — skip clinical_observations
            csv_lab_val_errs.append(_csv_validation_error(
                "CSV_UNMAPPED_LAB_CODE", "test_code",
                f"test_code '{test_code}' (test_name='{test_name}') has no LOINC mapping; "
                f"entry written to terminology_unmapped_codes",
                ECW_LABS_FILE, result_id, test_code, pipeline_run_id,
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
# MAGIC ### Step CSV-4 — Write ECW lab rows to Silver

# COMMAND ----------

if csv_obs_rows:
    obs_csv_df = spark.createDataFrame(csv_obs_rows, schema=CLINICAL_OBSERVATIONS_SCHEMA)
    obs_csv_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_OBS)
    print(f"Wrote {obs_csv_df.count():,} ECW observation row(s) to {TBL_CLINICAL_OBS}")

all_unmapped = fhir_unmapped_rows + csv_unmapped_rows
if csv_unmapped_rows:
    unmapped_csv_df = spark.createDataFrame(csv_unmapped_rows, schema=TERMINOLOGY_UNMAPPED_CODES_SCHEMA)
    unmapped_csv_df.write.format("delta").mode("append").insertInto(TBL_UNMAPPED)
    print(f"Wrote {len(csv_unmapped_rows)} ECW unmapped code(s) to {TBL_UNMAPPED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step CSV-5 — Write validation errors and audit log

# COMMAND ----------

if csv_validation_errs:
    val_df_csv = spark.createDataFrame(csv_validation_errs, schema=AUDIT_VALIDATION_ERRORS_SCHEMA)
    val_df_csv.write.format("delta").mode("append").insertInto(BRONZE_VALIDATION_TABLE)
    print(f"Wrote {val_df_csv.count():,} CSV validation error(s) to {BRONZE_VALIDATION_TABLE}")
else:
    print("No CSV validation errors")

csv_audit_entries = [
    {
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
    },
    {
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
    },
]

audit_df_csv = spark.createDataFrame(csv_audit_entries, schema=AUDIT_INGEST_LOG_SCHEMA)
audit_df_csv.write.format("delta").mode("append").insertInto(BRONZE_AUDIT_TABLE)
print(f"CSV audit entries written to {BRONZE_AUDIT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_mpi    = len(fhir_mpi_rows) + len(csv_mpi_rows)
total_xwalk  = len(fhir_xwalk_rows) + len(csv_xwalk_rows)
total_patients = len(fhir_patient_rows) + len(csv_patient_rows)
total_obs    = len(fhir_obs_rows) + len(csv_obs_rows)
total_unmapped = len(fhir_unmapped_rows) + len(csv_unmapped_rows)

print("=" * 60)
print(f"Bronze → Silver complete  |  pipeline_run_id: {pipeline_run_id}")
print("=" * 60)
print(f"  mpi_patient_index       : {total_mpi} new UMPI(s)")
print(f"  mpi_identity_crosswalk  : {total_xwalk} source link(s)")
print(f"  clinical_patients       : {total_patients} row(s)")
print(f"  clinical_observations   : {total_obs} row(s) (mapped only)")
print(f"  terminology_unmapped    : {total_unmapped} code(s)")
print(f"  validation_errors       : {len(csv_validation_errs)} CSV DQ issue(s)")

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
        cp.source_system,
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

display(spark.sql(f"""
    SELECT
        crosswalk_id,
        umpi,
        source_mrn,
        source_system,
        facility_id,
        match_confidence
    FROM {TBL_MPI_XWALK}
    WHERE created_at >= '{now_ts}'
    ORDER BY umpi
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return pipeline_run_id to orchestrator

# COMMAND ----------

dbutils.notebook.exit(pipeline_run_id)
