# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Bronze → Silver Normalization
# MAGIC
# MAGIC **Purpose:** Read FHIR R4 resource rows from Bronze, run identity resolution
# MAGIC and terminology normalization, and write normalized records to Silver CDM tables.
# MAGIC All MPI matching logic is delegated entirely to `transforms/identity_resolution.py`.
# MAGIC
# MAGIC **Processing order (enforced):**
# MAGIC 1. Seed in-memory MPIIndex from existing Silver records (idempotency)
# MAGIC 2. Patient resources → MPIIndex.resolve() → `mpi_patient_index` + `mpi_identity_crosswalk`
# MAGIC 3. Encounter resources → `encounters`
# MAGIC 4. Observation resources → LOINC normalization → `lab_observations` + `normalization_log`
# MAGIC 5. Condition resources → SNOMED dual-coding → `diagnoses`
# MAGIC
# MAGIC Unmapped terminology codes land in `terminology_unmapped_codes` — nothing is dropped.
# MAGIC Every mapping (mapped or UNMAPPED) is written to `normalization_log`.
# MAGIC
# MAGIC **Reads from:** `dev.fhir_bronze.fhir_resources`
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_silver.mpi_patient_index` — one row per unique UMPI (new patients only)
# MAGIC - `dev.fhir_silver.mpi_identity_crosswalk` — one row per source_id → UMPI mapping
# MAGIC - `dev.fhir_silver.encounters`
# MAGIC - `dev.fhir_silver.lab_observations`
# MAGIC - `dev.fhir_silver.diagnoses`
# MAGIC - `dev.fhir_silver.normalization_log`
# MAGIC - `dev.fhir_silver.terminology_unmapped_codes`
# MAGIC
# MAGIC **Run order:** Notebook 03 of 05. Run after `01_ingest_hl7.py` and
# MAGIC `02_ingest_fhir.py`, before `04_silver_to_gold.py`.

# COMMAND ----------

import sys
import uuid
import json
from datetime import datetime, timezone, date
from dataclasses import asdict

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

TENANT_ID          = "INTEGRIS_BAPTIST"
SRC_CATALOG        = "dev"
SRC_SCHEMA         = "fhir_bronze"
TGT_CATALOG        = "dev"
TGT_SCHEMA         = "fhir_silver"
BRONZE_FHIR_TABLE  = f"{SRC_CATALOG}.{SRC_SCHEMA}.fhir_resources"
NOTEBOOK_NAME      = "03_bronze_to_silver"
PIPELINE_VERSION   = "1.0.0"

TBL_MPI_PATIENTS   = f"{TGT_CATALOG}.{TGT_SCHEMA}.mpi_patient_index"
TBL_MPI_XWALK      = f"{TGT_CATALOG}.{TGT_SCHEMA}.mpi_identity_crosswalk"
TBL_ENCOUNTERS     = f"{TGT_CATALOG}.{TGT_SCHEMA}.encounters"
TBL_LAB_OBS        = f"{TGT_CATALOG}.{TGT_SCHEMA}.lab_observations"
TBL_DIAGNOSES      = f"{TGT_CATALOG}.{TGT_SCHEMA}.diagnoses"
TBL_NORM_LOG            = f"{TGT_CATALOG}.{TGT_SCHEMA}.normalization_log"
TBL_UNMAPPED            = f"{TGT_CATALOG}.{TGT_SCHEMA}.terminology_unmapped_codes"
TBL_CLINICAL_PATIENTS   = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_patients"
TBL_CLINICAL_OBS        = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_observations"
BRONZE_AUDIT_TABLE      = f"{SRC_CATALOG}.{SRC_SCHEMA}.audit_ingest_log"
BRONZE_VALIDATION_TABLE = f"{SRC_CATALOG}.{SRC_SCHEMA}.audit_validation_errors"

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"source          : {BRONZE_FHIR_TABLE}")
print(f"target catalog  : {TGT_CATALOG}.{TGT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget: upstream pipeline_run_id
# MAGIC
# MAGIC When run from the orchestrator (`00_run_pipeline.py`), this widget receives
# MAGIC the `pipeline_run_id` from notebook 02 so only that run's PENDING rows are
# MAGIC processed. Leave blank to process all PENDING rows in the Bronze table.

# COMMAND ----------

dbutils.widgets.text(
    "upstream_pipeline_run_id",
    "",
    "Pipeline Run ID from notebook 02 (blank = all PENDING rows)",
)
upstream_run_id = dbutils.widgets.get("upstream_pipeline_run_id").strip()

if upstream_run_id:
    bronze_filter = f"pipeline_run_id = '{upstream_run_id}' AND processing_status = 'PENDING'"
    print(f"Filtering Bronze by pipeline_run_id: {upstream_run_id}")
else:
    bronze_filter = "processing_status = 'PENDING'"
    print("No upstream run ID — processing all PENDING Bronze rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve repo root and import transform modules
# MAGIC
# MAGIC All MPI matching logic lives in `transforms/identity_resolution.py` and is
# MAGIC covered by 41 unit tests. This notebook is an orchestration layer only —
# MAGIC it calls `MPIIndex.resolve()` and writes the results. It does not implement
# MAGIC any matching logic itself.

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
print("MPI: 4-pass deterministic matching via MPIIndex (identity_resolution.py)")
print("  Pass 1: exact MRN + facility NPI")
print("  Pass 2: identifier system + value")
print("  Pass 3: SSN-4 + DOB + family name")
print("  Pass 4: DOB + full name + postal code")
print("  No match → NEW_RECORD, new UMPI minted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver catalog, schema, and target tables (idempotent DDL)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TGT_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TGT_CATALOG}.{TGT_SCHEMA}")

# ── mpi_patient_index — one row per unique UMPI ───────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_MPI_PATIENTS} (
        umpi                STRING  NOT NULL COMMENT 'Universal Master Patient Index — UUID',
        tenant_id           STRING  NOT NULL COMMENT 'Tenant that first created this record',
        match_method        STRING  NOT NULL COMMENT 'NEW_RECORD (first-seen patients only)',
        match_confidence    DOUBLE  COMMENT '0.0 for new records',
        family_name         STRING,
        given_name          STRING,
        birth_date          STRING  COMMENT 'ISO 8601 date string (YYYY-MM-DD)',
        gender              STRING,
        postal_code         STRING,
        ssn_last4           STRING  COMMENT 'Last 4 of SSN only — never full SSN',
        source_tenant_id    STRING,
        source_table        STRING  COMMENT 'Bronze table that provided the Patient resource',
        source_id           STRING  COMMENT 'Bronze resource_id for this patient (first seen)',
        pipeline_run_id     STRING  NOT NULL,
        created_ts          TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'One row per unique UMPI. Written only when a new patient is minted. Never updated.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── mpi_identity_crosswalk — one row per source_id → UMPI mapping ─────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_MPI_XWALK} (
        crosswalk_id                STRING  NOT NULL COMMENT 'UUID for this crosswalk row',
        umpi                        STRING  NOT NULL COMMENT 'Resolved UMPI',
        tenant_id                   STRING  NOT NULL,
        source_table                STRING  NOT NULL COMMENT 'Bronze table',
        source_id                   STRING  NOT NULL COMMENT 'Bronze resource_id',
        source_mrn                  STRING  COMMENT 'MRN from source system identifier',
        source_facility_npi         STRING  COMMENT 'Facility NPI — populated from HL7 MSH',
        source_identifier_system    STRING  COMMENT 'FHIR identifier.system (e.g. urn:oid:…)',
        source_identifier_value     STRING  COMMENT 'FHIR identifier.value (MRN value)',
        match_method                STRING  COMMENT 'DETERMINISTIC | NEW_RECORD',
        matched_on                  STRING  COMMENT 'Comma-separated list of matched fields',
        pipeline_run_id             STRING  NOT NULL,
        created_ts                  TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'One row per source_id → UMPI link. Written on every run including repeat runs.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── encounters ────────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_ENCOUNTERS} (
        encounter_id            STRING  NOT NULL,
        tenant_id               STRING  NOT NULL,
        umpi                    STRING  NOT NULL,
        source_encounter_id     STRING,
        encounter_class         STRING  COMMENT 'IMP, AMB, EMER, etc.',
        period_start            STRING  COMMENT 'ISO 8601 datetime',
        period_end              STRING  COMMENT 'ISO 8601 datetime',
        facility_name           STRING,
        encounter_status        STRING,
        source_table            STRING,
        source_id               STRING,
        pipeline_run_id         STRING  NOT NULL,
        created_ts              TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Normalized encounter records. One row per encounter per tenant.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── lab_observations ──────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_LAB_OBS} (
        observation_id          STRING  NOT NULL,
        tenant_id               STRING  NOT NULL,
        umpi                    STRING  NOT NULL,
        encounter_id            STRING,
        loinc_code              STRING  COMMENT 'Canonical LOINC code; NULL if unmapped',
        loinc_display           STRING,
        source_code             STRING,
        source_code_system      STRING,
        source_display          STRING,
        observation_status      STRING,
        value_quantity          DOUBLE,
        value_unit              STRING,
        value_string            STRING,
        interpretation_code     STRING,
        interpretation_display  STRING,
        reference_range_low     DOUBLE,
        reference_range_high    DOUBLE,
        reference_range_unit    STRING,
        effective_datetime      STRING,
        issued_datetime         STRING,
        loinc_mapped            BOOLEAN NOT NULL,
        loinc_map_method        STRING  NOT NULL COMMENT 'SOURCE_LOINC | TERMINOLOGY_SERVICE | UNMAPPED',
        source_table            STRING,
        source_id               STRING,
        pipeline_run_id         STRING  NOT NULL,
        created_ts              TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Normalized lab observations. LOINC is the canonical code system.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── diagnoses ─────────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_DIAGNOSES} (
        diagnosis_id            STRING  NOT NULL,
        tenant_id               STRING  NOT NULL,
        umpi                    STRING  NOT NULL,
        encounter_id            STRING,
        icd10_code              STRING,
        icd10_display           STRING,
        snomed_code             STRING  COMMENT 'Dual-coded from ICD-10 via SNOMED map',
        snomed_display          STRING,
        source_code             STRING,
        source_code_system      STRING,
        source_display          STRING,
        diagnosis_rank          INTEGER,
        clinical_status         STRING,
        onset_datetime          STRING,
        source_table            STRING,
        source_id               STRING,
        pipeline_run_id         STRING  NOT NULL,
        created_ts              TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Normalized diagnoses. ICD-10 primary, SNOMED-CT dual-coded.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── normalization_log ─────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_NORM_LOG} (
        log_id              STRING  NOT NULL,
        tenant_id           STRING  NOT NULL,
        source_table        STRING  NOT NULL,
        source_id           STRING  NOT NULL,
        target_table        STRING  NOT NULL,
        target_id           STRING,
        mapping_type        STRING  NOT NULL COMMENT 'LOINC_MAP | SNOMED_MAP | RXNORM_MAP | UNMAPPED',
        source_value        STRING,
        source_system       STRING,
        mapped_value        STRING  COMMENT 'NULL when mapping_method = UNMAPPED',
        mapped_system       STRING,
        mapping_confidence  DOUBLE,
        mapping_method      STRING  COMMENT 'SOURCE_LOINC | TERMINOLOGY_SERVICE | UNMAPPED',
        pipeline_version    STRING,
        pipeline_run_id     STRING  NOT NULL,
        processed_ts        TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Audit trail for every terminology mapping applied in Silver.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── terminology_unmapped_codes ────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_UNMAPPED} (
        record_id           STRING  NOT NULL,
        pipeline_run_id     STRING  NOT NULL,
        tenant_id           STRING  NOT NULL,
        source_table        STRING  NOT NULL,
        source_id           STRING  NOT NULL,
        fhir_resource_type  STRING  NOT NULL COMMENT 'Observation | Condition | MedicationRequest',
        target_terminology  STRING  NOT NULL COMMENT 'LOINC | SNOMED | RXNORM',
        source_code         STRING,
        source_code_system  STRING,
        source_display      STRING,
        created_ts          TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Unmapped terminology codes. Query this table to identify gaps in the terminology service.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── clinical_patients (CSV/ECW source) ───────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_CLINICAL_PATIENTS} (
        patient_id          STRING    NOT NULL COMMENT 'UUID for this Silver patient row',
        tenant_id           STRING    NOT NULL,
        umpi                STRING    NOT NULL COMMENT 'Resolved UMPI from MPIIndex',
        source_row_id       STRING    COMMENT 'patient_id from CSV (e.g. ECW-00125)',
        first_name          STRING,
        last_name           STRING,
        dob                 STRING    COMMENT 'ISO 8601 YYYY-MM-DD; NULL if parse fails',
        gender              STRING,
        ssn_last4           STRING,
        address             STRING,
        city                STRING,
        state               STRING,
        zip                 STRING,
        phone               STRING,
        pcp_npi             STRING,
        insurance_id        STRING,
        last_visit_date     STRING,
        primary_dx_icd10    STRING,
        mpi_match_method    STRING    COMMENT 'DETERMINISTIC | NEW_RECORD',
        source_table        STRING    COMMENT 'Source CSV file path',
        pipeline_run_id     STRING    NOT NULL,
        created_ts          TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Normalized patient records from CSV ingestion path (eClinicalWorks).'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── clinical_observations (CSV/ECW source) ────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_CLINICAL_OBS} (
        observation_id          STRING    NOT NULL COMMENT 'UUID for this Silver observation row',
        tenant_id               STRING    NOT NULL,
        umpi                    STRING    NOT NULL COMMENT 'Resolved UMPI',
        source_result_id        STRING    COMMENT 'result_id from CSV (e.g. ECW-LAB-000001)',
        source_patient_id       STRING    COMMENT 'patient_id from CSV (e.g. ECW-00125)',
        test_code               STRING    COMMENT 'Original local test code from CSV',
        test_name               STRING    COMMENT 'Original test display name from CSV',
        loinc_code              STRING    COMMENT 'Canonical LOINC code; NULL if unmapped',
        loinc_display           STRING,
        loinc_mapped            BOOLEAN   NOT NULL,
        loinc_map_method        STRING    NOT NULL COMMENT 'SOURCE_LOINC | TERMINOLOGY_SERVICE | UNMAPPED',
        result_value_raw        STRING    COMMENT 'Raw result_value from CSV',
        value_quantity          DOUBLE    COMMENT 'NULL when result_value is text or unparseable',
        value_unit              STRING,
        reference_range_low     STRING,
        reference_range_high    STRING,
        abnormal_flag           STRING,
        collection_date         STRING    COMMENT 'ISO 8601 date',
        ordering_provider_npi   STRING,
        result_status           STRING,
        source_table            STRING    COMMENT 'Source CSV file path',
        pipeline_run_id         STRING    NOT NULL,
        created_ts              TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Normalized lab observations from CSV ingestion path (eClinicalWorks).'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print("Silver tables verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Truncate stale data (development only)
# MAGIC
# MAGIC These TRUNCATE statements reset the MPI tables so each development run starts
# MAGIC clean. Comment them out in production — Silver tables accumulate across runs
# MAGIC and the seeding step (next cell) handles idempotency.

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {TBL_MPI_PATIENTS}")
spark.sql(f"TRUNCATE TABLE {TBL_MPI_XWALK}")
spark.sql(f"TRUNCATE TABLE {TBL_CLINICAL_PATIENTS}")
spark.sql(f"TRUNCATE TABLE {TBL_CLINICAL_OBS}")

print("Silver tables truncated (development mode)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify table names

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {TGT_CATALOG}.{TGT_SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed MPIIndex from existing Silver records (idempotency)
# MAGIC
# MAGIC Load previously resolved UMPIs from `mpi_patient_index` and
# MAGIC `mpi_identity_crosswalk` into the in-memory MPIIndex before processing
# MAGIC new Bronze rows. This ensures that patients seen in earlier runs are
# MAGIC matched to their existing UMPI instead of receiving a new one.
# MAGIC
# MAGIC Restores all four matching passes where stored data allows:
# MAGIC - Pass 1 (MRN+NPI) and Pass 2 (identifier system+value) from crosswalk
# MAGIC - Pass 3 (SSN4+DOB+name) and Pass 4 (DOB+name+zip) from mpi_patient_index

# COMMAND ----------

existing_patients = spark.sql(f"""
    SELECT umpi, tenant_id, family_name, given_name, birth_date,
           gender, postal_code, ssn_last4
    FROM {TBL_MPI_PATIENTS}
""").collect()

existing_xwalk = spark.sql(f"""
    SELECT umpi, source_mrn, source_facility_npi,
           source_identifier_system, source_identifier_value
    FROM {TBL_MPI_XWALK}
""").collect()

# Restore _umpi_records and demographic indexes from mpi_patient_index
for r in existing_patients:
    umpi = r["umpi"]
    mpi._umpi_records[umpi] = {
        "umpi":        umpi,
        "tenant_id":   r["tenant_id"],
        "family_name": r["family_name"],
        "given_name":  r["given_name"],
        "birth_date":  r["birth_date"],
        "gender":      r["gender"],
        "postal_code": r["postal_code"],
        "ssn_last4":   r["ssn_last4"],
    }
    # Restore Pass 3: SSN4 + DOB + family name
    if r["ssn_last4"] and r["birth_date"] and r["family_name"]:
        key = (r["ssn_last4"], r["birth_date"], mpi._normalize_name(r["family_name"]))
        mpi._ssn4_dob_name_index[key] = umpi
    # Restore Pass 4: DOB + full name + postal code
    if r["birth_date"] and r["family_name"] and r["given_name"] and r["postal_code"]:
        key = (
            r["birth_date"],
            mpi._normalize_name(r["family_name"]),
            mpi._normalize_name(r["given_name"]),
            r["postal_code"],
        )
        mpi._dob_name_zip_index[key] = umpi

# Restore Pass 1 (MRN+NPI) and Pass 2 (identifier system+value) from crosswalk
for r in existing_xwalk:
    umpi = r["umpi"]
    if r["source_mrn"] and r["source_facility_npi"]:
        key = (r["source_mrn"], r["source_facility_npi"])
        mpi._mrn_npi_index[key] = umpi
    if r["source_identifier_system"] and r["source_identifier_value"]:
        key = (r["source_identifier_system"], r["source_identifier_value"])
        mpi._identifier_index[key] = umpi

print(f"MPIIndex seeded from existing Silver records:")
print(f"  mpi_patient_index rows  : {len(existing_patients)}")
print(f"  mpi_identity_crosswalk  : {len(existing_xwalk)}")
print(f"  _umpi_records           : {len(mpi._umpi_records)}")
print(f"  _identifier_index       : {len(mpi._identifier_index)}")
print(f"  _ssn4_dob_name_index    : {len(mpi._ssn4_dob_name_index)}")
print(f"  _dob_name_zip_index     : {len(mpi._dob_name_zip_index)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze FHIR resources

# COMMAND ----------

bronze_df = spark.sql(f"""
    SELECT
        resource_id,
        tenant_id,
        bundle_id,
        fhir_resource_type,
        fhir_resource_id,
        raw_payload,
        pipeline_run_id AS bronze_pipeline_run_id
    FROM {BRONZE_FHIR_TABLE}
    WHERE {bronze_filter}
""")

bronze_rows = bronze_df.collect()

# Group by resource type for ordered processing
by_type = {}
for row in bronze_rows:
    rt = row["fhir_resource_type"]
    by_type.setdefault(rt, []).append(row)

print(f"Bronze rows loaded: {len(bronze_rows)}")
for rt, rows in sorted(by_type.items()):
    print(f"  {rt:<20} {len(rows)} row(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 1 — Identity Resolution (Patient resources)
# MAGIC
# MAGIC Every Patient resource is resolved through `MPIIndex.resolve()` which implements
# MAGIC the full 4-pass deterministic matching hierarchy from `identity_resolution.py`.
# MAGIC No matching logic is implemented in this notebook.
# MAGIC
# MAGIC Write pattern:
# MAGIC - `mpi_patient_index`: written only when `is_new_record=True` (one row per unique UMPI)
# MAGIC - `mpi_identity_crosswalk`: written for every source Bronze row (tracks all source → UMPI links)

# COMMAND ----------

now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

mpi_patient_rows  = []   # only new UMPIs — written to mpi_patient_index
crosswalk_rows    = []   # all resolutions — written to mpi_identity_crosswalk
patient_umpi_map  = {}   # fhir_resource_id → umpi (for Encounter/Obs/Condition linking)
bronze_to_umpi    = {}   # bronze resource_id → umpi

for row in by_type.get("Patient", []):
    resource    = json.loads(row["raw_payload"])
    resource_id = row["resource_id"]

    identity = fhir_patient_to_identity(
        resource=resource,
        tenant_id=TENANT_ID,
        source_table=BRONZE_FHIR_TABLE,
        source_id=resource_id,
    )

    result = mpi.resolve(identity)

    # Map both bare FHIR ID and urn:uuid: form for subject.reference resolution
    fhir_id = resource.get("id", "")
    patient_umpi_map[fhir_id]               = result.umpi
    patient_umpi_map[f"urn:uuid:{fhir_id}"] = result.umpi
    bronze_to_umpi[resource_id]             = result.umpi

    print(f"  Patient {fhir_id}  →  umpi={result.umpi}  "
          f"method={result.match_method}  new={result.is_new_record}")

    # mpi_patient_index: only write when this is a genuinely new patient
    if result.is_new_record:
        patient_rec = mpi.get_patient(result.umpi) or {}
        mpi_patient_rows.append({
            "umpi":             result.umpi,
            "tenant_id":        TENANT_ID,
            "match_method":     result.match_method,
            "match_confidence": result.match_confidence,
            "family_name":      patient_rec.get("family_name"),
            "given_name":       patient_rec.get("given_name"),
            "birth_date":       patient_rec.get("birth_date"),
            "gender":           patient_rec.get("gender"),
            "postal_code":      patient_rec.get("postal_code"),
            "ssn_last4":        patient_rec.get("ssn_last4"),
            "source_tenant_id": TENANT_ID,
            "source_table":     BRONZE_FHIR_TABLE,
            "source_id":        resource_id,
            "pipeline_run_id":  pipeline_run_id,
            "created_ts":       now_ts,
        })

    # mpi_identity_crosswalk: always write (tracks every source → UMPI link)
    crosswalk_rows.append({
        "crosswalk_id":             str(uuid.uuid4()),
        "umpi":                     result.umpi,
        "tenant_id":                TENANT_ID,
        "source_table":             BRONZE_FHIR_TABLE,
        "source_id":                resource_id,
        "source_mrn":               identity.source_mrn,
        "source_facility_npi":      identity.source_facility_npi,
        "source_identifier_system": identity.source_identifier_system,
        "source_identifier_value":  identity.source_mrn,
        "match_method":             result.match_method,
        "matched_on":               ",".join(result.matched_on) if result.matched_on else None,
        "pipeline_run_id":          pipeline_run_id,
        "created_ts":               now_ts,
    })

print(f"\nMPI resolved   : {len(crosswalk_rows)} patient(s)")
print(f"New UMPIs minted : {len(mpi_patient_rows)}")
print(f"Existing matches : {len(crosswalk_rows) - len(mpi_patient_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write mpi_patient_index (new patients only)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

mpi_patients_schema = StructType([
    StructField("umpi",             StringType(),    False),
    StructField("tenant_id",        StringType(),    False),
    StructField("match_method",     StringType(),    False),
    StructField("match_confidence", DoubleType(),    True),
    StructField("family_name",      StringType(),    True),
    StructField("given_name",       StringType(),    True),
    StructField("birth_date",       StringType(),    True),
    StructField("gender",           StringType(),    True),
    StructField("postal_code",      StringType(),    True),
    StructField("ssn_last4",        StringType(),    True),
    StructField("source_tenant_id", StringType(),    True),
    StructField("source_table",     StringType(),    True),
    StructField("source_id",        StringType(),    True),
    StructField("pipeline_run_id",  StringType(),    False),
    StructField("created_ts",       TimestampType(), False),
])

if mpi_patient_rows:
    mpi_df = spark.createDataFrame(mpi_patient_rows, schema=mpi_patients_schema)
    mpi_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TBL_MPI_PATIENTS)
    print(f"Wrote {len(mpi_patient_rows)} new UMPI row(s) to {TBL_MPI_PATIENTS}")
else:
    print("All patients matched existing UMPIs — mpi_patient_index not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write mpi_identity_crosswalk (every source → UMPI link)

# COMMAND ----------

xwalk_schema = StructType([
    StructField("crosswalk_id",             StringType(),    False),
    StructField("umpi",                     StringType(),    False),
    StructField("tenant_id",                StringType(),    False),
    StructField("source_table",             StringType(),    False),
    StructField("source_id",               StringType(),    False),
    StructField("source_mrn",              StringType(),    True),
    StructField("source_facility_npi",     StringType(),    True),
    StructField("source_identifier_system", StringType(),   True),
    StructField("source_identifier_value", StringType(),    True),
    StructField("match_method",            StringType(),    True),
    StructField("matched_on",              StringType(),    True),
    StructField("pipeline_run_id",         StringType(),    False),
    StructField("created_ts",             TimestampType(), False),
])

if crosswalk_rows:
    xwalk_df = spark.createDataFrame(crosswalk_rows, schema=xwalk_schema)
    xwalk_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TBL_MPI_XWALK)
    print(f"Wrote {len(crosswalk_rows)} crosswalk row(s) to {TBL_MPI_XWALK}")
else:
    print("No Patient resources found — crosswalk not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 2a — Encounters
# MAGIC
# MAGIC Encounter resources are linked to the resolved UMPI via the subject reference.

# COMMAND ----------

encounter_rows    = []
encounter_id_map  = {}   # fhir Encounter id → silver encounter_id

for row in by_type.get("Encounter", []):
    resource    = json.loads(row["raw_payload"])
    resource_id = row["resource_id"]

    subject_ref = resource.get("subject", {}).get("reference", "")
    fhir_pat_id = subject_ref.split("/")[-1].replace("urn:uuid:", "")
    umpi = patient_umpi_map.get(subject_ref) or patient_umpi_map.get(fhir_pat_id)

    if not umpi:
        print(f"  WARN: No UMPI found for Encounter subject ref '{subject_ref}' — skipping")
        continue

    silver_enc_id = str(uuid.uuid4())
    fhir_enc_id   = resource.get("id", "")
    encounter_id_map[fhir_enc_id]               = silver_enc_id
    encounter_id_map[f"urn:uuid:{fhir_enc_id}"] = silver_enc_id

    period = resource.get("period", {})
    class_codings = resource.get("class", {})
    enc_class = (
        class_codings.get("code")
        if isinstance(class_codings, dict)
        else None
    )

    encounter_rows.append({
        "encounter_id":        silver_enc_id,
        "tenant_id":           TENANT_ID,
        "umpi":                umpi,
        "source_encounter_id": fhir_enc_id,
        "encounter_class":     enc_class,
        "period_start":        period.get("start"),
        "period_end":          period.get("end"),
        "facility_name":       None,
        "encounter_status":    resource.get("status"),
        "source_table":        BRONZE_FHIR_TABLE,
        "source_id":           resource_id,
        "pipeline_run_id":     pipeline_run_id,
        "created_ts":          now_ts,
    })

    print(f"  Encounter {fhir_enc_id}  →  silver_id={silver_enc_id}  umpi={umpi}")

print(f"\nEncounters built: {len(encounter_rows)}")

# COMMAND ----------

enc_schema = StructType([
    StructField("encounter_id",        StringType(),    False),
    StructField("tenant_id",           StringType(),    False),
    StructField("umpi",                StringType(),    False),
    StructField("source_encounter_id", StringType(),    True),
    StructField("encounter_class",     StringType(),    True),
    StructField("period_start",        StringType(),    True),
    StructField("period_end",          StringType(),    True),
    StructField("facility_name",       StringType(),    True),
    StructField("encounter_status",    StringType(),    True),
    StructField("source_table",        StringType(),    True),
    StructField("source_id",           StringType(),    True),
    StructField("pipeline_run_id",     StringType(),    False),
    StructField("created_ts",          TimestampType(), False),
])

if encounter_rows:
    enc_df = spark.createDataFrame(encounter_rows, schema=enc_schema)
    enc_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TBL_ENCOUNTERS)
    print(f"Wrote {len(encounter_rows)} row(s) to {TBL_ENCOUNTERS}")
else:
    print("No Encounter resources found — skipping encounters write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 2b — Observations (LOINC normalization)
# MAGIC
# MAGIC Every Observation produces exactly one lab_observations row and at least one
# MAGIC normalization_log entry. Unmapped codes also land in terminology_unmapped_codes.

# COMMAND ----------

lab_rows      = []
norm_log_rows = []
unmapped_rows = []

for row in by_type.get("Observation", []):
    resource    = json.loads(row["raw_payload"])
    resource_id = row["resource_id"]

    subject_ref = resource.get("subject", {}).get("reference", "")
    fhir_pat_id = subject_ref.split("/")[-1].replace("urn:uuid:", "")
    umpi = patient_umpi_map.get(subject_ref) or patient_umpi_map.get(fhir_pat_id)

    if not umpi:
        print(f"  WARN: No UMPI for Observation subject '{subject_ref}' — skipping")
        continue

    enc_ref     = resource.get("encounter", {}).get("reference", "")
    enc_fhir_id = enc_ref.split("/")[-1].replace("urn:uuid:", "")
    silver_enc_id = encounter_id_map.get(enc_ref) or encounter_id_map.get(enc_fhir_id)

    silver_rec, norm_log = normalize_fhir_observation(
        resource=resource,
        tenant_id=TENANT_ID,
        umpi=umpi,
        source_id=resource_id,
        terminology=terminology,
        encounter_silver_id=silver_enc_id,
    )

    lab_row = asdict(silver_rec)
    lab_row["pipeline_run_id"] = pipeline_run_id
    lab_row["created_ts"]      = now_ts
    lab_rows.append(lab_row)

    for entry in norm_log:
        entry["pipeline_run_id"] = pipeline_run_id
        entry["processed_ts"]    = now_ts
        norm_log_rows.append(entry)

        if entry.get("mapping_method") == "UNMAPPED":
            unmapped_rows.append({
                "record_id":          str(uuid.uuid4()),
                "pipeline_run_id":    pipeline_run_id,
                "tenant_id":          TENANT_ID,
                "source_table":       BRONZE_FHIR_TABLE,
                "source_id":          resource_id,
                "fhir_resource_type": "Observation",
                "target_terminology": "LOINC",
                "source_code":        entry.get("source_value"),
                "source_code_system": entry.get("source_system"),
                "source_display":     entry.get("source_value"),
                "created_ts":         now_ts,
            })

    print(f"  Observation {resource.get('id')}  →  loinc={silver_rec.loinc_code}  "
          f"method={silver_rec.loinc_map_method}  value={silver_rec.value_quantity}")

print(f"\nLab observations built: {len(lab_rows)}")
print(f"Normalization log entries: {len(norm_log_rows)}")
print(f"Unmapped codes: {len(unmapped_rows)}")

# COMMAND ----------

from pyspark.sql.types import BooleanType, LongType

lab_schema = StructType([
    StructField("observation_id",         StringType(),  False),
    StructField("tenant_id",              StringType(),  False),
    StructField("umpi",                   StringType(),  False),
    StructField("encounter_id",           StringType(),  True),
    StructField("loinc_code",             StringType(),  True),
    StructField("loinc_display",          StringType(),  True),
    StructField("source_code",            StringType(),  True),
    StructField("source_code_system",     StringType(),  True),
    StructField("source_display",         StringType(),  True),
    StructField("observation_status",     StringType(),  True),
    StructField("value_quantity",         DoubleType(),  True),
    StructField("value_unit",             StringType(),  True),
    StructField("value_string",           StringType(),  True),
    StructField("interpretation_code",    StringType(),  True),
    StructField("interpretation_display", StringType(),  True),
    StructField("reference_range_low",    DoubleType(),  True),
    StructField("reference_range_high",   DoubleType(),  True),
    StructField("reference_range_unit",   StringType(),  True),
    StructField("effective_datetime",     StringType(),  True),
    StructField("issued_datetime",        StringType(),  True),
    StructField("loinc_mapped",           BooleanType(), False),
    StructField("loinc_map_method",       StringType(),  False),
    StructField("source_table",           StringType(),  True),
    StructField("source_id",             StringType(),  True),
    StructField("pipeline_run_id",        StringType(),  False),
    StructField("created_ts",             TimestampType(), False),
])

if lab_rows:
    lab_df = spark.createDataFrame(lab_rows, schema=lab_schema)
    lab_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TBL_LAB_OBS)
    print(f"Wrote {len(lab_rows)} row(s) to {TBL_LAB_OBS}")
else:
    print("No Observation resources found — skipping lab_observations write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write normalization_log

# COMMAND ----------

norm_log_schema = StructType([
    StructField("log_id",            StringType(),    False),
    StructField("tenant_id",         StringType(),    False),
    StructField("source_table",      StringType(),    False),
    StructField("source_id",         StringType(),    False),
    StructField("target_table",      StringType(),    False),
    StructField("target_id",         StringType(),    True),
    StructField("mapping_type",      StringType(),    False),
    StructField("source_value",      StringType(),    True),
    StructField("source_system",     StringType(),    True),
    StructField("mapped_value",      StringType(),    True),
    StructField("mapped_system",     StringType(),    True),
    StructField("mapping_confidence", DoubleType(),   True),
    StructField("mapping_method",    StringType(),    True),
    StructField("pipeline_version",  StringType(),    True),
    StructField("pipeline_run_id",   StringType(),    False),
    StructField("processed_ts",      TimestampType(), False),
])

if norm_log_rows:
    norm_df = spark.createDataFrame(norm_log_rows, schema=norm_log_schema)
    norm_df.write.format("delta").mode("append").saveAsTable(TBL_NORM_LOG)
    print(f"Wrote {len(norm_log_rows)} row(s) to {TBL_NORM_LOG}")
else:
    print("No normalization log entries — skipping write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write terminology_unmapped_codes
# MAGIC
# MAGIC All unmapped codes land here regardless of resource type.
# MAGIC Query this table to identify gaps in the terminology service lookup tables.

# COMMAND ----------

unmapped_schema = StructType([
    StructField("record_id",          StringType(),    False),
    StructField("pipeline_run_id",    StringType(),    False),
    StructField("tenant_id",          StringType(),    False),
    StructField("source_table",       StringType(),    False),
    StructField("source_id",          StringType(),    False),
    StructField("fhir_resource_type", StringType(),    False),
    StructField("target_terminology", StringType(),    False),
    StructField("source_code",        StringType(),    True),
    StructField("source_code_system", StringType(),    True),
    StructField("source_display",     StringType(),    True),
    StructField("created_ts",         TimestampType(), False),
])

if unmapped_rows:
    unmapped_df = spark.createDataFrame(unmapped_rows, schema=unmapped_schema)
    unmapped_df.write.format("delta").mode("append").saveAsTable(TBL_UNMAPPED)
    print(f"Wrote {len(unmapped_rows)} unmapped code(s) to {TBL_UNMAPPED}")
else:
    print("All terminology codes mapped successfully — terminology_unmapped_codes not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 2c — Conditions (ICD-10 + SNOMED dual-coding)

# COMMAND ----------

diagnosis_rows = []

for row in by_type.get("Condition", []):
    resource    = json.loads(row["raw_payload"])
    resource_id = row["resource_id"]

    subject_ref = resource.get("subject", {}).get("reference", "")
    fhir_pat_id = subject_ref.split("/")[-1].replace("urn:uuid:", "")
    umpi = patient_umpi_map.get(subject_ref) or patient_umpi_map.get(fhir_pat_id)

    if not umpi:
        print(f"  WARN: No UMPI for Condition subject '{subject_ref}' — skipping")
        continue

    enc_ref       = resource.get("encounter", {}).get("reference", "")
    enc_fhir_id   = enc_ref.split("/")[-1].replace("urn:uuid:", "")
    silver_enc_id = encounter_id_map.get(enc_ref) or encounter_id_map.get(enc_fhir_id)

    codings      = resource.get("code", {}).get("coding", [])
    source_code = source_display = source_system = icd10_code = icd10_display = None

    for coding in codings:
        system       = coding.get("system", "")
        code         = coding.get("code")
        code_display = coding.get("display")
        if "icd-10" in system.lower() or "icd10" in system.lower():
            icd10_code    = code
            icd10_display = code_display
        source_code    = source_code or code
        source_display = source_display or code_display
        source_system  = source_system or system

    snomed_code = snomed_display = None
    if icd10_code:
        snomed_result = terminology.map_snomed_from_icd10(icd10_code)
        if snomed_result:
            snomed_code, snomed_display = snomed_result
        else:
            unmapped_row = {
                "record_id":          str(uuid.uuid4()),
                "pipeline_run_id":    pipeline_run_id,
                "tenant_id":          TENANT_ID,
                "source_table":       BRONZE_FHIR_TABLE,
                "source_id":          resource_id,
                "fhir_resource_type": "Condition",
                "target_terminology": "SNOMED",
                "source_code":        icd10_code,
                "source_code_system": source_system,
                "source_display":     icd10_display,
                "created_ts":         now_ts,
            }
            unmapped_rows.append(unmapped_row)
            unmapped_df_single = spark.createDataFrame([unmapped_row], schema=unmapped_schema)
            unmapped_df_single.write.format("delta").mode("append").saveAsTable(TBL_UNMAPPED)

    clinical_status = (
        resource.get("clinicalStatus", {})
        .get("coding", [{}])[0]
        .get("code")
    )
    onset = resource.get("onsetDateTime")

    diagnosis_rows.append({
        "diagnosis_id":       str(uuid.uuid4()),
        "tenant_id":          TENANT_ID,
        "umpi":               umpi,
        "encounter_id":       silver_enc_id,
        "icd10_code":         icd10_code,
        "icd10_display":      icd10_display,
        "snomed_code":        snomed_code,
        "snomed_display":     snomed_display,
        "source_code":        source_code,
        "source_code_system": source_system,
        "source_display":     source_display,
        "diagnosis_rank":     1,
        "clinical_status":    clinical_status,
        "onset_datetime":     onset,
        "source_table":       BRONZE_FHIR_TABLE,
        "source_id":          resource_id,
        "pipeline_run_id":    pipeline_run_id,
        "created_ts":         now_ts,
    })

    print(f"  Condition {resource.get('id')}  →  icd10={icd10_code}  snomed={snomed_code}")

print(f"\nDiagnoses built: {len(diagnosis_rows)}")

# COMMAND ----------

from pyspark.sql.types import IntegerType

diag_schema = StructType([
    StructField("diagnosis_id",      StringType(),    False),
    StructField("tenant_id",         StringType(),    False),
    StructField("umpi",              StringType(),    False),
    StructField("encounter_id",      StringType(),    True),
    StructField("icd10_code",        StringType(),    True),
    StructField("icd10_display",     StringType(),    True),
    StructField("snomed_code",       StringType(),    True),
    StructField("snomed_display",    StringType(),    True),
    StructField("source_code",       StringType(),    True),
    StructField("source_code_system", StringType(),   True),
    StructField("source_display",    StringType(),    True),
    StructField("diagnosis_rank",    IntegerType(),   True),
    StructField("clinical_status",   StringType(),    True),
    StructField("onset_datetime",    StringType(),    True),
    StructField("source_table",      StringType(),    True),
    StructField("source_id",         StringType(),    True),
    StructField("pipeline_run_id",   StringType(),    False),
    StructField("created_ts",        TimestampType(), False),
])

if diagnosis_rows:
    diag_df = spark.createDataFrame(diagnosis_rows, schema=diag_schema)
    diag_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TBL_DIAGNOSES)
    print(f"Wrote {len(diagnosis_rows)} row(s) to {TBL_DIAGNOSES}")
else:
    print("No Condition resources found — skipping diagnoses write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print(f"Bronze → Silver complete  |  pipeline_run_id: {pipeline_run_id}")
print("=" * 60)
print(f"  mpi_patient_index       : {len(mpi_patient_rows)} new UMPI(s)")
print(f"  mpi_identity_crosswalk  : {len(crosswalk_rows)} source link(s)")
print(f"  encounters              : {len(encounter_rows)} row(s)")
print(f"  lab_observations        : {len(lab_rows)} row(s)")
print(f"  diagnoses               : {len(diagnosis_rows)} row(s)")
print(f"  normalization_log       : {len(norm_log_rows)} entry(ies)")
print(f"  terminology_unmapped    : {len(unmapped_rows)} code(s)")
print()

if unmapped_rows:
    print("UNMAPPED CODES (action required — expand terminology service lookup tables):")
    for u in unmapped_rows:
        print(f"  {u['fhir_resource_type']:<15} {u['target_terminology']:<8} "
              f"code={u['source_code']}  display={u['source_display']}")
else:
    print("All terminology codes mapped successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — preview Silver rows

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        mpi.umpi,
        mpi.family_name,
        mpi.given_name,
        mpi.birth_date,
        mpi.match_method,
        lab.loinc_code,
        lab.loinc_display,
        lab.value_quantity,
        lab.value_unit,
        lab.loinc_map_method
    FROM {TBL_MPI_PATIENTS} mpi
    LEFT JOIN {TBL_LAB_OBS} lab ON mpi.umpi = lab.umpi
    WHERE mpi.pipeline_run_id = '{pipeline_run_id}'
    ORDER BY lab.loinc_code
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        crosswalk_id,
        umpi,
        source_id,
        source_mrn,
        source_identifier_system,
        match_method,
        matched_on,
        pipeline_run_id
    FROM {TBL_MPI_XWALK}
    WHERE pipeline_run_id = '{pipeline_run_id}'
    ORDER BY umpi
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV Ingestion Path — eClinicalWorks Patients and Labs
# MAGIC
# MAGIC Reads `ecw_patients.csv` and `ecw_labs.csv` from the synthetic data directory.
# MAGIC Each patient is run through MPIIndex for identity resolution (same 4-pass logic
# MAGIC used for FHIR patients). Lab rows are normalized via TerminologyService.
# MAGIC
# MAGIC DQ issues detected and logged to `audit_validation_errors`:
# MAGIC - `CSV_BLANK_NAME`         — first_name or last_name is empty
# MAGIC - `CSV_MALFORMED_DOB`      — DOB not in YYYY-MM-DD format; birth_date → NULL
# MAGIC - `CSV_DUPLICATE_ROW`      — same patient_id seen more than once
# MAGIC - `CSV_INVALID_ICD10`      — primary_dx_icd10 produces no SNOMED mapping
# MAGIC - `CSV_UNMAPPED_LAB_CODE`  — test_code has no LOINC mapping
# MAGIC - `CSV_TEXT_RESULT_VALUE`  — result_value cannot be parsed as float

# COMMAND ----------

import csv
import os
import re as _re

ECW_PATIENTS_FILE = f"{REPO_ROOT}/data/synthetic/ecw_patients.csv"
ECW_LABS_FILE     = f"{REPO_ROOT}/data/synthetic/ecw_labs.csv"

# Identifier system for eClinicalWorks patient IDs
ECW_IDENTIFIER_SYSTEM = "urn:system:eclinicalworks"


def _parse_iso_dob(dob_str):
    """
    Accept YYYY-MM-DD only.
    Returns (iso_str_or_None, is_malformed: bool).
    is_malformed=True means the value was present but not in ISO format.
    """
    if not dob_str:
        return None, False
    if _re.fullmatch(r"\d{4}-\d{2}-\d{2}", dob_str.strip()):
        return dob_str.strip(), False
    return None, True   # present but not ISO → null birth_date + DQ flag


def _csv_validation_error(code, field, detail, source_file, row_id, raw_val, run_id):
    return {
        "error_id":           str(uuid.uuid4()),
        "pipeline_run_id":    run_id,
        "source_file":        source_file,
        "message_id":         row_id,
        "message_control_id": None,
        "error_code":         code,
        "error_field":        field,
        "error_detail":       detail,
        "raw_value":          str(raw_val) if raw_val is not None else None,
        "severity":           "WARNING",
        "logged_at":          datetime.now(timezone.utc).replace(tzinfo=None),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step CSV-1 — Process ecw_patients.csv

# COMMAND ----------

csv_patient_rows      = []    # → clinical_patients
csv_mpi_patient_rows  = []    # → mpi_patient_index (new UMPIs only)
csv_crosswalk_rows    = []    # → mpi_identity_crosswalk
csv_validation_errs   = []    # → audit_validation_errors
ecw_patient_umpi_map  = {}    # ECW patient_id → umpi (for lab linkage)
seen_patient_ids      = {}    # for duplicate detection: patient_id → first row index

csv_patients_started = datetime.now(timezone.utc).replace(tzinfo=None)

with open(ECW_PATIENTS_FILE, newline="", encoding="utf-8") as fh:
    reader = csv.DictReader(fh)
    for row_idx, row in enumerate(reader):
        now_ts     = datetime.now(timezone.utc).replace(tzinfo=None)
        patient_id = row["patient_id"].strip()
        first_name = row["first_name"].strip()
        last_name  = row["last_name"].strip()
        raw_dob    = row["dob"].strip()
        gender     = row["gender"].strip()
        ssn_last4  = row["ssn_last4"].strip() or None
        zip_code   = row["zip"].strip() or None
        pcp_npi    = row["pcp_npi"].strip() or None
        primary_dx = row["primary_dx_icd10"].strip() or None

        dob_iso, dob_malformed = _parse_iso_dob(raw_dob)

        # ── DQ: blank name ────────────────────────────────────────────────────
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

        # ── DQ: malformed DOB ─────────────────────────────────────────────────
        if dob_malformed:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_MALFORMED_DOB", "dob",
                "DOB is not in ISO 8601 format (expected YYYY-MM-DD); birth_date set to NULL",
                ECW_PATIENTS_FILE, patient_id, raw_dob, pipeline_run_id,
            ))

        # ── DQ: duplicate row ─────────────────────────────────────────────────
        if patient_id in seen_patient_ids:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_DUPLICATE_ROW", "patient_id",
                f"Duplicate patient_id first seen at row {seen_patient_ids[patient_id]}; "
                f"MPI will return same UMPI (DETERMINISTIC match)",
                ECW_PATIENTS_FILE, patient_id, patient_id, pipeline_run_id,
            ))
        else:
            seen_patient_ids[patient_id] = row_idx

        # ── MPI resolution ────────────────────────────────────────────────────
        from transforms.identity_resolution import PatientIdentity
        identity = PatientIdentity(
            source_id=patient_id,
            source_table=ECW_PATIENTS_FILE,
            tenant_id=TENANT_ID,
            family_name=last_name or None,
            given_name=first_name or None,
            birth_date=dob_iso,
            gender=gender or None,
            postal_code=zip_code,
            ssn_last4=ssn_last4,
            source_mrn=patient_id,
            source_facility_npi=pcp_npi,
            source_identifier_system=ECW_IDENTIFIER_SYSTEM,
            source_identifier_value=patient_id,
        )
        result = mpi.resolve(identity)
        ecw_patient_umpi_map[patient_id] = result.umpi

        # ── mpi_patient_index: new patients only ──────────────────────────────
        if result.is_new_record:
            patient_rec = mpi.get_patient(result.umpi) or {}
            csv_mpi_patient_rows.append({
                "umpi":             result.umpi,
                "tenant_id":        TENANT_ID,
                "match_method":     result.match_method,
                "match_confidence": result.match_confidence,
                "family_name":      patient_rec.get("family_name") or last_name or None,
                "given_name":       patient_rec.get("given_name") or first_name or None,
                "birth_date":       patient_rec.get("birth_date") or dob_iso,
                "gender":           patient_rec.get("gender") or gender or None,
                "postal_code":      patient_rec.get("postal_code") or zip_code,
                "ssn_last4":        patient_rec.get("ssn_last4") or ssn_last4,
                "source_tenant_id": TENANT_ID,
                "source_table":     ECW_PATIENTS_FILE,
                "source_id":        patient_id,
                "pipeline_run_id":  pipeline_run_id,
                "created_ts":       now_ts,
            })

        # ── mpi_identity_crosswalk: always ────────────────────────────────────
        csv_crosswalk_rows.append({
            "crosswalk_id":             str(uuid.uuid4()),
            "umpi":                     result.umpi,
            "tenant_id":                TENANT_ID,
            "source_table":             ECW_PATIENTS_FILE,
            "source_id":                patient_id,
            "source_mrn":               patient_id,
            "source_facility_npi":      pcp_npi,
            "source_identifier_system": ECW_IDENTIFIER_SYSTEM,
            "source_identifier_value":  patient_id,
            "match_method":             result.match_method,
            "matched_on":               ",".join(result.matched_on) if result.matched_on else None,
            "pipeline_run_id":          pipeline_run_id,
            "created_ts":               now_ts,
        })

        # ── DQ: invalid ICD-10 ────────────────────────────────────────────────
        if primary_dx and terminology.map_snomed_from_icd10(primary_dx) is None:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_INVALID_ICD10", "primary_dx_icd10",
                f"ICD-10 code '{primary_dx}' has no SNOMED mapping; "
                f"written as-is, UNMAPPED entry logged",
                ECW_PATIENTS_FILE, patient_id, primary_dx, pipeline_run_id,
            ))

        # ── clinical_patients row ─────────────────────────────────────────────
        csv_patient_rows.append({
            "patient_id":       str(uuid.uuid4()),
            "tenant_id":        TENANT_ID,
            "umpi":             result.umpi,
            "source_row_id":    patient_id,
            "first_name":       first_name or None,
            "last_name":        last_name or None,
            "dob":              dob_iso,
            "gender":           gender or None,
            "ssn_last4":        ssn_last4,
            "address":          row["address"].strip() or None,
            "city":             row["city"].strip() or None,
            "state":            row["state"].strip() or None,
            "zip":              zip_code,
            "phone":            row["phone"].strip() or None,
            "pcp_npi":          pcp_npi,
            "insurance_id":     row["insurance_id"].strip() or None,
            "last_visit_date":  row["last_visit_date"].strip() or None,
            "primary_dx_icd10": primary_dx,
            "mpi_match_method": result.match_method,
            "source_table":     ECW_PATIENTS_FILE,
            "pipeline_run_id":  pipeline_run_id,
            "created_ts":       now_ts,
        })

csv_patients_completed = datetime.now(timezone.utc).replace(tzinfo=None)

print(f"ECW patients processed : {len(csv_patient_rows):,} rows")
print(f"New UMPIs minted        : {len(csv_mpi_patient_rows)}")
print(f"Crosswalk entries       : {len(csv_crosswalk_rows)}")
print(f"Validation issues       : {len(csv_validation_errs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step CSV-2 — Write ECW patient rows to Silver

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

clinical_patients_schema = StructType([
    StructField("patient_id",       StringType(),    False),
    StructField("tenant_id",        StringType(),    False),
    StructField("umpi",             StringType(),    False),
    StructField("source_row_id",    StringType(),    True),
    StructField("first_name",       StringType(),    True),
    StructField("last_name",        StringType(),    True),
    StructField("dob",              StringType(),    True),
    StructField("gender",           StringType(),    True),
    StructField("ssn_last4",        StringType(),    True),
    StructField("address",          StringType(),    True),
    StructField("city",             StringType(),    True),
    StructField("state",            StringType(),    True),
    StructField("zip",              StringType(),    True),
    StructField("phone",            StringType(),    True),
    StructField("pcp_npi",          StringType(),    True),
    StructField("insurance_id",     StringType(),    True),
    StructField("last_visit_date",  StringType(),    True),
    StructField("primary_dx_icd10", StringType(),    True),
    StructField("mpi_match_method", StringType(),    True),
    StructField("source_table",     StringType(),    True),
    StructField("pipeline_run_id",  StringType(),    False),
    StructField("created_ts",       TimestampType(), False),
])

if csv_patient_rows:
    cp_df = spark.createDataFrame(csv_patient_rows, schema=clinical_patients_schema)
    cp_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_PATIENTS)
    print(f"Wrote {cp_df.count():,} row(s) to {TBL_CLINICAL_PATIENTS}")

# Write new UMPIs to mpi_patient_index (same schema as FHIR patients)
if csv_mpi_patient_rows:
    mpi_df_csv = spark.createDataFrame(csv_mpi_patient_rows, schema=mpi_patients_schema)
    mpi_df_csv.write.format("delta").mode("append").insertInto(TBL_MPI_PATIENTS)
    print(f"Wrote {len(csv_mpi_patient_rows)} new UMPI(s) to {TBL_MPI_PATIENTS}")

# Write crosswalk entries (same schema as FHIR crosswalk)
if csv_crosswalk_rows:
    xwalk_df_csv = spark.createDataFrame(csv_crosswalk_rows, schema=xwalk_schema)
    xwalk_df_csv.write.format("delta").mode("append").insertInto(TBL_MPI_XWALK)
    print(f"Wrote {len(csv_crosswalk_rows)} crosswalk row(s) to {TBL_MPI_XWALK}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step CSV-3 — Process ecw_labs.csv

# COMMAND ----------

csv_obs_rows        = []    # → clinical_observations
csv_lab_unmapped    = []    # → terminology_unmapped_codes
csv_lab_val_errs    = []    # appended to csv_validation_errs

csv_labs_started = datetime.now(timezone.utc).replace(tzinfo=None)

with open(ECW_LABS_FILE, newline="", encoding="utf-8") as fh:
    reader = csv.DictReader(fh)
    for row in reader:
        now_ts       = datetime.now(timezone.utc).replace(tzinfo=None)
        result_id    = row["result_id"].strip()
        ecw_pat_id   = row["patient_id"].strip()
        test_code    = row["test_code"].strip()
        test_name    = row["test_name"].strip()
        raw_value    = row["result_value"].strip()
        result_unit  = row["result_unit"].strip() or None
        ref_low      = row["reference_range_low"].strip() or None
        ref_high     = row["reference_range_high"].strip() or None
        abnormal     = row["abnormal_flag"].strip() or None
        collect_date = row["collection_date"].strip() or None
        prov_npi     = row["ordering_provider_npi"].strip() or None
        status       = row["status"].strip() or None

        umpi = ecw_patient_umpi_map.get(ecw_pat_id)
        if not umpi:
            # Patient not in ecw_patients.csv (shouldn't happen with synthetic data)
            umpi = "UNKNOWN"

        # ── LOINC normalization ───────────────────────────────────────────────
        loinc_result = terminology.map_loinc(test_code)
        map_method   = "SOURCE_LOINC"

        if loinc_result is None:
            # Fallback: try test_name (display string)
            loinc_result = terminology.map_loinc(test_name)
            map_method   = "TERMINOLOGY_SERVICE" if loinc_result else "UNMAPPED"

        loinc_code    = loinc_result[0] if loinc_result else None
        loinc_display = loinc_result[1] if loinc_result else None
        loinc_mapped  = loinc_result is not None

        # ── DQ: unmapped lab code ─────────────────────────────────────────────
        if not loinc_mapped:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_UNMAPPED_LAB_CODE", "test_code",
                f"test_code '{test_code}' (test_name='{test_name}') has no LOINC mapping; "
                f"loinc_mapped=False, entry written to terminology_unmapped_codes",
                ECW_LABS_FILE, result_id, test_code, pipeline_run_id,
            ))
            csv_lab_unmapped.append({
                "record_id":          str(uuid.uuid4()),
                "pipeline_run_id":    pipeline_run_id,
                "tenant_id":          TENANT_ID,
                "source_table":       ECW_LABS_FILE,
                "source_id":          result_id,
                "fhir_resource_type": "Observation",
                "target_terminology": "LOINC",
                "source_code":        test_code,
                "source_code_system": ECW_IDENTIFIER_SYSTEM,
                "source_display":     test_name,
                "created_ts":         now_ts,
            })

        # ── Parse result value ────────────────────────────────────────────────
        value_quantity = None
        is_text_value  = False
        try:
            value_quantity = float(raw_value)
        except (ValueError, TypeError):
            is_text_value = True

        # ── DQ: text result when numeric expected ─────────────────────────────
        if is_text_value and raw_value:
            csv_validation_errs.append(_csv_validation_error(
                "CSV_TEXT_RESULT_VALUE", "result_value",
                f"result_value '{raw_value}' cannot be parsed as float; "
                f"value_quantity=NULL, result_value_raw preserved",
                ECW_LABS_FILE, result_id, raw_value, pipeline_run_id,
            ))

        # ── clinical_observations row ─────────────────────────────────────────
        csv_obs_rows.append({
            "observation_id":        str(uuid.uuid4()),
            "tenant_id":             TENANT_ID,
            "umpi":                  umpi,
            "source_result_id":      result_id,
            "source_patient_id":     ecw_pat_id,
            "test_code":             test_code,
            "test_name":             test_name,
            "loinc_code":            loinc_code,
            "loinc_display":         loinc_display,
            "loinc_mapped":          loinc_mapped,
            "loinc_map_method":      map_method,
            "result_value_raw":      raw_value,
            "value_quantity":        value_quantity,
            "value_unit":            result_unit,
            "reference_range_low":   ref_low,
            "reference_range_high":  ref_high,
            "abnormal_flag":         abnormal,
            "collection_date":       collect_date,
            "ordering_provider_npi": prov_npi,
            "result_status":         status,
            "source_table":          ECW_LABS_FILE,
            "pipeline_run_id":       pipeline_run_id,
            "created_ts":            now_ts,
        })

csv_labs_completed = datetime.now(timezone.utc).replace(tzinfo=None)

print(f"ECW lab rows processed  : {len(csv_obs_rows):,}")
print(f"LOINC-mapped            : {sum(1 for r in csv_obs_rows if r['loinc_mapped']):,}")
print(f"LOINC-unmapped          : {sum(1 for r in csv_obs_rows if not r['loinc_mapped']):,}")
print(f"Text result values      : {sum(1 for r in csv_obs_rows if r['value_quantity'] is None):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step CSV-4 — Write ECW lab rows to Silver

# COMMAND ----------

from pyspark.sql.types import BooleanType

clinical_obs_schema = StructType([
    StructField("observation_id",        StringType(),    False),
    StructField("tenant_id",             StringType(),    False),
    StructField("umpi",                  StringType(),    False),
    StructField("source_result_id",      StringType(),    True),
    StructField("source_patient_id",     StringType(),    True),
    StructField("test_code",             StringType(),    True),
    StructField("test_name",             StringType(),    True),
    StructField("loinc_code",            StringType(),    True),
    StructField("loinc_display",         StringType(),    True),
    StructField("loinc_mapped",          BooleanType(),   False),
    StructField("loinc_map_method",      StringType(),    False),
    StructField("result_value_raw",      StringType(),    True),
    StructField("value_quantity",        DoubleType(),    True),
    StructField("value_unit",            StringType(),    True),
    StructField("reference_range_low",   StringType(),    True),
    StructField("reference_range_high",  StringType(),    True),
    StructField("abnormal_flag",         StringType(),    True),
    StructField("collection_date",       StringType(),    True),
    StructField("ordering_provider_npi", StringType(),    True),
    StructField("result_status",         StringType(),    True),
    StructField("source_table",          StringType(),    True),
    StructField("pipeline_run_id",       StringType(),    False),
    StructField("created_ts",            TimestampType(), False),
])

if csv_obs_rows:
    obs_df = spark.createDataFrame(csv_obs_rows, schema=clinical_obs_schema)
    obs_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_OBS)
    print(f"Wrote {obs_df.count():,} row(s) to {TBL_CLINICAL_OBS}")

if csv_lab_unmapped:
    ul_df = spark.createDataFrame(csv_lab_unmapped, schema=unmapped_schema)
    ul_df.write.format("delta").mode("append").insertInto(TBL_UNMAPPED)
    print(f"Wrote {len(csv_lab_unmapped)} unmapped code(s) to {TBL_UNMAPPED}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step CSV-5 — Write validation errors and audit log for CSV path

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

validation_schema_csv = StructType([
    StructField("error_id",            StringType(),    False),
    StructField("pipeline_run_id",     StringType(),    False),
    StructField("source_file",         StringType(),    False),
    StructField("message_id",          StringType(),    True),
    StructField("message_control_id",  StringType(),    True),
    StructField("error_code",          StringType(),    False),
    StructField("error_field",         StringType(),    True),
    StructField("error_detail",        StringType(),    True),
    StructField("raw_value",           StringType(),    True),
    StructField("severity",            StringType(),    False),
    StructField("logged_at",           TimestampType(), False),
])

all_csv_validation_errs = csv_validation_errs
if all_csv_validation_errs:
    val_df_csv = spark.createDataFrame(all_csv_validation_errs, schema=validation_schema_csv)
    val_df_csv.write.format("delta").mode("append").insertInto(BRONZE_VALIDATION_TABLE)
    print(f"Wrote {val_df_csv.count():,} CSV validation error(s) to {BRONZE_VALIDATION_TABLE}")
else:
    print("No CSV validation errors")

# One audit entry per CSV file
audit_schema_csv = StructType([
    StructField("log_id",            StringType(),    False),
    StructField("pipeline_run_id",   StringType(),    False),
    StructField("notebook_name",     StringType(),    False),
    StructField("target_table",      StringType(),    False),
    StructField("source_path",       StringType(),    True),
    StructField("started_at",        TimestampType(), False),
    StructField("completed_at",      TimestampType(), True),
    StructField("records_attempted", LongType(),      True),
    StructField("records_succeeded", LongType(),      True),
    StructField("records_failed",    LongType(),      True),
    StructField("status",            StringType(),    False),
    StructField("error_detail",      StringType(),    True),
    StructField("ingestion_version", StringType(),    True),
    StructField("created_ts",        TimestampType(), False),
])

csv_audit_entries = [
    {
        "log_id":            str(uuid.uuid4()),
        "pipeline_run_id":   pipeline_run_id,
        "notebook_name":     "03_bronze_to_silver/csv",
        "target_table":      TBL_CLINICAL_PATIENTS,
        "source_path":       ECW_PATIENTS_FILE,
        "started_at":        csv_patients_started,
        "completed_at":      csv_patients_completed,
        "records_attempted": len(csv_patient_rows),
        "records_succeeded": len(csv_patient_rows),
        "records_failed":    0,
        "status":            "COMPLETED",
        "error_detail":      None,
        "ingestion_version": PIPELINE_VERSION,
        "created_ts":        datetime.now(timezone.utc).replace(tzinfo=None),
    },
    {
        "log_id":            str(uuid.uuid4()),
        "pipeline_run_id":   pipeline_run_id,
        "notebook_name":     "03_bronze_to_silver/csv",
        "target_table":      TBL_CLINICAL_OBS,
        "source_path":       ECW_LABS_FILE,
        "started_at":        csv_labs_started,
        "completed_at":      csv_labs_completed,
        "records_attempted": len(csv_obs_rows),
        "records_succeeded": len(csv_obs_rows),
        "records_failed":    0,
        "status":            "COMPLETED",
        "error_detail":      None,
        "ingestion_version": PIPELINE_VERSION,
        "created_ts":        datetime.now(timezone.utc).replace(tzinfo=None),
    },
]

audit_df_csv = spark.createDataFrame(csv_audit_entries, schema=audit_schema_csv)
audit_df_csv.write.format("delta").mode("append").insertInto(BRONZE_AUDIT_TABLE)
print(f"CSV audit entries written to {BRONZE_AUDIT_TABLE}")
for e in csv_audit_entries:
    print(f"  {os.path.basename(e['source_path']):25s}  "
          f"attempted={e['records_attempted']:5,}  status={e['status']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSV Summary

# COMMAND ----------

print("=" * 60)
print(f"CSV ingestion complete  |  pipeline_run_id: {pipeline_run_id}")
print("=" * 60)
print(f"  clinical_patients     : {len(csv_patient_rows):,} row(s)")
print(f"  mpi_patient_index     : {len(csv_mpi_patient_rows)} new UMPI(s) from CSV")
print(f"  mpi_identity_crosswalk: {len(csv_crosswalk_rows)} CSV crosswalk entries")
print(f"  clinical_observations : {len(csv_obs_rows):,} row(s)")
print(f"  terminology_unmapped  : {len(csv_lab_unmapped)} CSV unmapped code(s)")
print(f"  validation_errors     : {len(all_csv_validation_errs)} CSV DQ issue(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return pipeline_run_id to orchestrator

# COMMAND ----------

dbutils.notebook.exit(pipeline_run_id)
