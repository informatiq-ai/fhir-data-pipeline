# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Bronze → Silver Normalization
# MAGIC
# MAGIC **Purpose:** Read FHIR R4 Bundles and HL7 v2 messages from Bronze, run identity
# MAGIC resolution and terminology normalization, and write normalized records to Silver CDM
# MAGIC tables. All MPI matching logic is delegated entirely to `transforms/identity_resolution.py`.
# MAGIC
# MAGIC Processes FHIR R4 and HL7 v2 paths only. CSV batch path is handled by
# MAGIC `06_bronze_to_silver_csv.py` (Job 2).
# MAGIC
# MAGIC **Processing order (enforced):**
# MAGIC 1. Seed in-memory MPIIndex from existing Silver records (idempotency)
# MAGIC 2. FHIR Patient resources → `mpi_patient_index` + `mpi_identity_crosswalk` + `clinical_patients`
# MAGIC 3. FHIR Observation resources → LOINC normalization → `clinical_observations` + `terminology_unmapped_codes`
# MAGIC 4. HL7 ADT messages → `clinical_encounters` + `clinical_conditions`
# MAGIC
# MAGIC Unmapped codes land in `terminology_unmapped_codes` — nothing is dropped silently.
# MAGIC `clinical_observations.loinc_code` is NOT NULL; unmapped observations are skipped
# MAGIC from that table and captured in `terminology_unmapped_codes` for human review.
# MAGIC
# MAGIC **Reads from:** `dev.fhir_bronze.ingest_fhir_bundles`, `dev.fhir_bronze.ingest_hl7_messages`
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_silver.mpi_patient_index`
# MAGIC - `dev.fhir_silver.mpi_identity_crosswalk`
# MAGIC - `dev.fhir_silver.clinical_patients`
# MAGIC - `dev.fhir_silver.clinical_observations`
# MAGIC - `dev.fhir_silver.terminology_unmapped_codes`
# MAGIC - `dev.fhir_bronze.audit_ingest_log`
# MAGIC
# MAGIC **Run order:** Notebook 03 of 04. Run after `01_ingest_hl7.py` and
# MAGIC `02_ingest_fhir.py`, before `04_silver_to_gold.py`.

# COMMAND ----------

import sys
import os
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

TBL_MPI_PATIENTS         = f"{TGT_CATALOG}.{TGT_SCHEMA}.mpi_patient_index"
TBL_MPI_XWALK            = f"{TGT_CATALOG}.{TGT_SCHEMA}.mpi_identity_crosswalk"
TBL_CLINICAL_PATIENTS    = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_patients"
TBL_CLINICAL_ENCOUNTERS  = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_encounters"
TBL_CLINICAL_CONDITIONS  = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_conditions"
TBL_CLINICAL_OBS         = f"{TGT_CATALOG}.{TGT_SCHEMA}.clinical_observations"
TBL_UNMAPPED             = f"{TGT_CATALOG}.{TGT_SCHEMA}.terminology_unmapped_codes"

BRONZE_HL7_TABLE        = f"{SRC_CATALOG}.{SRC_SCHEMA}.ingest_hl7_messages"
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
    CREATE TABLE IF NOT EXISTS {TBL_CLINICAL_ENCOUNTERS} (
        encounter_id          STRING    NOT NULL  COMMENT 'UUID (Silver internal)',
        umpi                  STRING    NOT NULL  COMMENT 'FK to mpi_patient_index',
        encounter_class       STRING              COMMENT 'IMP | AMB | EMER | VR (HL7 ActEncounterCode)',
        encounter_type        STRING              COMMENT 'Snomed/local type code',
        status                STRING              COMMENT 'planned | in-progress | finished | cancelled',
        admit_datetime        TIMESTAMP,
        discharge_datetime    TIMESTAMP,
        length_of_stay_hours  DOUBLE              COMMENT 'Derived: discharge - admit in hours',
        facility_id           STRING              COMMENT 'NPI or internal facility code',
        attending_provider_npi STRING,
        principal_icd10       STRING              COMMENT 'Principal diagnosis ICD-10 code (denormalized)',
        tenant_id             STRING    NOT NULL,
        source_system         STRING,
        source_record_id      STRING,
        created_at            TIMESTAMP NOT NULL,
        updated_at            TIMESTAMP
    )
    USING DELTA
    COMMENT 'Normalized encounters across all ingestion paths.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_CLINICAL_CONDITIONS} (
        condition_id          STRING    NOT NULL,
        umpi                  STRING    NOT NULL,
        encounter_id          STRING,
        icd10_code            STRING    NOT NULL  COMMENT 'Normalized ICD-10 code',
        icd10_display         STRING,
        condition_category    STRING              COMMENT 'primary | secondary | comorbidity',
        onset_datetime        TIMESTAMP,
        abatement_datetime    TIMESTAMP,
        clinical_status       STRING              COMMENT 'active | recurrence | relapse | inactive | remission | resolved',
        verification_status   STRING              COMMENT 'confirmed | provisional | differential | refuted',
        tenant_id             STRING    NOT NULL,
        source_system         STRING,
        source_code           STRING,
        source_record_id      STRING,
        created_at            TIMESTAMP NOT NULL,
        updated_at            TIMESTAMP
    )
    USING DELTA
    COMMENT 'ICD-10 normalized conditions and diagnoses.'
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

if TGT_CATALOG == "dev":
    spark.sql(f"TRUNCATE TABLE {TBL_MPI_PATIENTS}")
    spark.sql(f"TRUNCATE TABLE {TBL_MPI_XWALK}")
    spark.sql(f"TRUNCATE TABLE {TBL_CLINICAL_PATIENTS}")
    spark.sql(f"TRUNCATE TABLE {TBL_CLINICAL_ENCOUNTERS}")
    spark.sql(f"TRUNCATE TABLE {TBL_CLINICAL_CONDITIONS}")
    spark.sql(f"TRUNCATE TABLE {TBL_CLINICAL_OBS}")
    print("Silver tables truncated (dev mode only)")
else:
    print(f"Truncate skipped — catalog is '{TGT_CATALOG}' (dev-only operation)")

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


def _parse_hl7_date(s):
    """Parse HL7 v2 date (YYYYMMDD) to a date object for MPI PatientIdentity."""
    if not s:
        return None
    s = s.strip()
    try:
        return datetime.strptime(s[:8], "%Y%m%d").date()
    except (ValueError, TypeError):
        return None


def _parse_hl7_datetime(s):
    """Parse HL7 v2 timestamp (YYYYMMDD[HHMMSS]) to naive UTC datetime."""
    if not s:
        return None
    s = s.strip()
    try:
        if len(s) >= 14:
            return datetime.strptime(s[:14], "%Y%m%d%H%M%S")
        if len(s) >= 8:
            return datetime.strptime(s[:8], "%Y%m%d")
    except (ValueError, TypeError):
        pass
    return None


def _parse_hl7_segments(raw_payload):
    """Split a raw HL7 v2 message into a dict of {segment_name: [fields_list]}."""
    segs = {}
    for line in raw_payload.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        seg_name = parts[0]
        segs.setdefault(seg_name, []).append(parts)
    return segs


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

CLINICAL_ENCOUNTERS_SCHEMA = StructType([
    StructField("encounter_id",           StringType(),    False),  # NOT NULL
    StructField("umpi",                   StringType(),    False),  # NOT NULL
    StructField("encounter_class",        StringType(),    True),
    StructField("encounter_type",         StringType(),    True),
    StructField("status",                 StringType(),    True),
    StructField("admit_datetime",         TimestampType(), True),
    StructField("discharge_datetime",     TimestampType(), True),
    StructField("length_of_stay_hours",   DoubleType(),    True),
    StructField("facility_id",            StringType(),    True),
    StructField("attending_provider_npi", StringType(),    True),
    StructField("principal_icd10",        StringType(),    True),
    StructField("tenant_id",              StringType(),    False),  # NOT NULL
    StructField("source_system",          StringType(),    True),
    StructField("source_record_id",       StringType(),    True),
    StructField("created_at",             TimestampType(), False),  # NOT NULL
    StructField("updated_at",             TimestampType(), True),
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
# MAGIC ## Pass 3 — FHIR Encounter → clinical_encounters

# COMMAND ----------

fhir_encounter_rows = []

for rec in by_type.get("Encounter", []):
    resource      = rec["resource"]
    bundle_id     = rec["bundle_id"]
    fhir_id       = resource.get("id", str(uuid.uuid4()))
    source_rec_id = f"{bundle_id}/{fhir_id}"

    subject_ref = resource.get("subject", {}).get("reference", "")
    fhir_pat_id = subject_ref.split("/")[-1].replace("urn:uuid:", "")
    umpi = patient_umpi_map.get(subject_ref) or patient_umpi_map.get(fhir_pat_id)

    if not umpi:
        print(f"  WARN: No UMPI for Encounter subject '{subject_ref}' — skipping")
        continue

    period       = resource.get("period", {})
    admit_dt     = _parse_obs_datetime(period.get("start"))
    discharge_dt = _parse_obs_datetime(period.get("end"))

    los_hours = None
    if admit_dt and discharge_dt:
        los_hours = (discharge_dt - admit_dt).total_seconds() / 3600.0

    encounter_id    = str(uuid.uuid4())
    encounter_class = resource.get("class", {}).get("code")
    facility_id     = resource.get("serviceProvider", {}).get("display")

    fhir_encounter_rows.append({
        "encounter_id":           encounter_id,
        "umpi":                   umpi,
        "encounter_class":        encounter_class,
        "encounter_type":         None,
        "status":                 resource.get("status"),
        "admit_datetime":         admit_dt,
        "discharge_datetime":     discharge_dt,
        "length_of_stay_hours":   los_hours,
        "facility_id":            facility_id,
        "attending_provider_npi": None,
        "principal_icd10":        None,
        "tenant_id":              TENANT_ID,
        "source_system":          "FHIR_R4",
        "source_record_id":       source_rec_id,
        "created_at":             now_ts,
        "updated_at":             None,
    })

print(f"FHIR encounters built: {len(fhir_encounter_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write clinical_encounters — FHIR

# COMMAND ----------

if fhir_encounter_rows:
    enc_fhir_df = spark.createDataFrame(fhir_encounter_rows, schema=CLINICAL_ENCOUNTERS_SCHEMA)
    enc_fhir_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_ENCOUNTERS)
    print(f"Wrote {len(fhir_encounter_rows)} FHIR encounter row(s) to {TBL_CLINICAL_ENCOUNTERS}")
else:
    print("No FHIR Encounter resources — clinical_encounters (FHIR) not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 4 — FHIR Condition → clinical_conditions

# COMMAND ----------

fhir_condition_rows = []

for rec in by_type.get("Condition", []):
    resource      = rec["resource"]
    bundle_id     = rec["bundle_id"]
    fhir_id       = resource.get("id", str(uuid.uuid4()))
    source_rec_id = f"{bundle_id}/{fhir_id}"

    subject_ref = resource.get("subject", {}).get("reference", "")
    fhir_pat_id = subject_ref.split("/")[-1].replace("urn:uuid:", "")
    umpi = patient_umpi_map.get(subject_ref) or patient_umpi_map.get(fhir_pat_id)

    if not umpi:
        print(f"  WARN: No UMPI for Condition subject '{subject_ref}' — skipping")
        continue

    codings    = (resource.get("code", {}).get("coding") or [{}])
    icd10_code = codings[0].get("code") if codings else None
    icd10_disp = codings[0].get("display") if codings else None

    if not icd10_code:
        print(f"  WARN: Condition {fhir_id} has no code — skipping")
        continue

    clin_status   = ((resource.get("clinicalStatus", {}).get("coding") or [{}])[0]).get("code")
    verif_status  = ((resource.get("verificationStatus", {}).get("coding") or [{}])[0]).get("code")
    onset_dt      = _parse_obs_datetime(resource.get("onsetDateTime"))
    abatement_dt  = _parse_obs_datetime(resource.get("abatementDateTime"))

    fhir_condition_rows.append({
        "condition_id":         str(uuid.uuid4()),
        "umpi":                 umpi,
        "encounter_id":         None,
        "icd10_code":           icd10_code,
        "icd10_display":        icd10_disp,
        "condition_category":   "primary",
        "onset_datetime":       onset_dt,
        "abatement_datetime":   abatement_dt,
        "clinical_status":      clin_status,
        "verification_status":  verif_status,
        "tenant_id":            TENANT_ID,
        "source_system":        "FHIR_R4",
        "source_code":          icd10_code,
        "source_record_id":     source_rec_id,
        "created_at":           now_ts,
        "updated_at":           None,
    })

print(f"FHIR conditions built: {len(fhir_condition_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write clinical_conditions — FHIR

# COMMAND ----------

if fhir_condition_rows:
    cond_fhir_df = spark.createDataFrame(fhir_condition_rows, schema=CLINICAL_CONDITIONS_SCHEMA)
    cond_fhir_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_CONDITIONS)
    print(f"Wrote {len(fhir_condition_rows)} FHIR condition row(s) to {TBL_CLINICAL_CONDITIONS}")
else:
    print("No FHIR Condition resources — clinical_conditions (FHIR) not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 5 — HL7 ADT → clinical_encounters + clinical_conditions
# MAGIC
# MAGIC ADT^A01 (admission) → encounter status = in-progress
# MAGIC ADT^A03 (discharge) → encounter status = finished
# MAGIC DG1 segment (present in A01 only) → condition row with ICD-10 principal diagnosis.

# COMMAND ----------

hl7_adt_rows = spark.sql(f"""
    SELECT message_id, raw_payload, message_event, source_facility
    FROM {BRONZE_HL7_TABLE}
    WHERE message_type = 'ADT'
      AND message_event IN ('A01', 'A03')
      AND validation_status != 'ERROR'
""").collect()

_class_map = {"I": "IMP", "O": "AMB", "E": "EMER", "R": "AMB"}

hl7_encounter_rows  = []
hl7_condition_rows  = []

for row in hl7_adt_rows:
    now_ts_hl7    = datetime.now(timezone.utc).replace(tzinfo=None)
    raw           = row["raw_payload"]
    event_code    = row["message_event"]      # A01 or A03
    source_rec_id = row["message_id"]

    segs = _parse_hl7_segments(raw)

    # PID: patient demographics for MPI resolution
    pid       = (segs.get("PID") or [[]])[0]
    mrn_raw   = pid[3] if len(pid) > 3 else ""      # PID-3: MRN^^^facility^MR
    pid_parts = mrn_raw.split("^")
    mrn        = pid_parts[0] if pid_parts else None
    facility_from_pid = pid_parts[3] if len(pid_parts) > 3 else None

    name_raw  = pid[5] if len(pid) > 5 else ""      # PID-5: family^given^middle
    name_parts = name_raw.split("^")
    last_name  = name_parts[0] if name_parts else None
    first_name = name_parts[1] if len(name_parts) > 1 else None

    dob_raw    = pid[7] if len(pid) > 7 else None   # PID-7: YYYYMMDD
    gender     = pid[8] if len(pid) > 8 else None   # PID-8: M | F | U

    dob_date   = _parse_hl7_date(dob_raw)

    facility_npi = row["source_facility"] or facility_from_pid

    identity = PatientIdentity(
        source_id=source_rec_id,
        source_table=BRONZE_HL7_TABLE,
        tenant_id=TENANT_ID,
        family_name=last_name or None,
        given_name=first_name or None,
        birth_date=dob_date,
        gender=gender or None,
        postal_code=None,
        ssn_last4=None,
        source_mrn=mrn or None,
        source_facility_npi=facility_npi or None,
        source_identifier_system="urn:hl7:adt",
    )
    result = mpi.resolve(identity)
    umpi   = result.umpi

    # EVN-2: event date/time (admission or discharge)
    evn      = (segs.get("EVN") or [[]])[0]
    event_dt = _parse_hl7_datetime(evn[2] if len(evn) > 2 else None)

    # PV1: encounter metadata
    pv1            = (segs.get("PV1") or [[]])[0]
    patient_class  = pv1[2] if len(pv1) > 2 else None   # PV1-2: I | O | E
    loc_raw        = pv1[3] if len(pv1) > 3 else ""     # PV1-3: ward^room^bed^facility
    loc_parts      = loc_raw.split("^")
    enc_facility   = loc_parts[3] if len(loc_parts) > 3 else (facility_npi or None)

    att_raw   = pv1[7] if len(pv1) > 7 else ""           # PV1-7: NPI-xxx^name...
    att_parts = att_raw.split("^")
    att_npi   = att_parts[0].replace("NPI-", "") if att_parts and att_parts[0] else None

    enc_class = _class_map.get(patient_class) if patient_class else None

    if event_code == "A01":
        status       = "in-progress"
        admit_dt     = event_dt
        discharge_dt = None
        los_hours    = None
    else:  # A03
        status       = "finished"
        admit_dt     = None
        discharge_dt = event_dt
        los_hours    = None

    # DG1: principal ICD-10 diagnosis (present in A01; skip if absent)
    dg1           = (segs.get("DG1") or [[]])[0]
    principal_icd10 = None
    icd10_disp    = None
    if len(dg1) > 3:
        dg1_parts     = dg1[3].split("^")          # DG1-3: code^display^ICD10
        principal_icd10 = dg1_parts[0] if dg1_parts and dg1_parts[0] else None
        icd10_disp    = dg1_parts[1] if len(dg1_parts) > 1 else None

    encounter_id = str(uuid.uuid4())

    hl7_encounter_rows.append({
        "encounter_id":           encounter_id,
        "umpi":                   umpi,
        "encounter_class":        enc_class,
        "encounter_type":         None,
        "status":                 status,
        "admit_datetime":         admit_dt,
        "discharge_datetime":     discharge_dt,
        "length_of_stay_hours":   los_hours,
        "facility_id":            enc_facility,
        "attending_provider_npi": att_npi or None,
        "principal_icd10":        principal_icd10,
        "tenant_id":              TENANT_ID,
        "source_system":          "HL7_V2",
        "source_record_id":       source_rec_id,
        "created_at":             now_ts_hl7,
        "updated_at":             None,
    })

    if principal_icd10:
        clin_status = "active" if event_code == "A01" else "resolved"
        hl7_condition_rows.append({
            "condition_id":         str(uuid.uuid4()),
            "umpi":                 umpi,
            "encounter_id":         encounter_id,
            "icd10_code":           principal_icd10,
            "icd10_display":        icd10_disp,
            "condition_category":   "primary",
            "onset_datetime":       admit_dt,
            "abatement_datetime":   None,
            "clinical_status":      clin_status,
            "verification_status":  "confirmed",
            "tenant_id":            TENANT_ID,
            "source_system":        "HL7_V2",
            "source_code":          principal_icd10,
            "source_record_id":     source_rec_id,
            "created_at":           now_ts_hl7,
            "updated_at":           None,
        })

print(f"HL7 ADT messages processed : {len(hl7_adt_rows)}")
print(f"HL7 encounters built       : {len(hl7_encounter_rows)}")
print(f"HL7 conditions built (DG1) : {len(hl7_condition_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write clinical_encounters — HL7

# COMMAND ----------

if hl7_encounter_rows:
    enc_hl7_df = spark.createDataFrame(hl7_encounter_rows, schema=CLINICAL_ENCOUNTERS_SCHEMA)
    enc_hl7_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_ENCOUNTERS)
    print(f"Wrote {len(hl7_encounter_rows)} HL7 encounter row(s) to {TBL_CLINICAL_ENCOUNTERS}")
else:
    print("No HL7 ADT messages — clinical_encounters (HL7) not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write clinical_conditions — HL7

# COMMAND ----------

if hl7_condition_rows:
    cond_hl7_df = spark.createDataFrame(hl7_condition_rows, schema=CLINICAL_CONDITIONS_SCHEMA)
    cond_hl7_df.write.format("delta").mode("append").insertInto(TBL_CLINICAL_CONDITIONS)
    print(f"Wrote {len(hl7_condition_rows)} HL7 condition row(s) to {TBL_CLINICAL_CONDITIONS}")
else:
    print("No HL7 DG1 diagnoses — clinical_conditions (HL7) not written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_mpi        = len(fhir_mpi_rows)
total_xwalk      = len(fhir_xwalk_rows)
total_patients   = len(fhir_patient_rows)
total_encounters = len(fhir_encounter_rows) + len(hl7_encounter_rows)
total_conditions = len(fhir_condition_rows) + len(hl7_condition_rows)
total_obs        = len(fhir_obs_rows)
total_unmapped   = len(fhir_unmapped_rows)

print("=" * 60)
print(f"Bronze → Silver complete  |  pipeline_run_id: {pipeline_run_id}")
print("=" * 60)
print(f"  mpi_patient_index       : {total_mpi} new UMPI(s)")
print(f"  mpi_identity_crosswalk  : {total_xwalk} source link(s)")
print(f"  clinical_patients       : {total_patients} row(s)")
print(f"  clinical_encounters     : {total_encounters} row(s)  "
      f"(FHIR={len(fhir_encounter_rows)}  HL7={len(hl7_encounter_rows)})")
print(f"  clinical_conditions     : {total_conditions} row(s)  "
      f"(FHIR={len(fhir_condition_rows)}  HL7={len(hl7_condition_rows)})")
print(f"  clinical_observations   : {total_obs} row(s) (mapped only)")
print(f"  terminology_unmapped    : {total_unmapped} code(s)")

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
