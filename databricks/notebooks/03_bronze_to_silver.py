# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Bronze → Silver Normalization
# MAGIC
# MAGIC **Purpose:** Read FHIR R4 resource rows from Bronze, run identity resolution
# MAGIC and terminology normalization, and write normalized records to Silver CDM tables.
# MAGIC
# MAGIC **Processing order (enforced):**
# MAGIC 1. Patient resources → MPI resolution → `master_patient_index`
# MAGIC 2. Encounter resources → `encounters`
# MAGIC 3. Observation resources → LOINC normalization → `lab_observations` + `normalization_log`
# MAGIC 4. Condition resources → SNOMED dual-coding → `diagnoses`
# MAGIC
# MAGIC Unmapped terminology codes land in `terminology_unmapped_codes` — nothing is dropped.
# MAGIC Every mapping (mapped or UNMAPPED) is written to `normalization_log`.
# MAGIC
# MAGIC **Reads from:** `dev.fhir_bronze.fhir_resources`
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_silver.master_patient_index`
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
import os
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

TBL_MPI            = f"{TGT_CATALOG}.{TGT_SCHEMA}.master_patient_index"
TBL_ENCOUNTERS     = f"{TGT_CATALOG}.{TGT_SCHEMA}.encounters"
TBL_LAB_OBS        = f"{TGT_CATALOG}.{TGT_SCHEMA}.lab_observations"
TBL_DIAGNOSES      = f"{TGT_CATALOG}.{TGT_SCHEMA}.diagnoses"
TBL_NORM_LOG       = f"{TGT_CATALOG}.{TGT_SCHEMA}.normalization_log"
TBL_UNMAPPED       = f"{TGT_CATALOG}.{TGT_SCHEMA}.terminology_unmapped_codes"

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
print(f"MPI index initialized (in-memory, reference implementation)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver catalog, schema, and target tables (idempotent DDL)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TGT_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TGT_CATALOG}.{TGT_SCHEMA}")

# ── master_patient_index ──────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_MPI} (
        umpi                STRING  NOT NULL COMMENT 'Universal Master Patient Index — UUID',
        tenant_id           STRING  NOT NULL COMMENT 'Tenant that first created this record',
        match_method        STRING  NOT NULL COMMENT 'DETERMINISTIC | NEW_RECORD',
        match_confidence    DOUBLE  COMMENT '1.0 = deterministic lookup; 0.0 = new record',
        family_name         STRING,
        given_name          STRING,
        birth_date          STRING  COMMENT 'ISO 8601 date string (YYYY-MM-DD)',
        gender              STRING,
        postal_code         STRING,
        ssn_last4           STRING  COMMENT 'Last 4 of SSN only — never full SSN',
        source_tenant_id    STRING,
        source_table        STRING  COMMENT 'Bronze table that provided the Patient resource',
        source_id           STRING  COMMENT 'Bronze resource_id for this patient',
        pipeline_run_id     STRING  NOT NULL,
        created_ts          TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Universal patient identity index. Every Silver entity is keyed to a UMPI.'
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

print("Silver tables verified")

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
# MAGIC Every Patient resource in Bronze is resolved through the MPI before any
# MAGIC clinical normalization runs. The resulting UMPI is the key for all
# MAGIC subsequent Silver entities in this Bundle.

# COMMAND ----------

now_ts = datetime.utcnow()

mpi_rows         = []
# patient_umpi_map: fhir_resource_id → umpi (used by Encounter/Observation/Condition)
patient_umpi_map = {}
# bronze_resource_id → umpi (keyed by Bronze source_id for lineage)
bronze_to_umpi   = {}

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

    # Map the FHIR Patient ID (used as subject.reference target in other resources)
    fhir_id = resource.get("id", "")
    patient_umpi_map[fhir_id]          = result.umpi
    patient_umpi_map[f"urn:uuid:{fhir_id}"] = result.umpi
    bronze_to_umpi[resource_id]        = result.umpi

    patient_rec = mpi.get_patient(result.umpi) or {}

    mpi_rows.append({
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

    print(f"  Patient {fhir_id}  →  umpi={result.umpi}  method={result.match_method}")

print(f"\nMPI resolved: {len(mpi_rows)} patient(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write master_patient_index rows

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

mpi_schema = StructType([
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

if mpi_rows:
    mpi_df = spark.createDataFrame(mpi_rows, schema=mpi_schema)
    mpi_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TBL_MPI)
    print(f"Wrote {len(mpi_rows)} row(s) to {TBL_MPI}")
else:
    print("No Patient resources found — skipping MPI write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pass 2a — Encounters
# MAGIC
# MAGIC Encounter resources are linked to the resolved UMPI via the subject reference.

# COMMAND ----------

encounter_rows    = []
# encounter silver_id → encounter_id (for linking Observation / Condition)
encounter_id_map  = {}  # fhir Encounter id → silver encounter_id

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
    encounter_id_map[fhir_enc_id]          = silver_enc_id
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

    # Resolve encounter reference if present
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

    # Build lab_observations row
    lab_row = asdict(silver_rec)
    lab_row["pipeline_run_id"] = pipeline_run_id
    lab_row["created_ts"]      = now_ts
    lab_rows.append(lab_row)

    # Collect normalization log entries
    for entry in norm_log:
        entry["pipeline_run_id"] = pipeline_run_id
        entry["processed_ts"]    = now_ts
        norm_log_rows.append(entry)

        # Write to unmapped table if this is an UNMAPPED entry
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
    StructField("source_id",              StringType(),  True),
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

    # Extract ICD-10 code
    codings      = resource.get("code", {}).get("coding", [])
    source_code = source_display = source_system = icd10_code = icd10_display = None

    for coding in codings:
        system  = coding.get("system", "")
        code    = coding.get("code")
        display = coding.get("display")
        if "icd-10" in system.lower() or "icd10" in system.lower():
            icd10_code    = code
            icd10_display = display
        source_code    = source_code or code
        source_display = source_display or display
        source_system  = source_system or system

    # SNOMED dual-coding via terminology service
    snomed_code = snomed_display = None
    if icd10_code:
        snomed_result = terminology.map_snomed_from_icd10(icd10_code)
        if snomed_result:
            snomed_code, snomed_display = snomed_result
        else:
            # Log unmapped ICD-10 → SNOMED
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
            # Append immediately to Delta so it's captured even if later code fails
            unmapped_df_single = spark.createDataFrame([unmapped_row], schema=unmapped_schema)
            unmapped_df_single.write.format("delta").mode("append").saveAsTable(TBL_UNMAPPED)

    clinical_status = (
        resource.get("clinicalStatus", {})
        .get("coding", [{}])[0]
        .get("code")
    )
    onset = resource.get("onsetDateTime")

    diagnosis_rows.append({
        "diagnosis_id":     str(uuid.uuid4()),
        "tenant_id":        TENANT_ID,
        "umpi":             umpi,
        "encounter_id":     silver_enc_id,
        "icd10_code":       icd10_code,
        "icd10_display":    icd10_display,
        "snomed_code":      snomed_code,
        "snomed_display":   snomed_display,
        "source_code":      source_code,
        "source_code_system": source_system,
        "source_display":   source_display,
        "diagnosis_rank":   1,
        "clinical_status":  clinical_status,
        "onset_datetime":   onset,
        "source_table":     BRONZE_FHIR_TABLE,
        "source_id":        resource_id,
        "pipeline_run_id":  pipeline_run_id,
        "created_ts":       now_ts,
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
print(f"  master_patient_index    : {len(mpi_rows)} row(s)")
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
    FROM {TBL_MPI} mpi
    LEFT JOIN {TBL_LAB_OBS} lab ON mpi.umpi = lab.umpi
    WHERE mpi.pipeline_run_id = '{pipeline_run_id}'
    ORDER BY lab.loinc_code
"""))
