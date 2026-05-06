# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingest FHIR R4 Bundle (Bronze)
# MAGIC
# MAGIC **Purpose:** Read FHIR R4 transaction Bundles from two source files (a single-bundle
# MAGIC sample and a 500-bundle batch), split them into per-resource rows using
# MAGIC `ingestion/fhir_ingester.py` logic, and land one Bronze row per resource.
# MAGIC The full Bundle JSON is preserved on the first resource row of each sample-file
# MAGIC bundle for audit; batch bundles omit the full payload to avoid row bloat.
# MAGIC
# MAGIC Post-ingest validation catches two FHIR DQ issues and routes them to
# MAGIC `audit_validation_errors`:
# MAGIC - `FHIR_MISSING_TENANT`       — no tenant tag in Bundle.meta.tag
# MAGIC - `FHIR_MISSING_BIRTH_DATE`   — Patient resource has no birthDate
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_bronze.fhir_resources`           — one row per resource
# MAGIC - `dev.fhir_bronze.audit_validation_errors`   — structured DQ issues
# MAGIC - `dev.fhir_bronze.audit_ingest_log`          — one row per source file
# MAGIC
# MAGIC **Source files processed:**
# MAGIC - `data/synthetic/fhir_bundle_sample.json`  — single FHIR R4 Bundle
# MAGIC - `data/synthetic/fhir_bundle_batch.json`   — JSON array of 500 Bundles
# MAGIC
# MAGIC **Run order:** Notebook 02 of 04. Run after `01_ingest_hl7.py`,
# MAGIC before `03_bronze_to_silver.py`.

# COMMAND ----------

import sys
import os
import uuid
import json
from dataclasses import asdict
from datetime import datetime, timezone

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

TENANT_ID         = "INTEGRIS_BAPTIST"
CATALOG           = "dev"
SCHEMA            = "fhir_bronze"
TARGET_TABLE      = f"{CATALOG}.{SCHEMA}.fhir_resources"
AUDIT_TABLE       = f"{CATALOG}.{SCHEMA}.audit_ingest_log"
VALIDATION_TABLE  = f"{CATALOG}.{SCHEMA}.audit_validation_errors"
NOTEBOOK_NAME     = "02_ingest_fhir"
INGESTION_VERSION = "1.0.0"

print(f"pipeline_run_id  : {pipeline_run_id}")
print(f"target_table     : {TARGET_TABLE}")
print(f"audit_table      : {AUDIT_TABLE}")
print(f"validation_table : {VALIDATION_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve repo root and add ingestion module to sys.path

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

from ingestion.fhir_ingester import ingest_fhir_bundle, TENANT_TAG_SYSTEM

print("fhir_ingester imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog, schema, and target tables (idempotent DDL)
# MAGIC
# MAGIC `audit_ingest_log` and `audit_validation_errors` are shared with notebook 01
# MAGIC — `CREATE TABLE IF NOT EXISTS` is safe to repeat.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        resource_id         STRING  NOT NULL COMMENT 'Surrogate key — UUID generated at ingest',
        tenant_id           STRING  NOT NULL COMMENT 'Tenant from Bundle.meta.tag',
        bundle_id           STRING  COMMENT 'Bundle.id from source',
        bundle_timestamp    STRING  COMMENT 'Bundle.timestamp as ISO 8601 string',
        bundle_type         STRING  COMMENT 'transaction | message | collection',
        fhir_resource_type  STRING  NOT NULL COMMENT 'Patient | Encounter | Observation | Condition …',
        fhir_resource_id    STRING  COMMENT 'resource.id from source',
        fhir_version_id     STRING  COMMENT 'resource.meta.versionId',
        raw_payload         STRING  NOT NULL COMMENT 'JSON string of this resource — never modified',
        bundle_payload      STRING  COMMENT 'Full Bundle JSON on first resource row; NULL on remainder',
        source_system       STRING  COMMENT 'Source system tag from Bundle.meta.tag',
        feed_type           STRING  COMMENT 'FHIR_R4',
        batch_id            STRING  COMMENT 'Batch identifier',
        received_ts         STRING  NOT NULL COMMENT 'ISO 8601 UTC timestamp',
        ingestion_version   STRING  COMMENT 'Pipeline version that wrote this row',
        processing_status   STRING  NOT NULL COMMENT 'PENDING | ERROR',
        processing_error    STRING  COMMENT 'Exception detail if processing_status = ERROR',
        pipeline_run_id     STRING  NOT NULL COMMENT 'UUID linking all rows from one notebook run'
    )
    USING DELTA
    COMMENT 'Bronze landing table for FHIR R4 resources. One row per resource, not per Bundle.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        log_id              STRING      NOT NULL COMMENT 'UUID for this audit row',
        pipeline_run_id     STRING      NOT NULL COMMENT 'UUID linking to the notebook run',
        notebook_name       STRING      NOT NULL COMMENT 'Notebook that wrote this row',
        target_table        STRING      NOT NULL COMMENT 'Fully-qualified Delta table written to',
        source_path         STRING      COMMENT 'Source file path',
        started_at          TIMESTAMP   NOT NULL COMMENT 'UTC timestamp when file processing started',
        completed_at        TIMESTAMP   COMMENT 'UTC timestamp when file processing finished',
        records_attempted   LONG        COMMENT 'Total bundles processed from this file',
        records_succeeded   LONG        COMMENT 'Bundles ingested successfully',
        records_failed      LONG        COMMENT 'Bundles that produced ERROR records',
        status              STRING      NOT NULL COMMENT 'COMPLETED | PARTIAL | FAILED',
        error_detail        STRING      COMMENT 'Top-level exception message if status = FAILED',
        ingestion_version   STRING      COMMENT 'Pipeline version',
        created_ts          TIMESTAMP   NOT NULL COMMENT 'Row insert timestamp'
    )
    USING DELTA
    COMMENT 'Audit log — one row per source file per notebook run.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {VALIDATION_TABLE} (
        error_id            STRING      NOT NULL COMMENT 'UUID for this validation error row',
        pipeline_run_id     STRING      NOT NULL COMMENT 'UUID linking to the notebook run',
        source_file         STRING      NOT NULL COMMENT 'Source file that contained this bundle',
        message_id          STRING      COMMENT 'Bundle ID or resource_id of the affected record',
        message_control_id  STRING      COMMENT 'Not used for FHIR (HL7 MSH-10 field)',
        error_code          STRING      NOT NULL COMMENT 'Structured error code (e.g. FHIR_MISSING_TENANT)',
        error_field         STRING      COMMENT 'FHIR element path (e.g. Bundle.meta.tag)',
        error_detail        STRING      COMMENT 'Human-readable description of the issue',
        raw_value           STRING      COMMENT 'The raw field value that triggered the error',
        severity            STRING      NOT NULL COMMENT 'WARNING | ERROR',
        logged_at           TIMESTAMP   NOT NULL COMMENT 'When this error was recorded'
    )
    USING DELTA
    COMMENT 'Structured validation errors for Bronze FHIR ingestion. One row per issue per bundle/resource.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

print(f"Tables verified: {TARGET_TABLE}")
print(f"                 {AUDIT_TABLE}")
print(f"                 {VALIDATION_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper function — FHIR post-ingest validation
# MAGIC
# MAGIC Checks each ingested bundle for two structured DQ issues:
# MAGIC - `FHIR_MISSING_TENANT`     — no tenant tag in Bundle.meta.tag
# MAGIC - `FHIR_MISSING_BIRTH_DATE` — Patient resource has no birthDate field

# COMMAND ----------

def _detect_fhir_validation_issues(bundle_dict, ingest_result, source_file, run_id):
    """
    Post-ingest validation of a FHIR bundle and its ingested records.
    Returns list of validation_error dicts compatible with audit_validation_errors.
    """
    issues = []
    now_ts  = datetime.now(timezone.utc).replace(tzinfo=None)
    bundle_id = bundle_dict.get("id")

    def _issue(code, field, detail, msg_id=None, raw_val=None, severity="WARNING"):
        return {
            "error_id":           str(uuid.uuid4()),
            "pipeline_run_id":    run_id,
            "source_file":        source_file,
            "message_id":         msg_id or bundle_id,
            "message_control_id": None,
            "error_code":         code,
            "error_field":        field,
            "error_detail":       detail,
            "raw_value":          raw_val,
            "severity":           severity,
            "logged_at":          now_ts,
        }

    # DQ-FHIR-003: Missing Bundle.meta.tag (no tenant tag present)
    tags = bundle_dict.get("meta", {}).get("tag", [])
    has_tenant_tag = any(t.get("system") == TENANT_TAG_SYSTEM for t in tags)
    if not has_tenant_tag:
        issues.append(_issue(
            "FHIR_MISSING_TENANT",
            "Bundle.meta.tag",
            "No tenant tag found in Bundle.meta.tag; tenant_id resolved via default fallback",
            raw_val=str(tags),
        ))

    # DQ-FHIR-001: Missing Patient.birthDate
    for rec in ingest_result.records:
        if rec.fhir_resource_type == "Patient" and rec.processing_status == "PENDING":
            patient = json.loads(rec.raw_payload)
            if "birthDate" not in patient:
                issues.append(_issue(
                    "FHIR_MISSING_BIRTH_DATE",
                    "Patient.birthDate",
                    "Patient resource is missing birthDate; MPI pass 4 (DOB+name+zip) will be skipped",
                    msg_id=rec.resource_id,
                ))

    return issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source files
# MAGIC
# MAGIC - `fhir_bundle_sample.json` is a single Bundle JSON object.
# MAGIC - `fhir_bundle_batch.json` is a JSON array of 500 Bundle objects.
# MAGIC   Each element is ingested individually so bundle-level validation
# MAGIC   and audit entries remain per-bundle.

# COMMAND ----------

# (file_path, store_full_bundle)
# store_full_bundle=True only for the sample file to preserve audit payload;
# False for the batch file avoids attaching 500 full-bundle copies to Bronze rows.
SOURCE_FILES = [
    (f"{REPO_ROOT}/data/synthetic/fhir_bundle_sample.json", True),
    (f"{REPO_ROOT}/data/synthetic/fhir_bundle_batch.json",  False),
]

for path, store_full in SOURCE_FILES:
    exists = os.path.exists(path)
    print(f"  {'OK' if exists else 'MISSING':7s}  {os.path.basename(path)}  "
          f"(store_full_bundle={store_full})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest bundles per file
# MAGIC
# MAGIC Each file is loaded and its bundles are ingested one by one.
# MAGIC Structural failures produce a single ERROR record — nothing is silently dropped.
# MAGIC Per-bundle validation issues are collected for `audit_validation_errors`.

# COMMAND ----------

all_rows            = []   # rows for fhir_resources
all_validation_errs = []   # rows for audit_validation_errors
audit_entries       = []   # rows for audit_ingest_log (one per file)

for file_path, store_full_bundle in SOURCE_FILES:
    file_started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    with open(file_path, "r") as fh:
        content = fh.read()

    # Parse file: single bundle object or JSON array of bundles
    parsed = json.loads(content)
    bundles = parsed if isinstance(parsed, list) else [parsed]

    n_attempted = len(bundles)
    n_failed    = 0
    n_dq_issues = 0

    for bundle_dict in bundles:
        result = ingest_fhir_bundle(
            bundle_dict,
            default_tenant_id=TENANT_ID,
            store_full_bundle=store_full_bundle,
        )

        if not result.success:
            n_failed += 1

        for rec in result.records:
            row = asdict(rec)
            row["pipeline_run_id"] = pipeline_run_id
            all_rows.append(row)

        # Post-ingest validation: FHIR_MISSING_TENANT, FHIR_MISSING_BIRTH_DATE
        issues = _detect_fhir_validation_issues(bundle_dict, result, file_path, pipeline_run_id)
        all_validation_errs.extend(issues)
        n_dq_issues += len(issues)

    n_succeeded       = n_attempted - n_failed
    file_completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    audit_entries.append({
        "log_id":            str(uuid.uuid4()),
        "pipeline_run_id":   pipeline_run_id,
        "notebook_name":     NOTEBOOK_NAME,
        "target_table":      TARGET_TABLE,
        "source_path":       file_path,
        "started_at":        file_started_at,
        "completed_at":      file_completed_at,
        "records_attempted": n_attempted,
        "records_succeeded": n_succeeded,
        "records_failed":    n_failed,
        "status":            "COMPLETED" if n_failed == 0 else "PARTIAL",
        "error_detail":      None,
        "ingestion_version": INGESTION_VERSION,
        "created_ts":        datetime.now(timezone.utc).replace(tzinfo=None),
    })

    print(f"  {os.path.basename(file_path):35s}  "
          f"{n_attempted:5,} bundles  "
          f"{n_succeeded:5,} ok  "
          f"{n_failed:3} errors  "
          f"{n_dq_issues:3} dq_issues")

total_rows  = len(all_rows)
total_dq    = len(all_validation_errs)
total_bundles = sum(e["records_attempted"] for e in audit_entries)
print(f"\nTotal: {total_bundles:,} bundles → {total_rows:,} Bronze rows, "
      f"{total_dq:,} validation issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze FHIR rows to Delta table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

fhir_schema = StructType([
    StructField("resource_id",        StringType(), False),
    StructField("tenant_id",          StringType(), False),
    StructField("bundle_id",          StringType(), True),
    StructField("bundle_timestamp",   StringType(), True),
    StructField("bundle_type",        StringType(), True),
    StructField("fhir_resource_type", StringType(), False),
    StructField("fhir_resource_id",   StringType(), True),
    StructField("fhir_version_id",    StringType(), True),
    StructField("raw_payload",        StringType(), False),
    StructField("bundle_payload",     StringType(), True),
    StructField("source_system",      StringType(), True),
    StructField("feed_type",          StringType(), True),
    StructField("batch_id",           StringType(), True),
    StructField("received_ts",        StringType(), False),
    StructField("ingestion_version",  StringType(), True),
    StructField("processing_status",  StringType(), False),
    StructField("processing_error",   StringType(), True),
    StructField("pipeline_run_id",    StringType(), False),
])

df = spark.createDataFrame(all_rows, schema=fhir_schema)

df.write \
    .format("delta") \
    .mode("append") \
    .insertInto(TARGET_TABLE)

print(f"Wrote {df.count():,} row(s) to {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write validation errors to audit_validation_errors

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType

validation_schema = StructType([
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

if all_validation_errs:
    val_df = spark.createDataFrame(all_validation_errs, schema=validation_schema)
    val_df.write \
        .format("delta") \
        .mode("append") \
        .insertInto(VALIDATION_TABLE)
    print(f"Wrote {val_df.count():,} validation error row(s) to {VALIDATION_TABLE}")
else:
    print("No validation errors — skipping write to audit_validation_errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write audit log entries (one per source file)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

audit_schema = StructType([
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

audit_df = spark.createDataFrame(audit_entries, schema=audit_schema)

audit_df.write \
    .format("delta") \
    .mode("append") \
    .insertInto(AUDIT_TABLE)

print(f"Audit log written: {len(audit_entries)} file(s) logged to {AUDIT_TABLE}")
for e in audit_entries:
    print(f"  {os.path.basename(e['source_path']):35s}  "
          f"status={e['status']:10s}  "
          f"attempted={e['records_attempted']:5,}  "
          f"succeeded={e['records_succeeded']:5,}  "
          f"failed={e['records_failed']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — preview written rows

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            tenant_id,
            fhir_resource_type,
            processing_status,
            COUNT(*)              AS resource_count,
            COUNT(bundle_payload) AS with_full_bundle
        FROM {TARGET_TABLE}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        GROUP BY tenant_id, fhir_resource_type, processing_status
        ORDER BY resource_count DESC
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            error_code,
            error_field,
            severity,
            COUNT(*) AS issue_count
        FROM {VALIDATION_TABLE}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        GROUP BY error_code, error_field, severity
        ORDER BY issue_count DESC
    """)
)

# COMMAND ----------

# DBTITLE 1,Return pipeline_run_id to orchestrator
dbutils.notebook.exit(pipeline_run_id)
