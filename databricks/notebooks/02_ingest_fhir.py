# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingest FHIR R4 Bundle (Bronze)
# MAGIC
# MAGIC **Purpose:** Read FHIR R4 transaction Bundles from two source files (a single-bundle
# MAGIC sample and a 500-bundle batch) and land one row per Bundle in the Bronze Delta table.
# MAGIC The full Bundle JSON is preserved in `raw_payload` — immutable at ingest.
# MAGIC `resource_types` records which FHIR resource types are present in each bundle.
# MAGIC
# MAGIC Post-ingest validation catches two FHIR DQ issues and routes them to
# MAGIC `audit_validation_errors`:
# MAGIC - `FHIR_MISSING_TENANT`       — no tenant tag in Bundle.meta.tag
# MAGIC - `FHIR_MISSING_BIRTH_DATE`   — Patient resource has no birthDate
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_bronze.ingest_fhir_bundles`      — one row per Bundle
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
from datetime import datetime, timezone

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

TENANT_ID         = "INTEGRIS_BAPTIST"
CATALOG           = "dev"
SCHEMA            = "fhir_bronze"
TARGET_TABLE      = f"{CATALOG}.{SCHEMA}.ingest_fhir_bundles"
AUDIT_TABLE       = f"{CATALOG}.{SCHEMA}.audit_ingest_log"
VALIDATION_TABLE  = f"{CATALOG}.{SCHEMA}.audit_validation_errors"
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

from ingestion.fhir_ingester import TENANT_TAG_SYSTEM

print(f"TENANT_TAG_SYSTEM: {TENANT_TAG_SYSTEM}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog, schema, and target tables (idempotent DDL)
# MAGIC
# MAGIC Schema matches `databricks/fhir_pipeline_ddl.sql` exactly.
# MAGIC `audit_ingest_log` and `audit_validation_errors` are shared with notebook 01
# MAGIC — `CREATE TABLE IF NOT EXISTS` is safe to repeat.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        bundle_id         STRING    NOT NULL  COMMENT 'UUID generated at ingest',
        raw_payload       STRING    NOT NULL  COMMENT 'Full FHIR R4 Bundle JSON, immutable',
        bundle_type       STRING              COMMENT 'transaction | batch | collection',
        resource_types    ARRAY<STRING>       COMMENT 'Resource types present in bundle',
        tenant_id         STRING    NOT NULL  COMMENT 'Resolved from meta.tag',
        source_system     STRING              COMMENT 'Originating FHIR server',
        received_at       TIMESTAMP NOT NULL  COMMENT 'UTC timestamp at ingest boundary',
        validation_status STRING              COMMENT 'PASS | ERROR',
        pipeline_run_id   STRING              COMMENT 'Links to audit_ingest_log'
    )
    USING DELTA
    COMMENT 'Raw FHIR R4 Bundles. Bronze = immutable. Supports Silver replay when standards change.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        log_id            STRING    NOT NULL  COMMENT 'UUID',
        pipeline_run_id   STRING    NOT NULL  COMMENT 'Run identifier shared across tables',
        ingestion_path    STRING              COMMENT 'hl7 | fhir | csv',
        source_table      STRING              COMMENT 'Which ingest_* table was populated',
        record_count      BIGINT              COMMENT 'Total records received',
        pass_count        BIGINT              COMMENT 'Records that passed validation',
        error_count       BIGINT              COMMENT 'Records routed to validation_errors',
        tenant_id         STRING              COMMENT 'Tenant this run pertains to (NULL if multi)',
        run_started_at    TIMESTAMP           COMMENT 'UTC run start',
        run_completed_at  TIMESTAMP           COMMENT 'UTC run end',
        logged_at         TIMESTAMP NOT NULL  COMMENT 'UTC insert timestamp'
    )
    USING DELTA
    COMMENT 'Unified ingest audit trail across HL7, FHIR, and CSV paths.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {VALIDATION_TABLE} (
        error_id          STRING    NOT NULL  COMMENT 'UUID',
        pipeline_run_id   STRING              COMMENT 'Links to audit_ingest_log',
        ingestion_path    STRING              COMMENT 'hl7 | fhir | csv',
        source_record_id  STRING              COMMENT 'FK to the originating ingest_* table',
        error_code        STRING              COMMENT 'Structured error code (e.g. FHIR_MISSING_TENANT)',
        error_message     STRING              COMMENT 'Human-readable description',
        raw_payload       STRING              COMMENT 'Copied from source for review without joins',
        tenant_id         STRING,
        requires_review   BOOLEAN,
        reviewed_at       TIMESTAMP,
        reviewed_by       STRING,
        review_outcome    STRING              COMMENT 'APPROVED | REJECTED | ESCALATED',
        created_at        TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Validation failures requiring human review. Records here are blocked from Silver.'
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
# MAGIC Inspects each bundle dict directly for two structured DQ issues:
# MAGIC - `FHIR_MISSING_TENANT`     — no tenant tag in Bundle.meta.tag
# MAGIC - `FHIR_MISSING_BIRTH_DATE` — Patient resource has no birthDate field

# COMMAND ----------

def _detect_fhir_validation_issues(bundle_dict, source_bundle_id, run_id):
    """
    Post-ingest validation of a FHIR bundle dict.
    Returns list of validation_error dicts matching the audit_validation_errors DDL schema.
    """
    issues = []
    now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    def _issue(code, field, detail, raw_val=None):
        return {
            "error_id":         str(uuid.uuid4()),
            "pipeline_run_id":  run_id,
            "ingestion_path":   "fhir",
            "source_record_id": source_bundle_id,
            "error_code":       code,
            "error_message":    f"{field}: {detail}",
            "raw_payload":      str(raw_val)[:500] if raw_val is not None else None,
            "tenant_id":        None,
            "requires_review":  True,
            "reviewed_at":      None,
            "reviewed_by":      None,
            "review_outcome":   None,
            "created_at":       now_ts,
        }

    # DQ-FHIR-003: Missing Bundle.meta.tag (no tenant tag present)
    tags = bundle_dict.get("meta", {}).get("tag", [])
    has_tenant_tag = any(t.get("system") == TENANT_TAG_SYSTEM for t in tags)
    if not has_tenant_tag:
        issues.append(_issue(
            "FHIR_MISSING_TENANT",
            "Bundle.meta.tag",
            "No tenant tag found in Bundle.meta.tag; tenant_id resolved via default fallback",
            raw_val=tags,
        ))

    # DQ-FHIR-001: Missing Patient.birthDate (inspects bundle entries directly)
    for entry in bundle_dict.get("entry", []):
        resource = entry.get("resource", {})
        if resource.get("resourceType") == "Patient" and "birthDate" not in resource:
            issues.append(_issue(
                "FHIR_MISSING_BIRTH_DATE",
                "Patient.birthDate",
                "Patient resource is missing birthDate; MPI DOB-based passes will be skipped",
                raw_val=resource.get("id"),
            ))

    return issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source files
# MAGIC
# MAGIC - `fhir_bundle_sample.json` is a single Bundle JSON object.
# MAGIC - `fhir_bundle_batch.json` is a JSON array of 500 Bundle objects.
# MAGIC   Each element produces one row in `ingest_fhir_bundles`.

# COMMAND ----------

SOURCE_FILES = [
    f"{REPO_ROOT}/data/synthetic/fhir_bundle_sample.json",
    f"{REPO_ROOT}/data/synthetic/fhir_bundle_batch.json",
]

for path in SOURCE_FILES:
    exists = os.path.exists(path)
    print(f"  {'OK' if exists else 'MISSING':7s}  {os.path.basename(path)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest bundles per file
# MAGIC
# MAGIC Each file is loaded and its bundles are processed one by one. One Bronze row is
# MAGIC written per Bundle — the full Bundle JSON is stored in `raw_payload`. Resource
# MAGIC type extraction and tenant resolution are done from the bundle dict directly.
# MAGIC Per-bundle validation issues are collected for `audit_validation_errors`.

# COMMAND ----------

all_rows            = []   # rows for ingest_fhir_bundles
all_validation_errs = []   # rows for audit_validation_errors
audit_entries       = []   # rows for audit_ingest_log (one per file)

for file_path in SOURCE_FILES:
    file_started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    with open(file_path, "r") as fh:
        content = fh.read()

    # Parse file: single bundle object or JSON array of bundles
    parsed  = json.loads(content)
    bundles = parsed if isinstance(parsed, list) else [parsed]

    n_attempted = len(bundles)
    n_failed    = 0
    n_dq_issues = 0

    for bundle_dict in bundles:
        received_at  = datetime.now(timezone.utc).replace(tzinfo=None)
        bundle_id    = bundle_dict.get("id") or str(uuid.uuid4())

        # Resolve tenant_id from meta.tag; fall back to default
        tags = bundle_dict.get("meta", {}).get("tag", [])
        tenant_id = next(
            (t.get("code") for t in tags
             if t.get("system") == TENANT_TAG_SYSTEM and t.get("code")),
            TENANT_ID,
        )
        source_system = next(
            (t.get("code") for t in tags
             if t.get("system") != TENANT_TAG_SYSTEM and t.get("code")),
            None,
        )

        # Extract distinct resource types from bundle entries
        resource_types = list({
            e.get("resource", {}).get("resourceType")
            for e in bundle_dict.get("entry", [])
            if e.get("resource", {}).get("resourceType")
        })

        # Detect validation issues; count structural failures
        issues = _detect_fhir_validation_issues(bundle_dict, bundle_id, pipeline_run_id)
        all_validation_errs.extend(issues)
        n_dq_issues += len(issues)

        # A bundle is ERROR only on structural failure; DQ issues are warnings
        try:
            raw_payload      = json.dumps(bundle_dict)
            validation_status = "PASS"
        except (TypeError, ValueError) as exc:
            raw_payload       = str(bundle_dict)[:5000]
            validation_status = "ERROR"
            n_failed         += 1

        all_rows.append({
            "bundle_id":         bundle_id,
            "raw_payload":       raw_payload,
            "bundle_type":       bundle_dict.get("type"),
            "resource_types":    resource_types,
            "tenant_id":         tenant_id,
            "source_system":     source_system,
            "received_at":       received_at,
            "validation_status": validation_status,
            "pipeline_run_id":   pipeline_run_id,
        })

    n_succeeded       = n_attempted - n_failed
    file_completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    audit_entries.append({
        "log_id":           str(uuid.uuid4()),
        "pipeline_run_id":  pipeline_run_id,
        "ingestion_path":   "fhir",
        "source_table":     TARGET_TABLE,
        "record_count":     n_attempted,
        "pass_count":       n_succeeded,
        "error_count":      n_failed,
        "tenant_id":        TENANT_ID,
        "run_started_at":   file_started_at,
        "run_completed_at": file_completed_at,
        "logged_at":        datetime.now(timezone.utc).replace(tzinfo=None),
    })

    print(f"  {os.path.basename(file_path):35s}  "
          f"{n_attempted:5,} bundles  "
          f"{n_succeeded:5,} ok  "
          f"{n_failed:3} errors  "
          f"{n_dq_issues:3} dq_issues")

total_rows    = len(all_rows)
total_dq      = len(all_validation_errs)
total_bundles = sum(e["record_count"] for e in audit_entries)
print(f"\nTotal: {total_bundles:,} bundles → {total_rows:,} Bronze rows, "
      f"{total_dq:,} validation issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze FHIR rows to Delta table

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, ArrayType
)

fhir_schema = StructType([
    StructField("bundle_id",         StringType(),            False),  # NOT NULL
    StructField("raw_payload",       StringType(),            False),  # NOT NULL
    StructField("bundle_type",       StringType(),            True),
    StructField("resource_types",    ArrayType(StringType()), True),
    StructField("tenant_id",         StringType(),            False),  # NOT NULL
    StructField("source_system",     StringType(),            True),
    StructField("received_at",       TimestampType(),         False),  # NOT NULL
    StructField("validation_status", StringType(),            True),
    StructField("pipeline_run_id",   StringType(),            True),
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

from pyspark.sql.types import BooleanType

validation_schema = StructType([
    StructField("error_id",         StringType(),    False),  # NOT NULL
    StructField("pipeline_run_id",  StringType(),    True),
    StructField("ingestion_path",   StringType(),    True),
    StructField("source_record_id", StringType(),    True),
    StructField("error_code",       StringType(),    True),
    StructField("error_message",    StringType(),    True),
    StructField("raw_payload",      StringType(),    True),
    StructField("tenant_id",        StringType(),    True),
    StructField("requires_review",  BooleanType(),   True),
    StructField("reviewed_at",      TimestampType(), True),
    StructField("reviewed_by",      StringType(),    True),
    StructField("review_outcome",   StringType(),    True),
    StructField("created_at",       TimestampType(), False),  # NOT NULL
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

from pyspark.sql.types import LongType

audit_schema = StructType([
    StructField("log_id",           StringType(),    False),  # NOT NULL
    StructField("pipeline_run_id",  StringType(),    False),  # NOT NULL
    StructField("ingestion_path",   StringType(),    True),
    StructField("source_table",     StringType(),    True),
    StructField("record_count",     LongType(),      True),
    StructField("pass_count",       LongType(),      True),
    StructField("error_count",      LongType(),      True),
    StructField("tenant_id",        StringType(),    True),
    StructField("run_started_at",   TimestampType(), True),
    StructField("run_completed_at", TimestampType(), True),
    StructField("logged_at",        TimestampType(), False),  # NOT NULL
])

audit_df = spark.createDataFrame(audit_entries, schema=audit_schema)

audit_df.write \
    .format("delta") \
    .mode("append") \
    .insertInto(AUDIT_TABLE)

print(f"Audit log written: {len(audit_entries)} file(s) logged to {AUDIT_TABLE}")
for e in audit_entries:
    print(f"  ingestion_path={e['ingestion_path']}  "
          f"record_count={e['record_count']:5,}  "
          f"pass={e['pass_count']:5,}  "
          f"error={e['error_count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — preview written rows

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            tenant_id,
            bundle_type,
            validation_status,
            COUNT(*)                   AS bundle_count,
            COUNT(source_system)       AS with_source_system
        FROM {TARGET_TABLE}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        GROUP BY tenant_id, bundle_type, validation_status
        ORDER BY bundle_count DESC
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            error_code,
            ingestion_path,
            COUNT(*) AS issue_count
        FROM {VALIDATION_TABLE}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        GROUP BY error_code, ingestion_path
        ORDER BY issue_count DESC
    """)
)

# COMMAND ----------

# DBTITLE 1,Return pipeline_run_id to orchestrator
dbutils.notebook.exit(pipeline_run_id)
