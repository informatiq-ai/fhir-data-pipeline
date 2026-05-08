# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest HL7 v2 (Bronze)
# MAGIC
# MAGIC **Purpose:** Read HL7 v2 ADT and ORU messages from four source files (both
# MAGIC single-message samples and volume batch files), parse them using the
# MAGIC `ingestion/hl7_parser.py` logic, and land one row per message in the Bronze
# MAGIC Delta table. Every message lands — parse failures produce `validation_status=ERROR`
# MAGIC rows, never silent drops. Post-parse validation issues are captured in a
# MAGIC structured `audit_validation_errors` table with error codes.
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_bronze.ingest_hl7_messages`     — one row per message
# MAGIC - `dev.fhir_bronze.audit_validation_errors`  — structured DQ issues per message
# MAGIC - `dev.fhir_bronze.audit_ingest_log`         — one row per source file
# MAGIC
# MAGIC **Source files processed:**
# MAGIC - `data/synthetic/hl7_adt_sample.txt`  — single ADT^A01 sample (1 message)
# MAGIC - `data/synthetic/hl7_oru_sample.txt`  — single ORU^R01 sample (1 message)
# MAGIC - `data/synthetic/hl7_adt_batch.txt`   — volume ADT batch (1000 messages)
# MAGIC - `data/synthetic/hl7_oru_batch.txt`   — volume ORU batch (500 messages)
# MAGIC
# MAGIC **Run order:** This is notebook 01 of 04. Run before `02_ingest_fhir.py`.

# COMMAND ----------

import sys
import os
import uuid
from datetime import datetime, timezone

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

dbutils.widgets.text("tenant_id", "INTEGRIS_BAPTIST", "Tenant ID")
TENANT_ID         = dbutils.widgets.get("tenant_id")
CATALOG           = "dev"
SCHEMA            = "fhir_bronze"
TARGET_TABLE      = f"{CATALOG}.{SCHEMA}.ingest_hl7_messages"
AUDIT_TABLE       = f"{CATALOG}.{SCHEMA}.audit_ingest_log"
VALIDATION_TABLE  = f"{CATALOG}.{SCHEMA}.audit_validation_errors"
INGESTION_VERSION = "1.0.0"

print(f"pipeline_run_id   : {pipeline_run_id}")
print(f"tenant_id         : {TENANT_ID}")
print(f"target_table      : {TARGET_TABLE}")
print(f"audit_table       : {AUDIT_TABLE}")
print(f"validation_table  : {VALIDATION_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve repo root and add ingestion module to sys.path
# MAGIC
# MAGIC Databricks Repos clones the repository under `/Workspace/Repos/<user>/`.
# MAGIC We derive the repo root from the running notebook's workspace path so the
# MAGIC import works regardless of which user account runs this notebook.

# COMMAND ----------

_nb_path = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)
# e.g. /Repos/user@domain/fhir-data-pipeline/databricks/notebooks/01_ingest_hl7
REPO_ROOT = "/Workspace" + _nb_path.rsplit("/databricks/notebooks", 1)[0]
print(f"REPO_ROOT: {REPO_ROOT}")

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from ingestion.hl7_parser import parse_hl7_batch, parse_hl7_timestamp

print("hl7_parser imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog, schema, and target tables (idempotent DDL)
# MAGIC
# MAGIC Schema matches `databricks/fhir_pipeline_ddl.sql` exactly.
# MAGIC Do not add or rename columns here — the DDL file is the single source of truth.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        message_id        STRING    NOT NULL  COMMENT 'UUID generated at ingest',
        raw_payload       STRING    NOT NULL  COMMENT 'Full HL7 v2 message, immutable',
        message_type      STRING              COMMENT 'ADT | ORU | VXU | SIU',
        message_event     STRING              COMMENT 'A01 | A08 | R01 | etc.',
        tenant_id         STRING    NOT NULL  COMMENT 'Resolved from ZTN segment',
        source_system     STRING              COMMENT 'Sending application (MSH-3)',
        source_facility   STRING              COMMENT 'Sending facility (MSH-4)',
        received_at       TIMESTAMP NOT NULL  COMMENT 'UTC timestamp at ingest boundary',
        validation_status STRING              COMMENT 'PASS | ERROR',
        pipeline_run_id   STRING              COMMENT 'Links to audit_ingest_log'
    )
    USING DELTA
    COMMENT 'Raw HL7 v2 messages. Bronze = immutable. Supports Silver replay when standards change.'
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
        error_code        STRING              COMMENT 'Structured error code (e.g. HL7_MISSING_PID)',
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
# MAGIC ## Helper functions
# MAGIC
# MAGIC `_split_hl7_batch_file` splits a file containing one or many HL7 messages.
# MAGIC Single-message files (the sample .txt files) return a one-element list.
# MAGIC
# MAGIC `_to_timestamp` converts an ISO 8601 string from BronzeHL7Record to a
# MAGIC naive UTC datetime suitable for PySpark TimestampType.
# MAGIC
# MAGIC `_detect_validation_issues` performs post-parse validation on a successfully
# MAGIC parsed Bronze record. Checks MSH-4, PID-5, PID-7, and PID-8. Returns a list
# MAGIC of structured error dicts matching the `audit_validation_errors` DDL schema.

# COMMAND ----------

import re

# HL7 v2.5 Table 0001 — Administrative Sex
_VALID_GENDER_CODES = {"M", "F", "O", "U", "A", "N", "C"}


def _split_hl7_batch_file(content: str) -> list:
    """
    Split file content into individual HL7 messages.
    Messages are delimited by lines beginning with 'MSH|'.
    Works for both single-message samples and large batch files.
    """
    parts = re.split(r"\n(?=MSH\|)", content.strip())
    return [p.strip() for p in parts if p.strip()]


def _to_timestamp(ts_str: str) -> datetime:
    """Convert ISO 8601 string from BronzeHL7Record.received_ts to naive UTC datetime."""
    if not ts_str:
        return datetime.now(timezone.utc).replace(tzinfo=None)
    try:
        return datetime.fromisoformat(ts_str.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return datetime.now(timezone.utc).replace(tzinfo=None)


def _detect_validation_issues(record, run_id: str) -> list:
    """
    Post-parse validation of a Bronze HL7 record (BronzeHL7Record dataclass).
    Only called for records with processing_status=PENDING.
    Returns validation_error dicts matching the audit_validation_errors DDL schema.

    Error codes:
      HL7_MISSING_MSH4   — sending_facility (MSH-4) blank or absent
      HL7_MISSING_PID5   — patient name (PID-5) blank or all-hat characters
      HL7_MALFORMED_DOB  — PID-7 present but not parseable as HL7 timestamp
      HL7_INVALID_GENDER — PID-8 value not in HL7 Table 0001 sex codes
    """
    issues = []
    now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    def _issue(code, detail, raw_val):
        return {
            "error_id":         str(uuid.uuid4()),
            "pipeline_run_id":  run_id,
            "ingestion_path":   "hl7",
            "source_record_id": record.message_id,
            "error_code":       code,
            "error_message":    detail,
            "raw_payload":      str(raw_val) if raw_val is not None else None,
            "tenant_id":        record.tenant_id,
            "requires_review":  True,
            "reviewed_at":      None,
            "reviewed_by":      None,
            "review_outcome":   None,
            "created_at":       now_ts,
        }

    # DQ-ADT-005: Missing MSH-4 (sending facility)
    if not record.sending_facility:
        issues.append(_issue(
            "HL7_MISSING_MSH4",
            "Sending facility (MSH-4) is blank or absent; tenant resolved via ZTN fallback",
            record.sending_facility,
        ))

    # PID-segment checks require inspecting raw_payload
    lines = record.raw_payload.strip().splitlines()
    pid_line = next((l for l in lines if l.startswith("PID|")), None)

    if pid_line:
        pid_fields = pid_line.split("|")

        # DQ-ADT-001: Missing PID-5 (patient name)
        pid5 = pid_fields[5].strip() if len(pid_fields) > 5 else ""
        if not pid5 or set(pid5).issubset({"^", " "}):
            issues.append(_issue(
                "HL7_MISSING_PID5",
                "Patient name (PID-5) is blank or contains only component separators",
                pid5,
            ))

        # DQ-ADT-002: Malformed DOB (PID-7)
        pid7 = pid_fields[7].strip() if len(pid_fields) > 7 else ""
        if pid7 and parse_hl7_timestamp(pid7) is None:
            issues.append(_issue(
                "HL7_MALFORMED_DOB",
                "Date of birth (PID-7) is present but cannot be parsed as an HL7 timestamp",
                pid7,
            ))

        # DQ-ADT-003: Invalid gender code (PID-8)
        pid8_raw = pid_fields[8].strip() if len(pid_fields) > 8 else ""
        pid8 = pid8_raw.upper()
        if pid8 and pid8 not in _VALID_GENDER_CODES:
            issues.append(_issue(
                "HL7_INVALID_GENDER",
                f"Administrative sex (PID-8) value '{pid8_raw}' is not in HL7 Table 0001",
                pid8_raw,
            ))

    return issues

# COMMAND ----------

# MAGIC %md
# MAGIC ## Source files
# MAGIC
# MAGIC All four HL7 source files are processed. Sample files contain one message each;
# MAGIC batch files contain many messages that are split before parsing.

# COMMAND ----------

SOURCE_FILES = [
    f"{REPO_ROOT}/data/synthetic/hl7_adt_sample.txt",
    f"{REPO_ROOT}/data/synthetic/hl7_oru_sample.txt",
    f"{REPO_ROOT}/data/synthetic/hl7_adt_batch.txt",
    f"{REPO_ROOT}/data/synthetic/hl7_oru_batch.txt",
]

for path in SOURCE_FILES:
    exists = os.path.exists(path)
    print(f"  {'OK' if exists else 'MISSING':7s}  {os.path.basename(path)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse messages per file
# MAGIC
# MAGIC Each file is split into individual HL7 messages, then parsed via
# MAGIC `parse_hl7_batch()`. Every message produces one Bronze row (PASS or ERROR).
# MAGIC Successfully-parsed rows are also checked for soft validation issues
# MAGIC (missing PID-5, malformed DOB, invalid gender, missing MSH-4) and any issues
# MAGIC are collected for `audit_validation_errors`.
# MAGIC
# MAGIC Row fields are mapped to `ingest_hl7_messages` DDL columns:
# MAGIC - `sending_application` (MSH-3) → `source_system`
# MAGIC - `sending_facility`    (MSH-4) → `source_facility`
# MAGIC - `message_type` split on "^"   → `message_type` (ADT) + `message_event` (A01)
# MAGIC - `processing_status` PENDING   → `validation_status` PASS
# MAGIC - `received_ts` STRING           → `received_at` TIMESTAMP

# COMMAND ----------

from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType, TimestampType,
)

# Module-level constants — consumed by tests/test_contracts.py for DDL alignment checks.
# Names match databricks/fhir_pipeline_ddl.sql table names.  Do not rename.

INGEST_HL7_MESSAGES_SCHEMA = StructType([
    StructField("message_id",        StringType(),    False),  # NOT NULL
    StructField("raw_payload",       StringType(),    False),  # NOT NULL
    StructField("message_type",      StringType(),    True),
    StructField("message_event",     StringType(),    True),
    StructField("tenant_id",         StringType(),    False),  # NOT NULL
    StructField("source_system",     StringType(),    True),
    StructField("source_facility",   StringType(),    True),
    StructField("received_at",       TimestampType(), False),  # NOT NULL
    StructField("validation_status", StringType(),    True),
    StructField("pipeline_run_id",   StringType(),    True),
])

AUDIT_VALIDATION_ERRORS_SCHEMA = StructType([
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

AUDIT_INGEST_LOG_SCHEMA = StructType([
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

all_rows             = []   # rows for ingest_hl7_messages
all_validation_errs  = []   # rows for audit_validation_errors
audit_entries        = []   # rows for audit_ingest_log (one per file)

for file_path in SOURCE_FILES:
    file_started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    with open(file_path, "r") as fh:
        content = fh.read()

    messages    = _split_hl7_batch_file(content)
    n_attempted = len(messages)

    records, parse_failures = parse_hl7_batch(
        messages,
        file_source=file_path,
        default_tenant_id=TENANT_ID,
    )

    # parse_hl7_batch returns ALL records (PENDING + ERROR) in records,
    # and only the failure details in parse_failures.
    n_failed    = len(parse_failures)
    n_succeeded = n_attempted - n_failed

    # Build Bronze rows mapped to DDL column names
    for record in records:
        mt_parts = (record.message_type or "").split("^")
        all_rows.append({
            "message_id":        record.message_id,
            "raw_payload":       record.raw_payload,
            "message_type":      mt_parts[0] if mt_parts[0] else None,
            "message_event":     mt_parts[1] if len(mt_parts) > 1 else None,
            "tenant_id":         record.tenant_id,
            "source_system":     record.sending_application,
            "source_facility":   record.sending_facility,
            "received_at":       _to_timestamp(record.received_ts),
            "validation_status": "PASS" if record.processing_status == "PENDING" else "ERROR",
            "pipeline_run_id":   pipeline_run_id,
        })

    # Validation errors from parse failures
    for failure in parse_failures:
        all_validation_errs.append({
            "error_id":         str(uuid.uuid4()),
            "pipeline_run_id":  pipeline_run_id,
            "ingestion_path":   "hl7",
            "source_record_id": None,
            "error_code":       "HL7_PARSE_FAILURE",
            "error_message":    failure.error,
            "raw_payload":      failure.raw_payload[:500] if failure.raw_payload else None,
            "tenant_id":        TENANT_ID,
            "requires_review":  True,
            "reviewed_at":      None,
            "reviewed_by":      None,
            "review_outcome":   None,
            "created_at":       datetime.now(timezone.utc).replace(tzinfo=None),
        })

    # Post-parse validation on PENDING records
    n_validation_issues = 0
    for record in records:
        if record.processing_status == "PENDING":
            issues = _detect_validation_issues(record, pipeline_run_id)
            all_validation_errs.extend(issues)
            n_validation_issues += len(issues)

    file_completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

    audit_entries.append({
        "log_id":           str(uuid.uuid4()),
        "pipeline_run_id":  pipeline_run_id,
        "ingestion_path":   "hl7",
        "source_table":     TARGET_TABLE,
        "record_count":     n_attempted,
        "pass_count":       n_succeeded,
        "error_count":      n_failed,
        "tenant_id":        TENANT_ID,
        "run_started_at":   file_started_at,
        "run_completed_at": file_completed_at,
        "logged_at":        datetime.now(timezone.utc).replace(tzinfo=None),
    })

    print(f"  {os.path.basename(file_path):30s}  "
          f"{n_attempted:5,} msgs  "
          f"{n_succeeded:5,} ok  "
          f"{n_failed:3} parse_errors  "
          f"{n_validation_issues:3} dq_issues")

total_rows      = len(all_rows)
total_dq        = len(all_validation_errs)
total_attempted = sum(e["record_count"] for e in audit_entries)
print(f"\nTotal: {total_attempted:,} messages → {total_rows:,} Bronze rows, "
      f"{total_dq:,} validation issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze HL7 rows to Delta table

# COMMAND ----------

df = spark.createDataFrame(all_rows, schema=INGEST_HL7_MESSAGES_SCHEMA)

df.write \
    .format("delta") \
    .mode("append") \
    .insertInto(TARGET_TABLE)

print(f"Wrote {df.count():,} row(s) to {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write validation errors to audit_validation_errors

# COMMAND ----------

if all_validation_errs:
    val_df = spark.createDataFrame(all_validation_errs, schema=AUDIT_VALIDATION_ERRORS_SCHEMA)
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

audit_df = spark.createDataFrame(audit_entries, schema=AUDIT_INGEST_LOG_SCHEMA)

audit_df.write \
    .format("delta") \
    .mode("append") \
    .insertInto(AUDIT_TABLE)

print(f"Audit log written: {len(audit_entries)} file(s) logged to {AUDIT_TABLE}")
for e in audit_entries:
    src = e["source_table"].split(".")[-1]
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
            source_facility,
            validation_status,
            message_type,
            message_event,
            COUNT(*)                    AS msg_count,
            COUNT(source_facility)      AS with_facility
        FROM {TARGET_TABLE}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        GROUP BY source_facility, validation_status, message_type, message_event
        ORDER BY source_facility, validation_status
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
