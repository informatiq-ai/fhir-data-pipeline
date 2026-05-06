# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest HL7 v2 (Bronze)
# MAGIC
# MAGIC **Purpose:** Read HL7 v2 ADT and ORU messages from four source files (both
# MAGIC single-message samples and volume batch files), parse them using the
# MAGIC `ingestion/hl7_parser.py` logic, and land one row per message in the Bronze
# MAGIC Delta table. Every message lands — parse failures produce `processing_status=ERROR`
# MAGIC rows, never silent drops. Post-parse validation issues are captured in a
# MAGIC structured `audit_validation_errors` table with error codes.
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_bronze.hl7_messages`          — one row per message
# MAGIC - `dev.fhir_bronze.audit_validation_errors` — structured DQ issues per message
# MAGIC - `dev.fhir_bronze.audit_ingest_log`       — one row per source file
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
from dataclasses import asdict
from datetime import datetime, timezone

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

TENANT_ID         = "INTEGRIS_BAPTIST"
CATALOG           = "dev"
SCHEMA            = "fhir_bronze"
TARGET_TABLE      = f"{CATALOG}.{SCHEMA}.hl7_messages"
AUDIT_TABLE       = f"{CATALOG}.{SCHEMA}.audit_ingest_log"
VALIDATION_TABLE  = f"{CATALOG}.{SCHEMA}.audit_validation_errors"
NOTEBOOK_NAME     = "01_ingest_hl7"
INGESTION_VERSION = "1.0.0"

print(f"pipeline_run_id   : {pipeline_run_id}")
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

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        message_id          STRING  NOT NULL COMMENT 'Surrogate key — UUID generated at ingest',
        tenant_id           STRING  NOT NULL COMMENT 'Tenant identifier from ZTN segment or config',
        sending_application STRING  COMMENT 'MSH.3 sending application',
        sending_facility    STRING  COMMENT 'MSH.4 sending facility',
        message_type        STRING  COMMENT 'MSH.9 message type (e.g. ADT^A01)',
        message_control_id  STRING  COMMENT 'MSH.10 message control ID',
        message_datetime    STRING  COMMENT 'MSH.7 datetime as ISO 8601 string',
        source_system       STRING  COMMENT 'Source EHR system identifier',
        feed_type           STRING  COMMENT 'ADT, ORU, SIU, VXU',
        batch_id            STRING  COMMENT 'Batch identifier from ZTN segment',
        raw_payload         STRING  NOT NULL COMMENT 'Full original HL7 message — never modified',
        received_ts         STRING  NOT NULL COMMENT 'ISO 8601 UTC timestamp when message arrived',
        ingestion_version   STRING  COMMENT 'Pipeline version that wrote this row',
        file_source         STRING  COMMENT 'SFTP path, MQ topic, or file path',
        processing_status   STRING  NOT NULL COMMENT 'PENDING | ERROR',
        processing_error    STRING  COMMENT 'Exception detail if processing_status = ERROR',
        pipeline_run_id     STRING  NOT NULL COMMENT 'UUID linking all rows from one notebook run'
    )
    USING DELTA
    COMMENT 'Bronze landing table for HL7 v2 messages. Immutable after insert.'
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
        records_attempted   LONG        COMMENT 'Total messages parsed from this file',
        records_succeeded   LONG        COMMENT 'Messages written with PENDING status',
        records_failed      LONG        COMMENT 'Messages written with ERROR status',
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
        source_file         STRING      NOT NULL COMMENT 'Source file that contained this message',
        message_id          STRING      COMMENT 'Bronze message_id of the affected message',
        message_control_id  STRING      COMMENT 'MSH.10 control ID from the message',
        error_code          STRING      NOT NULL COMMENT 'Structured error code (e.g. HL7_MISSING_PID5)',
        error_field         STRING      COMMENT 'HL7 field reference (e.g. PID-5)',
        error_detail        STRING      COMMENT 'Human-readable description of the issue',
        raw_value           STRING      COMMENT 'The raw field value that triggered the error',
        severity            STRING      NOT NULL COMMENT 'WARNING | ERROR',
        logged_at           TIMESTAMP   NOT NULL COMMENT 'When this error was recorded'
    )
    USING DELTA
    COMMENT 'Structured validation errors for Bronze HL7 ingestion. One row per issue per message.'
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
# MAGIC `_detect_validation_issues` performs post-parse validation on a successfully
# MAGIC parsed Bronze record. Checks MSH-4, PID-5, PID-7, and PID-8. Returns a list
# MAGIC of structured error dicts for insertion into `audit_validation_errors`.

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


def _detect_validation_issues(record, source_file: str, run_id: str) -> list:
    """
    Post-parse validation of a Bronze HL7 record (BronzeHL7Record dataclass).
    Only called for records with processing_status=PENDING.
    Returns a list of validation_error dicts.

    Error codes:
      HL7_MISSING_MSH4   — sending_facility (MSH-4) blank or absent
      HL7_MISSING_PID5   — patient name (PID-5) blank or all-hat characters
      HL7_MALFORMED_DOB  — PID-7 present but not parseable as HL7 timestamp
      HL7_INVALID_GENDER — PID-8 value not in HL7 Table 0001 sex codes
    """
    issues = []
    now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    def _issue(code, field, detail, raw_val, severity="WARNING"):
        return {
            "error_id":           str(uuid.uuid4()),
            "pipeline_run_id":    run_id,
            "source_file":        source_file,
            "message_id":         record.message_id,
            "message_control_id": record.message_control_id,
            "error_code":         code,
            "error_field":        field,
            "error_detail":       detail,
            "raw_value":          raw_val,
            "severity":           severity,
            "logged_at":          now_ts,
        }

    # DQ-ADT-005: Missing MSH-4 (sending facility)
    if not record.sending_facility:
        issues.append(_issue(
            "HL7_MISSING_MSH4", "MSH-4",
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
                "HL7_MISSING_PID5", "PID-5",
                "Patient name (PID-5) is blank or contains only component separators",
                pid5,
            ))

        # DQ-ADT-002: Malformed DOB (PID-7)
        pid7 = pid_fields[7].strip() if len(pid_fields) > 7 else ""
        if pid7 and parse_hl7_timestamp(pid7) is None:
            issues.append(_issue(
                "HL7_MALFORMED_DOB", "PID-7",
                "Date of birth (PID-7) is present but cannot be parsed as an HL7 timestamp",
                pid7,
            ))

        # DQ-ADT-003: Invalid gender code (PID-8)
        pid8_raw = pid_fields[8].strip() if len(pid_fields) > 8 else ""
        pid8 = pid8_raw.upper()
        if pid8 and pid8 not in _VALID_GENDER_CODES:
            issues.append(_issue(
                "HL7_INVALID_GENDER", "PID-8",
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
# MAGIC `parse_hl7_batch()`. Every message produces one Bronze row (PENDING or ERROR).
# MAGIC Successfully-parsed rows are also checked for soft validation issues
# MAGIC (missing PID-5, malformed DOB, invalid gender, missing MSH-4) and any issues
# MAGIC are collected for `audit_validation_errors`.

# COMMAND ----------

all_rows             = []   # rows for hl7_messages
all_validation_errs  = []   # rows for audit_validation_errors
audit_entries        = []   # rows for audit_ingest_log (one per file)

for file_path in SOURCE_FILES:
    file_started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    with open(file_path, "r") as fh:
        content = fh.read()

    messages  = _split_hl7_batch_file(content)
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

    # Collect Bronze rows
    for record in records:
        row = asdict(record)
        row["pipeline_run_id"] = pipeline_run_id
        all_rows.append(row)

    # Validation errors from parse failures
    for failure in parse_failures:
        all_validation_errs.append({
            "error_id":           str(uuid.uuid4()),
            "pipeline_run_id":    pipeline_run_id,
            "source_file":        file_path,
            "message_id":         None,
            "message_control_id": None,
            "error_code":         "HL7_PARSE_FAILURE",
            "error_field":        None,
            "error_detail":       failure.error,
            "raw_value":          failure.raw_payload[:200] if failure.raw_payload else None,
            "severity":           "ERROR",
            "logged_at":          datetime.now(timezone.utc).replace(tzinfo=None),
        })

    # Post-parse validation on PENDING records
    n_validation_issues = 0
    for record in records:
        if record.processing_status == "PENDING":
            issues = _detect_validation_issues(record, file_path, pipeline_run_id)
            all_validation_errs.extend(issues)
            n_validation_issues += len(issues)

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

    print(f"  {os.path.basename(file_path):30s}  "
          f"{n_attempted:5,} msgs  "
          f"{n_succeeded:5,} ok  "
          f"{n_failed:3} parse_errors  "
          f"{n_validation_issues:3} dq_issues")

total_rows    = len(all_rows)
total_dq      = len(all_validation_errs)
total_attempted = sum(e["records_attempted"] for e in audit_entries)
print(f"\nTotal: {total_attempted:,} messages → {total_rows:,} Bronze rows, "
      f"{total_dq:,} validation issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze HL7 rows to Delta table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

hl7_schema = StructType([
    StructField("message_id",          StringType(), False),
    StructField("tenant_id",           StringType(), False),
    StructField("sending_application", StringType(), True),
    StructField("sending_facility",    StringType(), True),
    StructField("message_type",        StringType(), True),
    StructField("message_control_id",  StringType(), True),
    StructField("message_datetime",    StringType(), True),
    StructField("source_system",       StringType(), True),
    StructField("feed_type",           StringType(), True),
    StructField("batch_id",            StringType(), True),
    StructField("raw_payload",         StringType(), False),
    StructField("received_ts",         StringType(), False),
    StructField("ingestion_version",   StringType(), True),
    StructField("file_source",         StringType(), True),
    StructField("processing_status",   StringType(), False),
    StructField("processing_error",    StringType(), True),
    StructField("pipeline_run_id",     StringType(), False),
])

df = spark.createDataFrame(all_rows, schema=hl7_schema)

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
    print(f"  {os.path.basename(e['source_path']):30s}  "
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
            file_source,
            processing_status,
            feed_type,
            COUNT(*)                    AS msg_count,
            COUNT(message_control_id)   AS with_control_id,
            COUNT(sending_facility)     AS with_facility
        FROM {TARGET_TABLE}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        GROUP BY file_source, processing_status, feed_type
        ORDER BY file_source, processing_status
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
