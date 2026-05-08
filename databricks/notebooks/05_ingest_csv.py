# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 — Ingest CSV Batch (Bronze)
# MAGIC
# MAGIC **Purpose:** Read eClinicalWorks flat-file exports from SFTP drop, validate each row,
# MAGIC and land one immutable Bronze row per source file in `dev.fhir_bronze.ingest_csv_batches`.
# MAGIC Row-level DQ failures are routed to `audit_validation_errors` for human review.
# MAGIC This is the Bronze-layer entry point for the CSV batch pipeline (Job 2).
# MAGIC
# MAGIC **DQ rules — ecw_patients.csv:**
# MAGIC - `CSV_MISSING_REQUIRED_FIELD` — patient_id, last_name, dob, or zip is blank
# MAGIC - `CSV_DUPLICATE_RECORD`       — patient_id seen more than once in the file
# MAGIC - `CSV_MALFORMED_DOB`          — dob not in YYYY-MM-DD format
# MAGIC
# MAGIC **DQ rules — ecw_labs.csv:**
# MAGIC - `CSV_MISSING_REQUIRED_FIELD` — result_id, patient_id, test_code, or result_value is blank
# MAGIC - `CSV_NON_NUMERIC_RESULT`     — result_value cannot be parsed as a number
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_bronze.ingest_csv_batches`      — one row per source file (immutable)
# MAGIC - `dev.fhir_bronze.audit_validation_errors` — one row per DQ failure
# MAGIC - `dev.fhir_bronze.audit_ingest_log`        — one row per source file
# MAGIC
# MAGIC **Source files processed:**
# MAGIC - `data/synthetic/ecw_patients.csv`
# MAGIC - `data/synthetic/ecw_labs.csv`
# MAGIC
# MAGIC **Run order:** First task of Job 2 (CSV Batch Pipeline).
# MAGIC Downstream: `03_bronze_to_silver.py` → `04_silver_to_gold.py`

# COMMAND ----------

import csv
import io
import os
import re as _re
import uuid
from datetime import datetime, timezone

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

dbutils.widgets.text("tenant_id", "INTEGRIS_BAPTIST", "Tenant ID")
TENANT_ID       = dbutils.widgets.get("tenant_id")
CATALOG         = "dev"
BRONZE_SCHEMA   = "fhir_bronze"
TARGET_TABLE    = f"{CATALOG}.{BRONZE_SCHEMA}.ingest_csv_batches"
AUDIT_TABLE     = f"{CATALOG}.{BRONZE_SCHEMA}.audit_ingest_log"
VALIDATION_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.audit_validation_errors"
NOTEBOOK_NAME   = "05_ingest_csv"

print(f"pipeline_run_id  : {pipeline_run_id}")
print(f"tenant_id        : {TENANT_ID}")
print(f"target_table     : {TARGET_TABLE}")
print(f"audit_table      : {AUDIT_TABLE}")
print(f"validation_table : {VALIDATION_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve repo root

# COMMAND ----------

_nb_path = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)
REPO_ROOT = "/Workspace" + _nb_path.rsplit("/databricks/notebooks", 1)[0]
print(f"REPO_ROOT: {REPO_ROOT}")

ECW_PATIENTS_FILE = f"{REPO_ROOT}/data/synthetic/ecw_patients.csv"
ECW_LABS_FILE     = f"{REPO_ROOT}/data/synthetic/ecw_labs.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema constants and error codes
# MAGIC
# MAGIC All module-level constants are defined here — before any file I/O — so that
# MAGIC `test_contracts.py` and `test_notebook_imports.py` can import this notebook
# MAGIC as a module and assert on them without triggering filesystem access.
# MAGIC
# MAGIC Schema names match `databricks/fhir_pipeline_ddl.sql` exactly.
# MAGIC Do not add or rename columns here — the DDL is the single source of truth.

# COMMAND ----------

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── ingest_csv_batches (dev.fhir_bronze.ingest_csv_batches) ───────────────────
CSV_BATCHES_SCHEMA = StructType([
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

# ── audit_validation_errors (dev.fhir_bronze.audit_validation_errors) ─────────
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
VALIDATION_SCHEMA = AUDIT_VALIDATION_ERRORS_SCHEMA  # alias — tests/test_contracts.py checks this name

# ── audit_ingest_log (dev.fhir_bronze.audit_ingest_log) ───────────────────────
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

# ── DQ error code constants (module-level strings) ────────────────────────────
CSV_MISSING_REQUIRED_FIELD = "CSV_MISSING_REQUIRED_FIELD"
CSV_DUPLICATE_RECORD       = "CSV_DUPLICATE_RECORD"
CSV_MALFORMED_DOB          = "CSV_MALFORMED_DOB"
CSV_NON_NUMERIC_RESULT     = "CSV_NON_NUMERIC_RESULT"

print("Schema constants and error codes defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze tables (idempotent DDL)
# MAGIC
# MAGIC Schema matches `databricks/fhir_pipeline_ddl.sql` exactly.
# MAGIC `audit_ingest_log` and `audit_validation_errors` are shared across notebooks —
# MAGIC `CREATE TABLE IF NOT EXISTS` is safe to repeat.

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        batch_id          STRING    NOT NULL  COMMENT 'UUID generated at ingest',
        raw_payload       STRING    NOT NULL  COMMENT 'Full CSV content, immutable',
        source_system     STRING              COMMENT 'eClinicalWorks | Athena',
        batch_frequency   STRING              COMMENT 'hourly | daily | weekly',
        file_name         STRING              COMMENT 'Original SFTP file name',
        file_size_bytes   BIGINT              COMMENT 'File size at time of ingest',
        row_count         BIGINT              COMMENT 'Row count from file header or scan',
        tenant_id         STRING    NOT NULL  COMMENT 'Resolved from batch config',
        received_at       TIMESTAMP NOT NULL  COMMENT 'UTC timestamp at ingest boundary',
        validation_status STRING              COMMENT 'PASS | ERROR',
        pipeline_run_id   STRING              COMMENT 'Links to audit_ingest_log'
    )
    USING DELTA
    COMMENT 'Raw CSV batch files. Bronze = immutable. Supports Silver replay when standards change.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {VALIDATION_TABLE} (
        error_id          STRING    NOT NULL  COMMENT 'UUID',
        pipeline_run_id   STRING              COMMENT 'Links to audit_ingest_log',
        ingestion_path    STRING              COMMENT 'hl7 | fhir | csv',
        source_record_id  STRING              COMMENT 'FK to the originating ingest_* table',
        error_code        STRING              COMMENT 'Structured error code (e.g. CSV_MISSING_REQUIRED_FIELD)',
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
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
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
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print(f"Bronze tables verified: {TARGET_TABLE}")
print(f"                        {VALIDATION_TABLE}")
print(f"                        {AUDIT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper functions

# COMMAND ----------

_DOB_RE = _re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_valid_dob(s):
    """Return True only if s matches YYYY-MM-DD and is a real date."""
    if not s or not _DOB_RE.fullmatch(s.strip()):
        return False
    try:
        datetime.strptime(s.strip(), "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _validation_error(code, source_record_id, message, raw_val, run_id):
    return {
        "error_id":          str(uuid.uuid4()),
        "pipeline_run_id":   run_id,
        "ingestion_path":    "csv",
        "source_record_id":  source_record_id,
        "error_code":        code,
        "error_message":     message,
        "raw_payload":       str(raw_val)[:2000] if raw_val is not None else None,
        "tenant_id":         TENANT_ID,
        "requires_review":   True,
        "reviewed_at":       None,
        "reviewed_by":       None,
        "review_outcome":    None,
        "created_at":        datetime.now(timezone.utc).replace(tzinfo=None),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process ecw_patients.csv
# MAGIC
# MAGIC Required fields: patient_id, last_name, dob, zip
# MAGIC - Missing any required field → `CSV_MISSING_REQUIRED_FIELD`
# MAGIC - Duplicate patient_id          → `CSV_DUPLICATE_RECORD`
# MAGIC - dob not in YYYY-MM-DD format  → `CSV_MALFORMED_DOB`

# COMMAND ----------

patients_started = datetime.now(timezone.utc).replace(tzinfo=None)
patients_validation_errs = []
seen_patient_ids = {}  # patient_id → first row index (duplicate detection)

PATIENT_REQUIRED = ("patient_id", "last_name", "dob", "zip")

with open(ECW_PATIENTS_FILE, newline="", encoding="utf-8") as fh:
    raw_patients_content = fh.read()

patients_file_size = os.path.getsize(ECW_PATIENTS_FILE)
patients_total_rows = 0

reader = csv.DictReader(io.StringIO(raw_patients_content))
for row_idx, row in enumerate(reader):
    patients_total_rows += 1
    patient_id = row.get("patient_id", "").strip()
    source_id  = patient_id or f"row_{row_idx}"
    raw_repr   = dict(row)

    # Required field check
    for field in PATIENT_REQUIRED:
        val = row.get(field, "").strip()
        if not val:
            patients_validation_errs.append(_validation_error(
                CSV_MISSING_REQUIRED_FIELD,
                source_id,
                f"Required field '{field}' is blank or missing",
                raw_repr,
                pipeline_run_id,
            ))

    # Duplicate patient_id
    if patient_id:
        if patient_id in seen_patient_ids:
            patients_validation_errs.append(_validation_error(
                CSV_DUPLICATE_RECORD,
                source_id,
                f"patient_id '{patient_id}' seen again (first at row {seen_patient_ids[patient_id]}); "
                "MPI will return same UMPI via deterministic match",
                patient_id,
                pipeline_run_id,
            ))
        else:
            seen_patient_ids[patient_id] = row_idx

    # Malformed DOB
    dob_raw = row.get("dob", "").strip()
    if dob_raw and not _is_valid_dob(dob_raw):
        patients_validation_errs.append(_validation_error(
            CSV_MALFORMED_DOB,
            source_id,
            f"dob '{dob_raw}' is not a valid YYYY-MM-DD date; date_of_birth will be NULL in Silver",
            dob_raw,
            pipeline_run_id,
        ))

patients_completed = datetime.now(timezone.utc).replace(tzinfo=None)
patients_error_count = len(patients_validation_errs)
patients_pass_count  = patients_total_rows - sum(
    1 for e in patients_validation_errs
    if e["error_code"] == CSV_MISSING_REQUIRED_FIELD
)

print(f"ecw_patients.csv processed")
print(f"  Total rows         : {patients_total_rows:,}")
print(f"  Validation errors  : {patients_error_count:,}")
print(f"  File size (bytes)  : {patients_file_size:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process ecw_labs.csv
# MAGIC
# MAGIC Required fields: result_id, patient_id, test_code, result_value
# MAGIC - Missing any required field → `CSV_MISSING_REQUIRED_FIELD`
# MAGIC - result_value non-numeric    → `CSV_NON_NUMERIC_RESULT`

# COMMAND ----------

labs_started = datetime.now(timezone.utc).replace(tzinfo=None)
labs_validation_errs = []

LAB_REQUIRED = ("result_id", "patient_id", "test_code", "result_value")

with open(ECW_LABS_FILE, newline="", encoding="utf-8") as fh:
    raw_labs_content = fh.read()

labs_file_size = os.path.getsize(ECW_LABS_FILE)
labs_total_rows = 0

reader = csv.DictReader(io.StringIO(raw_labs_content))
for row in reader:
    labs_total_rows += 1
    result_id = row.get("result_id", "").strip()
    source_id  = result_id or f"row_{labs_total_rows}"
    raw_repr   = dict(row)

    # Required field check
    for field in LAB_REQUIRED:
        val = row.get(field, "").strip()
        if not val:
            labs_validation_errs.append(_validation_error(
                CSV_MISSING_REQUIRED_FIELD,
                source_id,
                f"Required field '{field}' is blank or missing",
                raw_repr,
                pipeline_run_id,
            ))

    # Non-numeric result_value
    result_value_raw = row.get("result_value", "").strip()
    if result_value_raw:
        try:
            float(result_value_raw)
        except (ValueError, TypeError):
            labs_validation_errs.append(_validation_error(
                CSV_NON_NUMERIC_RESULT,
                source_id,
                f"result_value '{result_value_raw}' cannot be parsed as a number; "
                "value_quantity will be NULL in Silver",
                result_value_raw,
                pipeline_run_id,
            ))

labs_completed = datetime.now(timezone.utc).replace(tzinfo=None)
labs_error_count = len(labs_validation_errs)

print(f"ecw_labs.csv processed")
print(f"  Total rows         : {labs_total_rows:,}")
print(f"  Validation errors  : {labs_error_count:,}")
print(f"  File size (bytes)  : {labs_file_size:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze batch rows — one row per source file
# MAGIC
# MAGIC `raw_payload` stores the full file content, immutable.
# MAGIC `validation_status` = PASS if no errors detected, ERROR if any DQ issues found.
# MAGIC The `source_system` field distinguishes patients ('eClinicalWorks') from labs ('ECW_LABS').

# COMMAND ----------

batch_rows = [
    {
        "batch_id":          str(uuid.uuid4()),
        "raw_payload":       raw_patients_content,
        "source_system":     "eClinicalWorks",
        "batch_frequency":   "daily",
        "file_name":         os.path.basename(ECW_PATIENTS_FILE),
        "file_size_bytes":   patients_file_size,
        "row_count":         patients_total_rows,
        "tenant_id":         TENANT_ID,
        "received_at":       patients_started,
        "validation_status": "ERROR" if patients_validation_errs else "PASS",
        "pipeline_run_id":   pipeline_run_id,
    },
    {
        "batch_id":          str(uuid.uuid4()),
        "raw_payload":       raw_labs_content,
        "source_system":     "ECW_LABS",
        "batch_frequency":   "daily",
        "file_name":         os.path.basename(ECW_LABS_FILE),
        "file_size_bytes":   labs_file_size,
        "row_count":         labs_total_rows,
        "tenant_id":         TENANT_ID,
        "received_at":       labs_started,
        "validation_status": "ERROR" if labs_validation_errs else "PASS",
        "pipeline_run_id":   pipeline_run_id,
    },
]

batch_df = spark.createDataFrame(batch_rows, schema=CSV_BATCHES_SCHEMA)
batch_df.write.format("delta").mode("append").insertInto(TARGET_TABLE)
print(f"Wrote {len(batch_rows)} batch row(s) to {TARGET_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write validation errors to audit_validation_errors

# COMMAND ----------

all_validation_errs = patients_validation_errs + labs_validation_errs

if all_validation_errs:
    val_df = spark.createDataFrame(all_validation_errs, schema=VALIDATION_SCHEMA)
    val_df.write.format("delta").mode("append").insertInto(VALIDATION_TABLE)
    print(f"Wrote {val_df.count():,} validation error row(s) to {VALIDATION_TABLE}")
else:
    print("No validation errors — skipping write to audit_validation_errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write audit log — one row per source file

# COMMAND ----------

audit_entries = [
    {
        "log_id":            str(uuid.uuid4()),
        "pipeline_run_id":   pipeline_run_id,
        "ingestion_path":    "csv",
        "source_table":      TARGET_TABLE,
        "record_count":      patients_total_rows,
        "pass_count":        patients_total_rows - len([
                                 e for e in patients_validation_errs
                                 if e["error_code"] == CSV_MISSING_REQUIRED_FIELD
                             ]),
        "error_count":       len([
                                 e for e in patients_validation_errs
                                 if e["error_code"] == CSV_MISSING_REQUIRED_FIELD
                             ]),
        "tenant_id":         TENANT_ID,
        "run_started_at":    patients_started,
        "run_completed_at":  patients_completed,
        "logged_at":         datetime.now(timezone.utc).replace(tzinfo=None),
    },
    {
        "log_id":            str(uuid.uuid4()),
        "pipeline_run_id":   pipeline_run_id,
        "ingestion_path":    "csv",
        "source_table":      TARGET_TABLE,
        "record_count":      labs_total_rows,
        "pass_count":        labs_total_rows - len([
                                 e for e in labs_validation_errs
                                 if e["error_code"] == CSV_MISSING_REQUIRED_FIELD
                             ]),
        "error_count":       len([
                                 e for e in labs_validation_errs
                                 if e["error_code"] == CSV_MISSING_REQUIRED_FIELD
                             ]),
        "tenant_id":         TENANT_ID,
        "run_started_at":    labs_started,
        "run_completed_at":  labs_completed,
        "logged_at":         datetime.now(timezone.utc).replace(tzinfo=None),
    },
]

audit_df = spark.createDataFrame(audit_entries, schema=AUDIT_INGEST_LOG_SCHEMA)
audit_df.write.format("delta").mode("append").insertInto(AUDIT_TABLE)
print(f"Audit log written: {len(audit_entries)} file(s) logged to {AUDIT_TABLE}")
for e in audit_entries:
    print(f"  file={e['source_table'].split('.')[-1]}  "
          f"records={e['record_count']:,}  "
          f"pass={e['pass_count']:,}  "
          f"errors={e['error_count']:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

total_errors = len(all_validation_errs)
patients_dq_breakdown = {}
labs_dq_breakdown = {}
for e in patients_validation_errs:
    patients_dq_breakdown[e["error_code"]] = patients_dq_breakdown.get(e["error_code"], 0) + 1
for e in labs_validation_errs:
    labs_dq_breakdown[e["error_code"]] = labs_dq_breakdown.get(e["error_code"], 0) + 1

print("=" * 60)
print(f"CSV batch ingest complete  |  pipeline_run_id: {pipeline_run_id}")
print("=" * 60)
print(f"\necw_patients.csv:")
print(f"  rows ingested      : {patients_total_rows:,}")
print(f"  validation errors  : {patients_error_count:,}")
for code, count in sorted(patients_dq_breakdown.items()):
    print(f"    {code:<35} {count:,}")
print(f"\necw_labs.csv:")
print(f"  rows ingested      : {labs_total_rows:,}")
print(f"  validation errors  : {labs_error_count:,}")
for code, count in sorted(labs_dq_breakdown.items()):
    print(f"    {code:<35} {count:,}")
print(f"\nTotal DQ issues written to audit_validation_errors: {total_errors:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — preview batch rows

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        file_name,
        source_system,
        row_count,
        validation_status,
        pipeline_run_id
    FROM {TARGET_TABLE}
    WHERE pipeline_run_id = '{pipeline_run_id}'
    ORDER BY file_name
"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        error_code,
        COUNT(*) AS issue_count
    FROM {VALIDATION_TABLE}
    WHERE pipeline_run_id = '{pipeline_run_id}'
    GROUP BY error_code
    ORDER BY issue_count DESC
"""))

# COMMAND ----------

# DBTITLE 1,Return pipeline_run_id to orchestrator
dbutils.notebook.exit(pipeline_run_id)
