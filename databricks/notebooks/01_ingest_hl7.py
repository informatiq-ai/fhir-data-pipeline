# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingest HL7 v2 (Bronze)
# MAGIC
# MAGIC **Purpose:** Read synthetic HL7 v2 ADT and ORU messages, parse them using the
# MAGIC `ingestion/hl7_parser.py` logic, and land one row per message in the Bronze
# MAGIC Delta table. Every message lands — parse failures produce `processing_status=ERROR`
# MAGIC rows, never silent drops.
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_bronze.hl7_messages`
# MAGIC - `dev.fhir_bronze.audit_ingest_log`
# MAGIC
# MAGIC **Run order:** This is notebook 01 of 05. Run before `02_ingest_fhir.py`.

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
NOTEBOOK_NAME     = "01_ingest_hl7"
INGESTION_VERSION = "1.0.0"

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"target_table    : {TARGET_TABLE}")
print(f"audit_table     : {AUDIT_TABLE}")

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

from ingestion.hl7_parser import parse_hl7_message

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
        source_path         STRING      COMMENT 'Source file path(s) or table name',
        started_at          TIMESTAMP   NOT NULL COMMENT 'UTC timestamp when notebook started',
        completed_at        TIMESTAMP   COMMENT 'UTC timestamp when notebook finished',
        records_attempted   LONG        COMMENT 'Total records parsed or read',
        records_succeeded   LONG        COMMENT 'Records written successfully',
        records_failed      LONG        COMMENT 'Records that produced ERROR rows',
        status              STRING      NOT NULL COMMENT 'COMPLETED | PARTIAL | FAILED',
        error_detail        STRING      COMMENT 'Top-level exception message if status = FAILED',
        ingestion_version   STRING      COMMENT 'Pipeline version',
        created_ts          TIMESTAMP   NOT NULL COMMENT 'Row insert timestamp'
    )
    USING DELTA
    COMMENT 'Audit log for all ingestion notebook runs across notebooks 01 and 02.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

print(f"Tables verified: {TARGET_TABLE}, {AUDIT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read source HL7 files

# COMMAND ----------

SOURCE_FILES = [
    f"{REPO_ROOT}/data/synthetic/hl7_adt_sample.txt",
    f"{REPO_ROOT}/data/synthetic/hl7_oru_sample.txt",
]

raw_messages = []
for path in SOURCE_FILES:
    with open(path, "r") as fh:
        content = fh.read().strip()
    raw_messages.append((content, path))
    print(f"  loaded {len(content):,} chars  ← {os.path.basename(path)}")

print(f"\nTotal messages to parse: {len(raw_messages)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse messages and build row dicts
# MAGIC
# MAGIC Every message produces exactly one Bronze row.
# MAGIC Parse failures land with `processing_status=ERROR` — nothing is dropped.

# COMMAND ----------

started_at = datetime.now(timezone.utc)

rows     = []
n_failed = 0

for raw_msg, file_path in raw_messages:
    result = parse_hl7_message(
        raw_msg,
        file_source=file_path,
        default_tenant_id=TENANT_ID,
    )

    if result.success:
        row = asdict(result.record)
    else:
        n_failed += 1
        row = {
            "message_id":          str(uuid.uuid4()),
            "tenant_id":           TENANT_ID,
            "sending_application": None,
            "sending_facility":    None,
            "message_type":        None,
            "message_control_id":  None,
            "message_datetime":    None,
            "source_system":       None,
            "feed_type":           None,
            "batch_id":            None,
            "raw_payload":         raw_msg,
            "received_ts":         datetime.now(timezone.utc).isoformat(),
            "ingestion_version":   INGESTION_VERSION,
            "file_source":         file_path,
            "processing_status":   "ERROR",
            "processing_error":    result.error,
        }

    row["pipeline_run_id"] = pipeline_run_id
    rows.append(row)

n_succeeded = len(rows) - n_failed
print(f"Parsed {len(rows)} message(s): {n_succeeded} succeeded, {n_failed} error(s)")
for r in rows:
    print(f"  {r['processing_status']:8s}  tenant={r['tenant_id']}  type={r['message_type']}  "
          f"feed={r['feed_type']}  payload_len={len(r['raw_payload'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze rows to Delta table (PySpark)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType
)

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

df = spark.createDataFrame(rows, schema=hl7_schema)

df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(TARGET_TABLE)

completed_at = datetime.now(timezone.utc)
duration_s   = (completed_at - started_at).total_seconds()
print(f"Wrote {df.count()} row(s) to {TARGET_TABLE} in {duration_s:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write audit log entry

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from datetime import datetime, timezone

audit_rows = [{
    "log_id": str(uuid.uuid4()),
    "pipeline_run_id": pipeline_run_id,
    "ingestion_path": "fhir",  # or "hl7" or "csv" based on your pipeline
    "source_table": TARGET_TABLE,
    "record_count": len(rows),
    "pass_count": n_succeeded,
    "error_count": n_failed,
    "tenant_id": None,  # Set to actual tenant_id if applicable
    "run_started_at": started_at.replace(tzinfo=None),
    "run_completed_at": completed_at.replace(tzinfo=None),
    "logged_at": datetime.now(timezone.utc).replace(tzinfo=None)
}]

audit_schema = StructType([
    StructField("log_id", StringType(), nullable=False),
    StructField("pipeline_run_id", StringType(), nullable=False),
    StructField("ingestion_path", StringType(), nullable=True),
    StructField("source_table", StringType(), nullable=True),
    StructField("record_count", LongType(), nullable=True),
    StructField("pass_count", LongType(), nullable=True),
    StructField("error_count", LongType(), nullable=True),
    StructField("tenant_id", StringType(), nullable=True),
    StructField("run_started_at", TimestampType(), nullable=True),
    StructField("run_completed_at", TimestampType(), nullable=True),
    StructField("logged_at", TimestampType(), nullable=False)
])

audit_df = spark.createDataFrame(audit_rows, schema=audit_schema)

audit_df.write \
    .format("delta") \
    .mode("append") \
    .insertInto(AUDIT_TABLE)

print(f"Audit log written: status={'COMPLETED' if n_failed == 0 else 'PARTIAL'}  "
      f"attempted={len(rows)}  "
      f"succeeded={n_succeeded}  "
      f"failed={n_failed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — preview written rows

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            message_id,
            tenant_id,
            message_type,
            feed_type,
            message_control_id,
            message_datetime,
            processing_status,
            pipeline_run_id,
            LENGTH(raw_payload) AS raw_payload_chars
        FROM {TARGET_TABLE}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        ORDER BY received_ts
    """)
)

# COMMAND ----------

# DBTITLE 1,Return pipeline_run_id to orchestrator
# Return pipeline_run_id to orchestrator
dbutils.notebook.exit(pipeline_run_id)
