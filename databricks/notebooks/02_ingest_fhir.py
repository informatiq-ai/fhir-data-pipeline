# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — Ingest FHIR R4 Bundle (Bronze)
# MAGIC
# MAGIC **Purpose:** Read the synthetic FHIR R4 transaction Bundle, split it into
# MAGIC per-resource rows using `ingestion/fhir_ingester.py` logic, and land one
# MAGIC Bronze row per resource in the target Delta table. The full Bundle JSON is
# MAGIC preserved on the first resource row for audit; subsequent rows carry only
# MAGIC their own resource payload to avoid row bloat.
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_bronze.fhir_resources`
# MAGIC - `dev.fhir_bronze.audit_ingest_log`
# MAGIC
# MAGIC **Run order:** Notebook 02 of 05. Run after `01_ingest_hl7.py`,
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
NOTEBOOK_NAME     = "02_ingest_fhir"
INGESTION_VERSION = "1.0.0"

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"target_table    : {TARGET_TABLE}")
print(f"audit_table     : {AUDIT_TABLE}")

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

from ingestion.fhir_ingester import ingest_fhir_bundle

print("fhir_ingester imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create catalog, schema, and target tables (idempotent DDL)

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

# audit_ingest_log is shared with notebook 01 — CREATE IF NOT EXISTS is safe to repeat
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        log_id              STRING      NOT NULL,
        pipeline_run_id     STRING      NOT NULL,
        notebook_name       STRING      NOT NULL,
        target_table        STRING      NOT NULL,
        source_path         STRING,
        started_at          TIMESTAMP   NOT NULL,
        completed_at        TIMESTAMP,
        records_attempted   LONG,
        records_succeeded   LONG,
        records_failed      LONG,
        status              STRING      NOT NULL,
        error_detail        STRING,
        ingestion_version   STRING,
        created_ts          TIMESTAMP   NOT NULL
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
# MAGIC ## Read source FHIR Bundle file

# COMMAND ----------

SOURCE_FILE = f"{REPO_ROOT}/data/synthetic/fhir_bundle_sample.json"

with open(SOURCE_FILE, "r") as fh:
    raw_bundle = fh.read()

bundle_preview = json.loads(raw_bundle)
print(f"Bundle ID       : {bundle_preview.get('id')}")
print(f"Bundle type     : {bundle_preview.get('type')}")
print(f"Resource count  : {len(bundle_preview.get('entry', []))}")
print(f"Raw size        : {len(raw_bundle):,} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Bundle and build row dicts
# MAGIC
# MAGIC `ingest_fhir_bundle` splits the Bundle into per-resource records.
# MAGIC Structural failures produce a single ERROR record — nothing is dropped.

# COMMAND ----------

started_at = datetime.now(timezone.utc)

result = ingest_fhir_bundle(
    raw_bundle,
    default_tenant_id=TENANT_ID,
    store_full_bundle=True,
)

print(f"Ingest success  : {result.success}")
print(f"Bundle ID       : {result.bundle_id}")
print(f"Resource count  : {result.resource_count}")
print(f"Skipped types   : {result.skipped_resource_types}")
if not result.success:
    print(f"Error           : {result.error}")

rows     = []
n_failed = 0

for rec in result.records:
    row = asdict(rec)
    # raw_payload and bundle_payload are already JSON strings from the ingester
    row["pipeline_run_id"] = pipeline_run_id
    if rec.processing_status == "ERROR":
        n_failed += 1
    rows.append(row)

n_succeeded = len(rows) - n_failed
print(f"\nRows built: {len(rows)} ({n_succeeded} succeeded, {n_failed} error(s))")
for r in rows:
    print(f"  {r['processing_status']:8s}  type={r['fhir_resource_type']:<15}  "
          f"tenant={r['tenant_id']}  bundle_payload={'yes' if r.get('bundle_payload') else 'null'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze rows to Delta table (PySpark)

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

df = spark.createDataFrame(rows, schema=fhir_schema)

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

from pyspark.sql.types import LongType, TimestampType

audit_rows = [{
    "log_id":            str(uuid.uuid4()),
    "pipeline_run_id":   pipeline_run_id,
    "notebook_name":     NOTEBOOK_NAME,
    "target_table":      TARGET_TABLE,
    "source_path":       SOURCE_FILE,
    "started_at":        started_at.replace(tzinfo=None),
    "completed_at":      completed_at.replace(tzinfo=None),
    "records_attempted": len(rows),
    "records_succeeded": n_succeeded,
    "records_failed":    n_failed,
    "status":            "COMPLETED" if n_failed == 0 else "PARTIAL",
    "error_detail":      result.error if not result.success else None,
    "ingestion_version": INGESTION_VERSION,
    "created_ts":        datetime.utcnow(),
}]

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

audit_df = spark.createDataFrame(audit_rows, schema=audit_schema)

audit_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(AUDIT_TABLE)

print(f"Audit log written: status={audit_rows[0]['status']}  "
      f"attempted={audit_rows[0]['records_attempted']}  "
      f"succeeded={audit_rows[0]['records_succeeded']}  "
      f"failed={audit_rows[0]['records_failed']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — preview written rows

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            resource_id,
            tenant_id,
            fhir_resource_type,
            fhir_resource_id,
            bundle_id,
            processing_status,
            pipeline_run_id,
            CASE WHEN bundle_payload IS NOT NULL THEN 'yes' ELSE 'null' END AS has_bundle_payload,
            LENGTH(raw_payload) AS raw_payload_chars
        FROM {TARGET_TABLE}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        ORDER BY fhir_resource_type
    """)
)
