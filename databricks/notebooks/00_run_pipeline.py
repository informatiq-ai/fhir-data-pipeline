# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Pipeline Orchestrator
# MAGIC
# MAGIC **Purpose:** Run the full Bronze → Silver → Gold pipeline in sequence by
# MAGIC calling notebooks 01 through 04 via `dbutils.notebook.run()`. Each notebook
# MAGIC receives the `pipeline_run_id` from the previous stage so downstream
# MAGIC notebooks process only the records produced by this run.
# MAGIC
# MAGIC **Execution order:**
# MAGIC 1. `01_ingest_hl7`      — HL7 v2 → Bronze
# MAGIC 2. `02_ingest_fhir`     — FHIR R4 → Bronze
# MAGIC 3. `03_bronze_to_silver` — Bronze → Silver CDM
# MAGIC 4. `04_silver_to_gold`  — Silver → Gold analytics
# MAGIC
# MAGIC **Trigger:** Run this notebook manually, on a schedule, or via a
# MAGIC Databricks Workflow job. Each stage is idempotent — re-running a stage
# MAGIC appends new rows; it does not overwrite or delete existing data.
# MAGIC
# MAGIC **Timeout:** Each stage is given 30 minutes (`timeout_seconds=1800`).
# MAGIC Adjust for larger data volumes.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

orchestrator_run_id = str(uuid.uuid4())
started_at = datetime.now(timezone.utc)

print(f"orchestrator_run_id : {orchestrator_run_id}")
print(f"started_at          : {started_at.isoformat()}")
print(f"{'─' * 60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 1 — HL7 v2 Bronze Ingestion

# COMMAND ----------

print("Running 01_ingest_hl7 ...")

result_01 = dbutils.notebook.run(
    "01_ingest_hl7",
    timeout_seconds=1800,
    arguments={},
)

print(f"01_ingest_hl7 completed")
print(f"  result: {result_01}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2 — FHIR R4 Bronze Ingestion

# COMMAND ----------

print("Running 02_ingest_fhir ...")

result_02 = dbutils.notebook.run(
    "02_ingest_fhir",
    timeout_seconds=1800,
    arguments={},
)

# Extract the pipeline_run_id written by notebook 02 so notebook 03
# can scope its Bronze read to only this run's rows.
fhir_pipeline_run_id = result_02 if result_02 else ""

print(f"02_ingest_fhir completed")
print(f"  fhir_pipeline_run_id: {fhir_pipeline_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3 — Bronze → Silver Normalization

# COMMAND ----------

print("Running 03_bronze_to_silver ...")

result_03 = dbutils.notebook.run(
    "03_bronze_to_silver",
    timeout_seconds=1800,
    arguments={
        "upstream_pipeline_run_id": fhir_pipeline_run_id,
    },
)

silver_pipeline_run_id = result_03 if result_03 else ""

print(f"03_bronze_to_silver completed")
print(f"  silver_pipeline_run_id: {silver_pipeline_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 4 — Silver → Gold Analytics

# COMMAND ----------

print("Running 04_silver_to_gold ...")

result_04 = dbutils.notebook.run(
    "04_silver_to_gold",
    timeout_seconds=1800,
    arguments={
        "upstream_pipeline_run_id": silver_pipeline_run_id,
    },
)

print(f"04_silver_to_gold completed")
print(f"  result: {result_04}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

completed_at = datetime.now(timezone.utc)
duration_s   = (completed_at - started_at).total_seconds()

print(f"{'─' * 60}")
print(f"Pipeline run complete")
print(f"  orchestrator_run_id  : {orchestrator_run_id}")
print(f"  started_at           : {started_at.isoformat()}")
print(f"  completed_at         : {completed_at.isoformat()}")
print(f"  total_duration_s     : {duration_s:.1f}")
print(f"{'─' * 60}")
print(f"Stage results:")
print(f"  01_ingest_hl7        : {result_01 or 'OK'}")
print(f"  02_ingest_fhir       : {result_02 or 'OK'}")
print(f"  03_bronze_to_silver  : {result_03 or 'OK'}")
print(f"  04_silver_to_gold    : {result_04 or 'OK'}")

# Return the orchestrator run ID so a calling workflow can link audit records
dbutils.notebook.exit(orchestrator_run_id)
