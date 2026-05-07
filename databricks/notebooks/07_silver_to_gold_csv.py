# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 07 — Silver → Gold: CSV Batch Path
# MAGIC
# MAGIC **Purpose:** Final task in Job 2. Delegates to `04_silver_to_gold.py`, which reads
# MAGIC all Silver tables (HL7, FHIR, and CSV-sourced) and writes unified Gold output.
# MAGIC Gold tables are source-agnostic by design — no separate CSV Gold tables needed.
# MAGIC
# MAGIC No analytics logic lives here. This notebook is a thin orchestration wrapper.
# MAGIC
# MAGIC **Run order:** Task 3 of Job 2. Run after `06_bronze_to_silver_csv.py`.

# COMMAND ----------

import uuid

pipeline_run_id = str(uuid.uuid4())

NOTEBOOK_NAME = "07_silver_to_gold_csv"

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"Delegating Gold layer to 04_silver_to_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget: upstream pipeline_run_id

# COMMAND ----------

dbutils.widgets.text(
    "upstream_pipeline_run_id",
    "",
    "Pipeline Run ID from notebook 06 (passed through to 04_silver_to_gold)",
)
upstream_pipeline_run_id = dbutils.widgets.get("upstream_pipeline_run_id").strip()

if upstream_pipeline_run_id:
    print(f"upstream_pipeline_run_id: {upstream_pipeline_run_id}")
else:
    print("No upstream_pipeline_run_id — 04_silver_to_gold will process all Silver rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve notebook path and delegate to 04_silver_to_gold

# COMMAND ----------

_nb_path = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)

notebook_path = _nb_path.rsplit("/", 1)[0] + "/04_silver_to_gold"
print(f"Delegating to: {notebook_path}")

result = dbutils.notebook.run(
    notebook_path,
    timeout_seconds=3600,
    arguments={"upstream_pipeline_run_id": upstream_pipeline_run_id},
)
print(f"04_silver_to_gold completed — pipeline_run_id: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Return pipeline_run_id to orchestrator

# COMMAND ----------

dbutils.notebook.exit(result)
