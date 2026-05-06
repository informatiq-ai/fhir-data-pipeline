# Databricks Notebooks

This directory contains Databricks notebooks that execute the full Bronze → Silver → Gold
pipeline against a Unity Catalog environment. The notebooks are a thin orchestration layer
over the Python modules in `ingestion/` and `transforms/` — all business logic lives there.

---

## Notebook Execution Order

| Notebook | Stage | Reads from | Writes to |
|---|---|---|---|
| `00_run_pipeline.py` | Orchestrator | — | Calls 01–04 in sequence |
| `01_ingest_hl7.py` | Bronze | `data/synthetic/hl7_*.txt` | `dev.fhir_bronze.hl7_messages` |
| `02_ingest_fhir.py` | Bronze | `data/synthetic/fhir_bundle_sample.json` | `dev.fhir_bronze.fhir_resources` |
| `03_bronze_to_silver.py` | Silver | `dev.fhir_bronze.fhir_resources` | `dev.fhir_silver.*` (6 tables) |
| `04_silver_to_gold.py` | Gold | `dev.fhir_silver.*` | `dev.fhir_gold.*` (3 tables) |

Run notebooks individually in order (01 → 02 → 03 → 04), or trigger `00_run_pipeline.py`
to execute the full sequence automatically.

---

## Catalog and Schema Structure

```
dev (catalog)
├── fhir_bronze (schema)
│   ├── hl7_messages          — Raw HL7 v2 messages, one row per message
│   ├── fhir_resources        — Raw FHIR R4 resources, one row per resource
│   └── audit_ingest_log      — Audit record for every notebook 01/02 run
├── fhir_silver (schema)
│   ├── master_patient_index  — UMPI assignments (deterministic MPI)
│   ├── encounters            — Normalized encounter records
│   ├── lab_observations      — LOINC-mapped observations
│   ├── diagnoses             — ICD-10 + SNOMED dual-coded diagnoses
│   ├── normalization_log     — Audit trail for every terminology mapping
│   └── terminology_unmapped_codes — Codes with no mapping (action queue)
└── fhir_gold (schema)
    ├── patient_summary       — Population health profile + Charlson risk
    ├── quality_measures      — HEDIS CDC HbA1c Control measure results
    ├── adt_event_feed        — ADT events with 30-day readmission flag
    └── audit_gold_log        — Audit record for every notebook 04 run
```

All tables use Delta Lake format with Change Data Feed enabled.

---

## Importing into Databricks

### Option 1 — Databricks Repos (recommended)

1. In the Databricks workspace sidebar, go to **Repos → Add Repo**.
2. Enter the repository URL and click **Create Repo**.
3. The repo clones under `/Workspace/Repos/<your-user>/fhir-data-pipeline/`.
4. Open any notebook in the `databricks/notebooks/` folder and attach a cluster.

The notebooks derive `REPO_ROOT` automatically from the running notebook's workspace
path, so `ingestion/` and `transforms/` modules are importable without manual
`sys.path` configuration.

### Option 2 — Manual import

1. In the Databricks workspace, navigate to a folder.
2. Click **Import** and upload each `.py` file from `databricks/notebooks/`.
3. Before running, set `REPO_ROOT` manually at the top of each notebook to the
   path where the repo modules (`ingestion/`, `transforms/`) are accessible,
   or install them as a library on the cluster.

---

## Cluster Requirements

| Requirement | Minimum |
|---|---|
| Databricks Runtime | 13.3 LTS (Python 3.10+) |
| Unity Catalog | Required (three-part table names: `catalog.schema.table`) |
| Python packages | `hl7apy`, `fhir.resources` (install via cluster init script or `%pip install`) |

Install packages on the cluster before running:

```python
%pip install hl7apy fhir.resources
```

Or add to the cluster's init script:

```bash
pip install hl7apy fhir.resources
```

---

## Widget: upstream_pipeline_run_id

Notebooks 03 and 04 accept an `upstream_pipeline_run_id` widget parameter.
When called from `00_run_pipeline.py`, this widget scopes each stage to only
the rows produced by the immediately preceding stage, preventing a full-table
re-scan on incremental runs.

When running notebooks individually, leave the widget blank to process all
eligible rows (PENDING status for notebook 03, all Silver rows for notebook 04).

---

## Permissions

The `dev` catalog must exist and the running principal must have:

```sql
GRANT CREATE SCHEMA ON CATALOG dev TO `your-user-or-group`;
GRANT USE CATALOG ON CATALOG dev TO `your-user-or-group`;
```

Each notebook creates its schema and tables idempotently (`CREATE IF NOT EXISTS`),
so no manual DDL is required before the first run.
