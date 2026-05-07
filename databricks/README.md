# Databricks Notebooks

This directory contains Databricks notebooks that execute the full Bronze → Silver → Gold
pipeline against a Unity Catalog environment. The notebooks are a thin orchestration layer
over the Python modules in `ingestion/` and `transforms/` — all business logic lives there.

---

## Two-Job Architecture

The pipeline is split into two Databricks jobs that share Silver and Gold processing
but have distinct Bronze ingestion paths.

### Job 1 — Real-Time Pipeline

**Trigger:** Streaming or scheduled hourly (interface engine drops HL7/FHIR)

| Task | Notebook | Stage | Description |
|---|---|---|---|
| 1 | `01_ingest_hl7.py` | Bronze | HL7 v2 ADT/ORU → `ingest_hl7_messages` |
| 2 | `02_ingest_fhir.py` | Bronze | FHIR R4 Bundles → `ingest_fhir_bundles` |
| 3 | `03_bronze_to_silver.py` | Silver | Identity resolution + terminology normalization |
| 4 | `04_silver_to_gold.py` | Gold | Risk scoring, quality measures, ADT feed |

### Job 2 — CSV Batch Pipeline

**Trigger:** Scheduled daily or on SFTP file arrival (eClinicalWorks flat-file drop)

| Task | Notebook | Stage | Description |
|---|---|---|---|
| 1 | `05_ingest_csv.py` | Bronze | eClinicalWorks CSV → `ingest_csv_batches` |
| 2 | `03_bronze_to_silver.py` | Silver | Identity resolution + terminology normalization |
| 3 | `04_silver_to_gold.py` | Gold | Risk scoring, quality measures, ADT feed |

**Note:** `03_bronze_to_silver.py` and `04_silver_to_gold.py` are shared between
both jobs. They are **idempotent** — safe to run from either job without producing
duplicate Silver or Gold records.

---

## Notebook Reference

| Notebook | Job(s) | Reads from | Writes to |
|---|---|---|---|
| `01_ingest_hl7.py` | Job 1, Task 1 | `data/synthetic/hl7_*.txt` | `ingest_hl7_messages`, `audit_*` |
| `02_ingest_fhir.py` | Job 1, Task 2 | `data/synthetic/fhir_bundle_*.json` | `ingest_fhir_bundles`, `audit_*` |
| `03_bronze_to_silver.py` | Job 1, Task 3 / Job 2, Task 2 | `ingest_fhir_bundles` | `mpi_*`, `clinical_*`, `terminology_*` |
| `04_silver_to_gold.py` | Job 1, Task 4 / Job 2, Task 3 | `fhir_silver.*` | `analytics_*`, `export_*` |
| `05_ingest_csv.py` | Job 2, Task 1 | `data/synthetic/ecw_*.csv` | `ingest_csv_batches`, `audit_*` |

---

## Catalog and Schema Structure

```
dev (catalog)
├── fhir_bronze (schema)
│   ├── ingest_hl7_messages       — Raw HL7 v2 messages, one row per message
│   ├── ingest_fhir_bundles       — Raw FHIR R4 Bundles, one row per Bundle
│   ├── ingest_csv_batches        — Raw CSV flat files, one row per file
│   ├── audit_ingest_log          — Run-level audit trail (all three paths)
│   └── audit_validation_errors   — DQ failures requiring human review
├── fhir_silver (schema)
│   ├── mpi_patient_index         — UMPI registry (4-pass deterministic MPI)
│   ├── mpi_identity_crosswalk    — Source MRN → UMPI mapping per facility
│   ├── clinical_patients         — Canonical demographics (UMPI-keyed)
│   ├── clinical_encounters       — Normalized encounters
│   ├── clinical_observations     — LOINC-normalized observations
│   ├── clinical_conditions       — ICD-10 normalized diagnoses
│   ├── clinical_medications      — RxNorm-normalized medications
│   ├── terminology_unmapped_codes — Codes with no mapping (action queue)
│   └── dq_tenant_scorecard       — Per-tenant DQ scorecard per run
└── fhir_gold (schema)
    ├── analytics_patient_summary  — Charlson risk scoring, chronic condition flags
    ├── analytics_quality_measures — HEDIS CDC HbA1c Control results
    ├── analytics_adt_events       — ADT event feed with 30-day readmission flag
    ├── export_uscdi_v3_patient    — TEFCA/QHIN-ready USCDI v3 export records
    ├── patient_summary_v          — Tenant-scoped RLS view
    ├── quality_measures_v         — Tenant-scoped RLS view
    └── adt_events_v               — Tenant-scoped RLS view
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
When called from a Databricks job, this widget scopes each stage to only
the rows produced by the immediately preceding stage, preventing a full-table
re-scan on incremental runs.

When running notebooks individually, leave the widget blank to process all
eligible rows (all Bronze bundles for notebook 03; all Silver rows for notebook 04).

---

## Permissions

The `dev` catalog must exist and the running principal must have:

```sql
GRANT CREATE SCHEMA ON CATALOG dev TO `your-user-or-group`;
GRANT USE CATALOG ON CATALOG dev TO `your-user-or-group`;
```

Each notebook creates its schema and tables idempotently (`CREATE IF NOT EXISTS`),
so no manual DDL is required before the first run.
