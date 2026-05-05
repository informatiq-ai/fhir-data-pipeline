# fhir-data-pipeline

A vendor-agnostic reference architecture for multi-tenant clinical data normalization at scale.

This repo demonstrates a medallion-style (Bronze → Silver → Gold) pipeline for ingesting, normalizing, and exposing clinical data from heterogeneous EHR sources. It is designed for the class of problem faced by Health Information Exchanges (HIEs), Health Data Utilities (HDUs), and large integrated delivery networks onboarding disparate provider organizations.

Synthetic data is included as a proof of concept. No real patient data is used anywhere in this repository.

---

## The Problem This Solves

Healthcare data arrives in two flavors, and neither is clean.

The first is real-time HL7 v2 and FHIR R4 — ADT event streams, lab results, clinical summaries — from modern EHRs like Epic and Cerner. These are structured but inconsistent: local code systems, mismatched terminology, and patient identifiers that mean something inside one organization and nothing outside it.

The second is the batch flat file — a massive CSV dropped on an SFTP server at midnight by a vendor whose API story is "we don't have one." This is not a legacy edge case. It is a present-day reality for a significant portion of the ambulatory market, including platforms like eClinicalWorks and Athena in their older deployment configurations.

A state-scale HIE has to handle both, normalize both into a single coherent clinical record, resolve patient identity across all of them, and serve that data to multiple tenants who cannot see each other's records.

This architecture handles that problem.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                           │
│  Epic · Cerner · Meditech · eClinicalWorks · Athena · Others   │
└──────────────┬──────────────────────────────┬───────────────────┘
               │ HL7 v2 / FHIR R4             │ CSV / Flat File
               │ (real-time)                  │ (batch SFTP)
               ▼                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Raw)                          │
│  Immutable landing zone. JSONB/VARIANT blobs. No transforms.    │
│  Every source message preserved exactly as received.            │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Curated)                       │
│  Identity Resolution → UMPI assignment (Verato-pattern MPI)    │
│  Semantic Normalization → LOINC · SNOMED-CT · RxNorm           │
│  Schema Alignment → Canonical FHIR-based CDM                   │
│  Tenant tagging and PHI segmentation                            │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Analytics)                      │
│  Row-Level Security / Unity Catalog tenant isolation            │
│  Clinical Logic: Attribution · Risk Stratification             │
│  Quality Measures: HEDIS · CMS · USCDI export                  │
│  Semantic layer for BI (Power BI · Tableau)                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Repository Structure

```
fhir-data-pipeline/
├── README.md
├── docs/
│   ├── architecture.md          # Full blueprint and design rationale
│   ├── canonical-data-model.md  # CDM entity definitions
│   ├── tenant-isolation.md      # Multi-tenancy patterns
│   └── uscdi-alignment.md       # USCDI data class mapping
├── data/
│   └── synthetic/
│       ├── hl7_adt_sample.txt   # Synthetic ADT^A01 message
│       ├── hl7_oru_sample.txt   # Synthetic ORU^R01 lab result
│       └── fhir_bundle_sample.json  # Synthetic FHIR R4 Bundle
├── ingestion/
│   ├── hl7_parser.py            # HL7 v2 → raw JSONB
│   └── fhir_ingester.py         # FHIR R4 bundle validation + landing
├── transforms/
│   ├── bronze_to_silver.py      # Normalization pipeline
│   ├── identity_resolution.py   # Deterministic MPI matching
│   └── silver_to_gold.py        # Analytics layer construction
├── schema/
│   ├── bronze.sql               # Raw landing tables
│   ├── silver.sql               # Canonical CDM schema
│   └── gold.sql                 # Analytics + RLS tables
└── tests/
    └── test_transforms.py       # Unit tests for transform logic
```

---

## Design Principles

**1. Bronze is sacred.**
Raw data lands immutably. Nothing is transformed at ingest. This preserves the legal audit trail of exactly what each source system sent and allows Silver reprocessing when standards change — without re-pulling from source.

**2. Identity resolution before clinical normalization.**
You cannot normalize clinical data across tenants until you know you're talking about the same patient. The MPI step runs first, assigns a Universal Master Patient Index (UMPI), and every downstream record is keyed to it.

**3. Terminology is deterministic.**
LOINC, SNOMED-CT, and RxNorm mappings are sourced from authoritative terminology servers — not inferred. LLMs are appropriate for extracting structure from unstructured text (faxed notes, free-text fields). They are not appropriate for mapping "HgbA1c" to a LOINC code. That mapping is a table lookup.

**4. Tenant isolation at the Gold layer.**
Row-level security enforces that Hospital A cannot query Hospital B's records. The HDU operator can query aggregated cross-tenant views where data sharing agreements permit. This is enforced at the platform level, not the application level.

**5. Vendor agnostic by design.**
SQL is written in ANSI standard with callout comments for Snowflake and Databricks/Delta Lake variants. Python dependencies are standard library + `hl7apy` + `fhir.resources`. No proprietary SDK lock-in.

---

## Synthetic Data

All sample data in `data/synthetic/` was generated for demonstration purposes. Names, MRNs, dates, and identifiers are fictional. The messages are structurally valid HL7 v2 and FHIR R4 and will parse correctly with the included ingestion scripts.

---

## Relevance to HIE / HDU Deployments

This architecture directly addresses the canonical problems in state-scale clinical data platforms:

| Problem | This Architecture |
|---|---|
| Duplicate patient records across tenants | Deterministic MPI with referential matching |
| Inconsistent terminology (A1c vs HgbA1c) | Terminology service normalization at Silver |
| Vendor CSV dumps alongside FHIR feeds | Dual-path Bronze ingestion (real-time + batch) |
| Tenant data leakage | RLS + Unity Catalog at Gold |
| Re-processing when standards change | Immutable Bronze, replayable Silver |
| USCDI / TEFCA exchange readiness | Gold layer USCDI-aligned export schema |

---

## Stack

- **Language:** Python 3.11+
- **SQL:** ANSI SQL (Snowflake / Databricks Delta Lake variants noted)
- **HL7 parsing:** `hl7apy`
- **FHIR parsing:** `fhir.resources`
- **Platform:** Lakehouse-agnostic (Databricks, Snowflake, or self-hosted Postgres for local dev)

## Setup

```bash
pip install hl7apy fhir.resources
```

No cloud credentials required to run the ingestion scripts against the synthetic data samples.

---

## Author

Phil Johnson · [informatiq.ai](https://informatiq.ai)

15+ years across payer and provider organizations including the VA, IU Health, Anthem, Intermountain Health, and CVS. This reference architecture reflects real patterns encountered normalizing Epic and legacy EHR data onto Cerner's HealtheIntent platform and integrating flat-file batch feeds from closed vendors into enterprise data platforms.
