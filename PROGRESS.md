# PROGRESS.md — fhir-data-pipeline

Project progress tracker. Updated at the end of every working session.
Read alongside CLAUDE.md and TESTING.md at the start of each new session.

---

## Project Status

**Phase:** Reference architecture complete — pre-publication  
**Next action:** Write blog post for informatiq.ai, then publish repo to GitHub

---

## Milestone Summary

| Milestone | Status | Completed |
|---|---|---|
| M1 — Repository structure and synthetic data | ✅ Complete | Session 1 |
| M2 — Bronze layer schemas and ingestion scripts | ✅ Complete | Session 1 |
| M3 — Silver layer CDM schema and normalization | ✅ Complete | Session 1 |
| M4 — Gold layer analytics and RLS schema | ✅ Complete | Session 1 |
| M5 — Identity resolution module | ✅ Complete | Session 1 |
| M6 — Unit test suite (41 tests) | ✅ Complete | Session 1 |
| M7 — Documentation (architecture, CDM, tenancy, USCDI) | ✅ Complete | Session 1 |
| M8 — Pipeline runner and results automation | ✅ Complete | Session 1 |
| M9 — CI/CD (GitHub Actions) | ✅ Complete | Session 1 |
| M10 — CLAUDE.md / TESTING.md / PROGRESS.md | ✅ Complete | Session 1 |
| M11 — Blog post (informatiq.ai) | 🔲 Not started | — |
| M12 — GitHub repo publication | 🔲 Not started | — |
| M13 — Integration tests | 🔲 Not started | — |
| M14 — LinkedIn outreach to Katy Decker | 🔲 Not started | — |

---

## Session Log

### Session 1 — Initial build
**Date:** 2026-05-05  
**Duration:** Single extended session

**Completed:**

Repository structure established with full medallion architecture across
Bronze, Silver, and Gold layers. All schema files written in ANSI SQL with
Snowflake and Databricks Delta Lake variant callouts in comments.

Synthetic data generated for three source system scenarios:
- `hl7_adt_sample.txt` — ADT^A01 admission from Epic via HL7 v2
- `hl7_oru_sample.txt` — ORU^R01 lab results from Epic via HL7 v2
- `fhir_bundle_sample.json` — FHIR R4 transaction Bundle (Patient, Encounter, Observation, Condition)

Ingestion layer implemented:
- `hl7_parser.py` — MSH and ZTN segment extraction, batch-safe error handling, Bronze record production
- `fhir_ingester.py` — Bundle splitting to per-resource records, tenant extraction from meta.tag

Transform layer implemented:
- `identity_resolution.py` — Four-pass deterministic MPI with MPIIndex class and FHIR Patient extractor
- `bronze_to_silver.py` — TerminologyService (LOINC, RxNorm, SNOMED), FHIR Observation normalization, full normalization audit log
- `silver_to_gold.py` — Patient summary with Charlson risk scoring, CDC HbA1c Control quality measure, ADT event feed with 30-day readmission flagging

Test suite: 41 unit tests, all passing. Organized into six classes covering
timestamp parsing, HL7 parsing, FHIR ingestion, MPI matching, terminology
service, and Silver normalization.

Code review performed by Gemini Pro. Three bugs identified and fixed:
1. `delta.days` → `delta.total_seconds()` in 30-day readmission logic
2. Removed `source_display` guard in unmapped LOINC path to ensure audit log always written
3. Replaced inline ICD-10 sets with global constants in Silver → Gold layer

Pipeline runner (`run_pipeline.py`) implemented with three modes:
- Default: runs all 5 stages + tests, writes `PIPELINE_RESULTS.md`
- `--no-write`: runs pipeline, prints output only
- `--tests-only`: runs pytest suite only

Makefile added with `install`, `run`, `test`, `results`, `clean` targets.

GitHub Actions implemented:
- `ci.yml`: runs on push/PR, tests Python 3.11 and 3.12
- `regenerate-results.yml`: auto-commits updated `PIPELINE_RESULTS.md` on push to main

Project documentation:
- `CLAUDE.md` — session rules, repo structure, CI/CD architecture, development rules, known gaps
- `TESTING.md` — all 41 test cases documented with rationale, integration test plan
- `PROGRESS.md` — this file

**Files created this session (22 files):**

```
README.md
CLAUDE.md
TESTING.md
PROGRESS.md
PIPELINE_RESULTS.md
Makefile
run_pipeline.py
.gitignore
.github/workflows/ci.yml
.github/workflows/regenerate-results.yml
data/synthetic/hl7_adt_sample.txt
data/synthetic/hl7_oru_sample.txt
data/synthetic/fhir_bundle_sample.json
schema/bronze.sql
schema/silver.sql
schema/gold.sql
ingestion/hl7_parser.py
ingestion/fhir_ingester.py
transforms/identity_resolution.py
transforms/bronze_to_silver.py
transforms/silver_to_gold.py
docs/architecture.md
docs/canonical-data-model.md
docs/tenant-isolation.md
docs/uscdi-alignment.md
tests/test_transforms.py
```

**Bugs fixed this session:**
- Readmission window: `delta.days` → `delta.total_seconds()` (same-day readmission edge case)
- Unmapped norm log: removed `source_display` guard so UNMAPPED entries always written
- ICD-10 constants: inline sets replaced with global `DIABETES_ICD10`, `HYPERTENSION_ICD10`, etc.

**Known gaps carried forward:**
- No integration tests (`tests/test_integration.py` not yet created)
- No deployment pipeline (no target environment defined)
- Elixhauser Index scaffolded but not implemented in `silver_to_gold.py`
- Procedures and Immunizations are schema extension points only — no Silver tables yet
- Terminology service uses static lookup tables; production replacement is NLM VSAC API

---

## Planned Work

### M11 — Blog post (informatiq.ai)

Write a practitioner-voice blog post titled something like
"Designing a Canonical Clinical Data Model for a State HIE."

Audience: Healthcare data and IT professionals.
Tone: First-person, direct, no whitepaper language.

Key narrative beats:
- The two ingestion realities (real-time FHIR vs midnight CSV)
- Why identity resolution must precede clinical normalization
- The A1c / HgbA1c problem as the canonical HIE terminology example
- Why terminology mapping is deterministic lookup, not model inference
- The Bronze immutability guarantee and why it matters for reprocessing
- Walk through the Carlos Ramirez scenario end-to-end

Grounded in real experience from:
- IU Health: Epic → HealtheIntent via HL7/FHIR normalization
- Intermountain: eClinicalWorks → SQL Server via daily CSV (batch flat-file path)

### M12 — GitHub repo publication

Steps:
1. Delete `__pycache__` directories before first commit
   `find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true`
2. Initialize git: `git init && git add . && git commit -m "feat: initial reference architecture"`
3. Create GitHub repo: `fhir-data-pipeline` (public)
4. Push: `git remote add origin <url> && git push -u origin main`
5. Enable Actions write permissions: Settings → Actions → General → "Read and write permissions"
6. Verify both workflows appear in the Actions tab
7. Trigger `regenerate-results.yml` manually to confirm auto-commit works

### M13 — Integration tests

Create `tests/test_integration.py` with the five planned test cases documented
in TESTING.md:
- `test_hl7_adt_to_silver_encounter`
- `test_fhir_bundle_to_silver_full`
- `test_cross_tenant_mpi_linkage`
- `test_full_pipeline_to_gold`
- `test_malformed_batch_does_not_block_valid`

Update `ci.yml` to run integration tests as a separate job after unit tests pass.
Update TESTING.md test count. Update CLAUDE.md testing strategy section.

### M14 — LinkedIn outreach to Katy Decker, Director of Clinical Informatics, MyHealth Access

After blog post is published and repo is live:
- Connect on LinkedIn without a message first
- Wait for connection to be accepted
- Send a short note linking the blog post, no ask beyond peer conversation
- Frame as: practitioner sharing a reference architecture, inviting perspective
  from someone working in the problem space

---

## Architecture Decisions Log

| Decision | Rationale | Alternatives Considered |
|---|---|---|
| Row-level tenant isolation (not schema-level) | Scales to hundreds of tenants; simplifies schema evolution; enables cross-tenant analytics for HDU operator without cross-schema queries | Schema-per-tenant (too much operational overhead at scale) |
| Bronze is immutable, no transforms at ingest | Legal audit trail; enables Silver reprocessing when standards change without re-pulling from source | Transform-on-ingest (loses provenance, can't replay) |
| Identity resolution before clinical normalization | Cross-tenant clinical data is only coherent after patient identity is resolved | Normalize first (produces disconnected records per MRN) |
| Terminology mapping is deterministic lookup | Clinical codes are legal/clinical records; mapping must be auditable and reproducible | LLM inference (non-deterministic, not auditable) |
| LOINC for labs, ICD-10 for diagnoses, RxNorm for meds, SNOMED dual-coded | USCDI v3 alignment; TEFCA exchange readiness; HEDIS measure requirements | Local code systems only (not interoperable) |
| patient_key (hashed UMPI) in Gold, raw UMPI in Silver | Additional de-identification layer for BI tools that may cache query results | Raw UMPI in Gold (unnecessary PHI exposure to BI layer) |
| ANSI SQL with platform variant callouts | Vendor-agnostic reference implementation; Snowflake and Databricks variants documented in comments | Platform-specific DDL (limits portability) |
| Static lookup tables in TerminologyService | Sufficient for reference implementation; production replacement is explicit and documented | Hardcoded conditionals (not maintainable); LLM inference (not deterministic) |
| `total_seconds()` for readmission window | Correctly handles same-day readmissions where `delta.days == 0` | `delta.days` (misses same-day readmissions — identified in code review) |
