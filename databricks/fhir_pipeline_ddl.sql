-- =============================================================================
-- fhir-data-pipeline: Unity Catalog DDL
-- Author: Phillip Johnson <phil@informatiq.ai>
-- Repo:   https://github.com/informatiq-ai/fhir-data-pipeline
--
-- Hierarchy: Catalog → Schema → Table (Unity Catalog is 3 levels only)
-- Logical sub-grouping within schemas is expressed via table name prefixes:
--   fhir_bronze: ingest_* and audit_*
--   fhir_silver: mpi_*, clinical_*, terminology_*, dq_*
--   fhir_gold:   analytics_*, export_*, *_v (views)
--
-- Run order: dev → test → prod (identical schema; environment enforced by catalog)
-- =============================================================================


-- =============================================================================
-- CATALOGS
-- =============================================================================

CREATE CATALOG IF NOT EXISTS dev
  COMMENT 'Development environment — synthetic data only, permissive access';

CREATE CATALOG IF NOT EXISTS test
  COMMENT 'Test environment — synthetic data, CI/CD pipeline validation';

CREATE CATALOG IF NOT EXISTS prod
  COMMENT 'Production environment — stricter RLS, restricted write access';


-- =============================================================================
-- SCHEMAS
-- =============================================================================

-- Dev
CREATE SCHEMA IF NOT EXISTS dev.fhir_bronze
  COMMENT 'Immutable landing zone. Raw payloads preserved as-is. No transforms at ingest ever.';
CREATE SCHEMA IF NOT EXISTS dev.fhir_silver
  COMMENT 'Identity resolved, terminology normalized, CDM-aligned clinical data.';
CREATE SCHEMA IF NOT EXISTS dev.fhir_gold
  COMMENT 'Analytically ready outputs. RLS enforced. patient_key = SHA-256(UMPI).';

-- Test (mirrors dev)
CREATE SCHEMA IF NOT EXISTS test.fhir_bronze  COMMENT 'Test: immutable ingest landing zone';
CREATE SCHEMA IF NOT EXISTS test.fhir_silver  COMMENT 'Test: normalized clinical CDM';
CREATE SCHEMA IF NOT EXISTS test.fhir_gold    COMMENT 'Test: analytics-ready gold layer';

-- Prod (mirrors dev)
CREATE SCHEMA IF NOT EXISTS prod.fhir_bronze  COMMENT 'Prod: immutable ingest landing zone';
CREATE SCHEMA IF NOT EXISTS prod.fhir_silver  COMMENT 'Prod: normalized clinical CDM';
CREATE SCHEMA IF NOT EXISTS prod.fhir_gold    COMMENT 'Prod: analytics-ready gold layer';


-- =============================================================================
-- BRONZE TABLES
-- Prefix guide:
--   ingest_*  Raw payloads from each ingestion path
--   audit_*   Validation outcomes and error records requiring human review
-- =============================================================================

-- -----------------------------------------------------------------------------
-- ingest_hl7_messages
-- Real-time path: ADT, ORU, VXU, SIU from interface engine (e.g. IU Health Epic)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_bronze.ingest_hl7_messages (
  message_id        STRING    NOT NULL  COMMENT 'UUID generated at ingest',
  raw_payload       STRING    NOT NULL  COMMENT 'Full HL7 v2 message, immutable',
  message_type      STRING              COMMENT 'ADT | ORU | VXU | SIU',
  message_event     STRING              COMMENT 'A01 | A08 | R01 | etc.',
  tenant_id         STRING    NOT NULL  COMMENT 'Resolved from ZTN segment',
  source_system     STRING              COMMENT 'Sending application (MSH-3)',
  source_facility   STRING              COMMENT 'Sending facility (MSH-4)',
  received_at       TIMESTAMP NOT NULL  COMMENT 'UTC timestamp at ingest boundary',
  validation_status STRING              COMMENT 'PASS | ERROR',
  pipeline_run_id   STRING              COMMENT 'Links to audit_ingest_log'
)
COMMENT 'Raw HL7 v2 messages. Bronze = immutable. Supports Silver replay when standards change.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS test.fhir_bronze.ingest_hl7_messages LIKE dev.fhir_bronze.ingest_hl7_messages;
CREATE TABLE IF NOT EXISTS prod.fhir_bronze.ingest_hl7_messages LIKE dev.fhir_bronze.ingest_hl7_messages;

-- -----------------------------------------------------------------------------
-- ingest_fhir_bundles
-- Native FHIR R4 API path: transaction Bundles, split per resource post-ingest
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_bronze.ingest_fhir_bundles (
  bundle_id         STRING    NOT NULL  COMMENT 'UUID generated at ingest',
  raw_payload       STRING    NOT NULL  COMMENT 'Full FHIR R4 Bundle JSON, immutable',
  bundle_type       STRING              COMMENT 'transaction | batch | collection',
  resource_types    ARRAY<STRING>       COMMENT 'Resource types present in bundle',
  tenant_id         STRING    NOT NULL  COMMENT 'Resolved from meta.tag',
  source_system     STRING              COMMENT 'Originating FHIR server',
  received_at       TIMESTAMP NOT NULL  COMMENT 'UTC timestamp at ingest boundary',
  validation_status STRING              COMMENT 'PASS | ERROR',
  pipeline_run_id   STRING              COMMENT 'Links to audit_ingest_log'
)
COMMENT 'Raw FHIR R4 Bundles. Bronze = immutable. Supports Silver replay when standards change.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS test.fhir_bronze.ingest_fhir_bundles LIKE dev.fhir_bronze.ingest_fhir_bundles;
CREATE TABLE IF NOT EXISTS prod.fhir_bronze.ingest_fhir_bundles LIKE dev.fhir_bronze.ingest_fhir_bundles;

-- -----------------------------------------------------------------------------
-- ingest_csv_batches
-- Batch flat-file path: eClinicalWorks, Athena (hourly / daily / weekly drops)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_bronze.ingest_csv_batches (
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
COMMENT 'Raw CSV batch files. Bronze = immutable. Supports Silver replay when standards change.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS test.fhir_bronze.ingest_csv_batches LIKE dev.fhir_bronze.ingest_csv_batches;
CREATE TABLE IF NOT EXISTS prod.fhir_bronze.ingest_csv_batches LIKE dev.fhir_bronze.ingest_csv_batches;

-- -----------------------------------------------------------------------------
-- audit_ingest_log
-- Unified run-level log across all three ingestion paths
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_bronze.audit_ingest_log (
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
COMMENT 'Unified ingest audit trail across HL7, FHIR, and CSV paths.';

CREATE TABLE IF NOT EXISTS test.fhir_bronze.audit_ingest_log LIKE dev.fhir_bronze.audit_ingest_log;
CREATE TABLE IF NOT EXISTS prod.fhir_bronze.audit_ingest_log LIKE dev.fhir_bronze.audit_ingest_log;

-- -----------------------------------------------------------------------------
-- audit_validation_errors
-- Failed records requiring human review before Silver promotion
-- Human-in-the-loop gate: these records do NOT flow to Silver until reviewed
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_bronze.audit_validation_errors (
  error_id          STRING    NOT NULL  COMMENT 'UUID',
  pipeline_run_id   STRING              COMMENT 'Links to audit_ingest_log',
  ingestion_path    STRING              COMMENT 'hl7 | fhir | csv',
  source_record_id  STRING              COMMENT 'FK to the originating ingest_* table',
  error_code        STRING              COMMENT 'Structured error code (e.g. HL7_MISSING_PID)',
  error_message     STRING              COMMENT 'Human-readable description',
  raw_payload       STRING              COMMENT 'Copied from source for review without joins',
  tenant_id         STRING,
  requires_review   BOOLEAN,
  reviewed_at       TIMESTAMP,
  reviewed_by       STRING,
  review_outcome    STRING              COMMENT 'APPROVED | REJECTED | ESCALATED',
  created_at        TIMESTAMP NOT NULL
)
COMMENT 'Validation failures requiring human review. Records here are blocked from Silver.';

CREATE TABLE IF NOT EXISTS test.fhir_bronze.audit_validation_errors LIKE dev.fhir_bronze.audit_validation_errors;
CREATE TABLE IF NOT EXISTS prod.fhir_bronze.audit_validation_errors LIKE dev.fhir_bronze.audit_validation_errors;


-- =============================================================================
-- SILVER TABLES
-- Prefix guide:
--   mpi_*           Master Patient Index — identity resolution output
--   clinical_*      Canonical CDM tables (LOINC/SNOMED/RxNorm/ICD-10 normalized)
--   terminology_*   Unmapped code audit log (no silent drops)
--   dq_*            Per-tenant data quality scorecards
-- =============================================================================

-- -----------------------------------------------------------------------------
-- mpi_patient_index
-- UMPI assigned here via 4-pass deterministic MPI
-- Identity resolution runs BEFORE terminology normalization
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.mpi_patient_index (
  umpi                  STRING    NOT NULL  COMMENT 'Universal Master Patient Index — surrogate key assigned by MPI',
  resolution_method     STRING              COMMENT 'pass1_exact | pass2_ssn4_dob | pass3_name_dob | pass4_manual',
  first_resolved_at     TIMESTAMP           COMMENT 'When this UMPI was first created',
  last_updated_at       TIMESTAMP           COMMENT 'When linkage last changed',
  linked_record_count   BIGINT              COMMENT 'Number of source records linked to this UMPI',
  tenant_ids            ARRAY<STRING>       COMMENT 'All tenants contributing records to this UMPI',
  is_merged             BOOLEAN             COMMENT 'True if this UMPI was merged from a prior duplicate',
  merged_into_umpi      STRING              COMMENT 'If merged, the surviving UMPI'
)
COMMENT 'Master patient index. UMPI is the Silver surrogate key for all clinical tables.'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS test.fhir_silver.mpi_patient_index LIKE dev.fhir_silver.mpi_patient_index;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.mpi_patient_index LIKE dev.fhir_silver.mpi_patient_index;

-- -----------------------------------------------------------------------------
-- mpi_identity_crosswalk
-- Source MRN → UMPI mapping per tenant/facility
-- Never deleted — supports audit trail for identity lineage
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.mpi_identity_crosswalk (
  crosswalk_id      STRING    NOT NULL  COMMENT 'UUID',
  umpi              STRING    NOT NULL  COMMENT 'FK to mpi_patient_index',
  source_mrn        STRING    NOT NULL  COMMENT 'Medical record number at source facility',
  tenant_id         STRING    NOT NULL,
  source_system     STRING              COMMENT 'Epic | eClinicalWorks | Athena | etc.',
  facility_id       STRING              COMMENT 'NPI or internal facility code',
  match_confidence  DOUBLE              COMMENT '0.0–1.0 deterministic confidence score',
  created_at        TIMESTAMP NOT NULL,
  updated_at        TIMESTAMP
)
COMMENT 'Source MRN to UMPI crosswalk. Append-only — identity lineage must never be deleted.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.mpi_identity_crosswalk LIKE dev.fhir_silver.mpi_identity_crosswalk;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.mpi_identity_crosswalk LIKE dev.fhir_silver.mpi_identity_crosswalk;

-- -----------------------------------------------------------------------------
-- clinical_patients
-- Canonical patient demographics — UMPI-keyed, tenant-aware
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.clinical_patients (
  patient_id        STRING    NOT NULL  COMMENT 'UUID (Silver internal)',
  umpi              STRING    NOT NULL  COMMENT 'FK to mpi_patient_index',
  first_name        STRING,
  last_name         STRING,
  date_of_birth     DATE,
  gender            STRING              COMMENT 'SNOMED-CT coded preferred',
  race              STRING              COMMENT 'OMB category',
  ethnicity         STRING              COMMENT 'OMB category',
  preferred_language STRING             COMMENT 'ISO 639-1 code',
  address_line1     STRING,
  address_line2     STRING,
  city              STRING,
  state             STRING              COMMENT '2-letter USPS abbreviation',
  zip               STRING,
  phone             STRING,
  email             STRING,
  tenant_id         STRING    NOT NULL,
  source_system     STRING,
  source_record_id  STRING              COMMENT 'FK back to Bronze source',
  created_at        TIMESTAMP NOT NULL,
  updated_at        TIMESTAMP
)
COMMENT 'Canonical patient demographics. One row per tenant_id + umpi combination.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.clinical_patients LIKE dev.fhir_silver.clinical_patients;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.clinical_patients LIKE dev.fhir_silver.clinical_patients;

-- -----------------------------------------------------------------------------
-- clinical_encounters
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.clinical_encounters (
  encounter_id          STRING    NOT NULL  COMMENT 'UUID (Silver internal)',
  umpi                  STRING    NOT NULL  COMMENT 'FK to mpi_patient_index',
  encounter_class       STRING              COMMENT 'IMP | AMB | EMER | VR (HL7 ActEncounterCode)',
  encounter_type        STRING              COMMENT 'Snomed/local type code',
  status                STRING              COMMENT 'planned | in-progress | finished | cancelled',
  admit_datetime        TIMESTAMP,
  discharge_datetime    TIMESTAMP,
  length_of_stay_hours  DOUBLE              COMMENT 'Derived: discharge - admit in hours',
  facility_id           STRING              COMMENT 'NPI or internal facility code',
  attending_provider_npi STRING,
  principal_icd10       STRING              COMMENT 'Principal diagnosis ICD-10 code (denormalized)',
  tenant_id             STRING    NOT NULL,
  source_system         STRING,
  source_record_id      STRING,
  created_at            TIMESTAMP NOT NULL,
  updated_at            TIMESTAMP
)
COMMENT 'Normalized encounters across all ingestion paths.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.clinical_encounters LIKE dev.fhir_silver.clinical_encounters;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.clinical_encounters LIKE dev.fhir_silver.clinical_encounters;

-- -----------------------------------------------------------------------------
-- clinical_observations
-- LOINC normalized. Source: ORU HL7, FHIR Observation, CSV lab exports
-- Reference test: test_terminology_service_fallback_for_local_display
--   eClinicalWorks CSV A1c → LOINC 4548-4
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.clinical_observations (
  observation_id        STRING    NOT NULL  COMMENT 'UUID (Silver internal)',
  umpi                  STRING    NOT NULL,
  encounter_id          STRING              COMMENT 'FK to clinical_encounters (nullable for ambulatory)',
  loinc_code            STRING    NOT NULL  COMMENT 'Normalized LOINC code — never inferred',
  loinc_display         STRING              COMMENT 'LOINC long common name',
  value_quantity        DOUBLE              COMMENT 'Numeric result value',
  value_unit            STRING              COMMENT 'UCUM unit (e.g. %)',
  value_string          STRING              COMMENT 'Text result (when non-numeric)',
  value_codeable_code   STRING              COMMENT 'Coded result code',
  value_codeable_system STRING              COMMENT 'Coded result code system',
  reference_range_low   DOUBLE,
  reference_range_high  DOUBLE,
  interpretation        STRING              COMMENT 'H | L | N | A (HL7 ObsInterpretation)',
  observation_datetime  TIMESTAMP,
  status                STRING              COMMENT 'registered | preliminary | final | amended',
  tenant_id             STRING    NOT NULL,
  source_system         STRING,
  source_code           STRING              COMMENT 'Original source code before normalization',
  source_record_id      STRING,
  created_at            TIMESTAMP NOT NULL,
  updated_at            TIMESTAMP
)
COMMENT 'LOINC-normalized observations. Unmapped codes written to terminology_unmapped_codes — no silent drops.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.clinical_observations LIKE dev.fhir_silver.clinical_observations;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.clinical_observations LIKE dev.fhir_silver.clinical_observations;

-- -----------------------------------------------------------------------------
-- clinical_conditions
-- ICD-10 normalized
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.clinical_conditions (
  condition_id          STRING    NOT NULL,
  umpi                  STRING    NOT NULL,
  encounter_id          STRING,
  icd10_code            STRING    NOT NULL  COMMENT 'Normalized ICD-10 code',
  icd10_display         STRING,
  condition_category    STRING              COMMENT 'primary | secondary | comorbidity',
  onset_datetime        TIMESTAMP,
  abatement_datetime    TIMESTAMP,
  clinical_status       STRING              COMMENT 'active | recurrence | relapse | inactive | remission | resolved',
  verification_status   STRING              COMMENT 'confirmed | provisional | differential | refuted',
  tenant_id             STRING    NOT NULL,
  source_system         STRING,
  source_code           STRING,
  source_record_id      STRING,
  created_at            TIMESTAMP NOT NULL,
  updated_at            TIMESTAMP
)
COMMENT 'ICD-10 normalized conditions and diagnoses.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.clinical_conditions LIKE dev.fhir_silver.clinical_conditions;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.clinical_conditions LIKE dev.fhir_silver.clinical_conditions;

-- -----------------------------------------------------------------------------
-- clinical_medications
-- RxNorm normalized
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.clinical_medications (
  medication_id         STRING    NOT NULL,
  umpi                  STRING    NOT NULL,
  encounter_id          STRING,
  rxnorm_code           STRING    NOT NULL  COMMENT 'Normalized RxNorm code',
  rxnorm_display        STRING,
  dose_quantity         DOUBLE,
  dose_unit             STRING              COMMENT 'UCUM unit',
  route                 STRING              COMMENT 'SNOMED-CT route code',
  frequency             STRING              COMMENT 'Sig text or FHIR Timing code',
  status                STRING              COMMENT 'active | on-hold | cancelled | completed | stopped',
  prescribed_datetime   TIMESTAMP,
  start_date            DATE,
  end_date              DATE,
  prescriber_npi        STRING,
  tenant_id             STRING    NOT NULL,
  source_system         STRING,
  source_code           STRING,
  source_record_id      STRING,
  created_at            TIMESTAMP NOT NULL,
  updated_at            TIMESTAMP
)
COMMENT 'RxNorm-normalized medications.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.clinical_medications LIKE dev.fhir_silver.clinical_medications;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.clinical_medications LIKE dev.fhir_silver.clinical_medications;

-- -----------------------------------------------------------------------------
-- clinical_procedures
-- Schema extension point — code scaffolded, not yet implemented
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.clinical_procedures (
  procedure_id          STRING    NOT NULL,
  umpi                  STRING    NOT NULL,
  encounter_id          STRING,
  procedure_code        STRING              COMMENT 'CPT | SNOMED-CT | ICD-10-PCS',
  code_system           STRING              COMMENT 'CPT | SNOMED | ICD-10-PCS',
  procedure_display     STRING,
  performed_datetime    TIMESTAMP,
  status                STRING,
  performing_provider_npi STRING,
  tenant_id             STRING    NOT NULL,
  source_system         STRING,
  source_record_id      STRING,
  created_at            TIMESTAMP NOT NULL,
  updated_at            TIMESTAMP
)
COMMENT 'Procedures — schema extension point. Terminology normalization not yet implemented.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.clinical_procedures LIKE dev.fhir_silver.clinical_procedures;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.clinical_procedures LIKE dev.fhir_silver.clinical_procedures;

-- -----------------------------------------------------------------------------
-- clinical_immunizations
-- Schema extension point — CVX coded, not yet fully implemented
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.clinical_immunizations (
  immunization_id       STRING    NOT NULL,
  umpi                  STRING    NOT NULL,
  cvx_code              STRING              COMMENT 'CDC CVX vaccine code',
  cvx_display           STRING,
  administered_datetime TIMESTAMP,
  lot_number            STRING,
  expiration_date       DATE,
  status                STRING              COMMENT 'completed | entered-in-error | not-done',
  administering_facility STRING,
  tenant_id             STRING    NOT NULL,
  source_system         STRING,
  source_record_id      STRING,
  created_at            TIMESTAMP NOT NULL,
  updated_at            TIMESTAMP
)
COMMENT 'Immunizations — schema extension point. CVX coded; full VXU parsing not yet implemented.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.clinical_immunizations LIKE dev.fhir_silver.clinical_immunizations;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.clinical_immunizations LIKE dev.fhir_silver.clinical_immunizations;

-- -----------------------------------------------------------------------------
-- terminology_unmapped_codes
-- Explicit audit log for every unmapped code across all normalizations
-- No silent drops — this is a portfolio differentiator and production requirement
-- Reference test: test_unmapped_loinc_still_produces_record
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.terminology_unmapped_codes (
  unmapped_id           STRING    NOT NULL  COMMENT 'UUID',
  source_code           STRING    NOT NULL  COMMENT 'Original untranslated code',
  source_display        STRING              COMMENT 'Original display text',
  target_system         STRING    NOT NULL  COMMENT 'LOINC | SNOMED-CT | RxNorm | ICD-10',
  source_system         STRING              COMMENT 'eClinicalWorks | Epic | Athena | etc.',
  record_type           STRING              COMMENT 'observation | condition | medication | procedure | immunization',
  source_record_id      STRING              COMMENT 'FK to clinical_* table record that triggered this',
  tenant_id             STRING    NOT NULL,
  pipeline_run_id       STRING,
  logged_at             TIMESTAMP NOT NULL,
  resolved              BOOLEAN,
  resolved_at           TIMESTAMP,
  resolved_by           STRING,
  resolved_mapping      STRING              COMMENT 'The mapping applied at resolution',
  resolution_notes      STRING
)
COMMENT 'Explicit unmapped terminology audit log. Every UNMAPPED code is written here. No silent drops ever.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.terminology_unmapped_codes LIKE dev.fhir_silver.terminology_unmapped_codes;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.terminology_unmapped_codes LIKE dev.fhir_silver.terminology_unmapped_codes;

-- -----------------------------------------------------------------------------
-- dq_tenant_scorecard
-- Per-tenant data quality scorecard generated post-normalization each pipeline run
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_silver.dq_tenant_scorecard (
  scorecard_id              STRING    NOT NULL,
  tenant_id                 STRING    NOT NULL,
  pipeline_run_id           STRING,
  total_records             BIGINT,
  complete_demographics_pct DOUBLE    COMMENT '% records with name + DOB + gender',
  loinc_coverage_pct        DOUBLE    COMMENT '% observations successfully LOINC mapped',
  icd10_coverage_pct        DOUBLE    COMMENT '% conditions successfully ICD-10 mapped',
  rxnorm_coverage_pct       DOUBLE    COMMENT '% medications successfully RxNorm mapped',
  mpi_match_rate_pct        DOUBLE    COMMENT '% records assigned a UMPI (not new/unlinked)',
  unmapped_code_count       BIGINT    COMMENT 'Total codes written to terminology_unmapped_codes this run',
  scored_at                 TIMESTAMP NOT NULL
)
COMMENT 'Per-tenant data quality scorecard. One row per tenant per pipeline run.';

CREATE TABLE IF NOT EXISTS test.fhir_silver.dq_tenant_scorecard LIKE dev.fhir_silver.dq_tenant_scorecard;
CREATE TABLE IF NOT EXISTS prod.fhir_silver.dq_tenant_scorecard LIKE dev.fhir_silver.dq_tenant_scorecard;


-- =============================================================================
-- GOLD TABLES
-- Prefix guide:
--   analytics_*   Analytically ready outputs consumed by dashboards / APIs
--   export_*      TEFCA / QHIN / USCDI export-formatted records
--   *_v           RLS-enforced views — tenant applications never query base tables
--
-- patient_key = SHA-256(UMPI) — raw UMPI stays in Silver
-- =============================================================================

-- -----------------------------------------------------------------------------
-- analytics_patient_summary
-- Charlson risk scoring, chronic condition flags, PCP attribution
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_gold.analytics_patient_summary (
  patient_key               STRING    NOT NULL  COMMENT 'SHA-256(UMPI) — UMPI never exposed in Gold',
  charlson_index            INT                 COMMENT 'Charlson Comorbidity Index score',
  elixhauser_index          INT                 COMMENT 'Elixhauser index — scaffolded, not yet implemented',
  chronic_condition_flags   MAP<STRING, BOOLEAN> COMMENT 'e.g. {CHF: true, CKD: false, DIABETES: true}',
  pcp_npi                   STRING              COMMENT 'Attributed primary care provider NPI',
  pcp_attribution_method    STRING              COMMENT 'plurality | most_recent | manual',
  last_encounter_date       DATE,
  total_encounter_count     BIGINT,
  tenant_id                 STRING    NOT NULL,
  pipeline_run_id           STRING,
  generated_at              TIMESTAMP NOT NULL
)
COMMENT 'Patient-level risk summary. Charlson scoring. Rebuilt each pipeline run.';

CREATE TABLE IF NOT EXISTS test.fhir_gold.analytics_patient_summary LIKE dev.fhir_gold.analytics_patient_summary;
CREATE TABLE IF NOT EXISTS prod.fhir_gold.analytics_patient_summary LIKE dev.fhir_gold.analytics_patient_summary;

-- -----------------------------------------------------------------------------
-- analytics_quality_measures
-- HEDIS CDC HbA1c Control implemented as reference measure
-- One row per patient per measure per measurement period
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_gold.analytics_quality_measures (
  measure_id                    STRING    NOT NULL,
  patient_key                   STRING    NOT NULL,
  measure_name                  STRING    NOT NULL  COMMENT 'HEDIS_CDC_HBAC1_CONTROL | etc.',
  measure_code                  STRING              COMMENT 'NCQA measure ID',
  in_denominator                BOOLEAN             COMMENT 'Patient meets denominator criteria',
  in_numerator                  BOOLEAN             COMMENT 'Patient meets numerator criteria',
  excluded                      BOOLEAN             COMMENT 'Patient meets an exclusion criterion',
  hba1c_value                   DOUBLE              COMMENT 'Most recent HbA1c result (measure-specific)',
  measurement_period_start      DATE,
  measurement_period_end        DATE,
  tenant_id                     STRING    NOT NULL,
  pipeline_run_id               STRING,
  generated_at                  TIMESTAMP NOT NULL
)
COMMENT 'Quality measure results. HEDIS CDC HbA1c Control is the reference implementation.';

CREATE TABLE IF NOT EXISTS test.fhir_gold.analytics_quality_measures LIKE dev.fhir_gold.analytics_quality_measures;
CREATE TABLE IF NOT EXISTS prod.fhir_gold.analytics_quality_measures LIKE dev.fhir_gold.analytics_quality_measures;

-- -----------------------------------------------------------------------------
-- analytics_adt_events
-- ADT event feed with 30-day readmission flag
-- Readmission window uses total_seconds() — catches same-day readmissions
-- (Bug fixed: delta.days missed same-day readmissions)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_gold.analytics_adt_events (
  event_id                  STRING    NOT NULL,
  patient_key               STRING    NOT NULL,
  event_type                STRING              COMMENT 'admit | discharge | transfer',
  event_subtype             STRING              COMMENT 'A01 | A02 | A03 | A08 | etc.',
  facility_id               STRING,
  event_datetime            TIMESTAMP NOT NULL,
  readmission_30day         BOOLEAN             COMMENT 'True if readmitted within 30 days of prior discharge',
  prior_discharge_datetime  TIMESTAMP           COMMENT 'Reference discharge for readmission calc',
  days_since_prior_discharge DOUBLE             COMMENT 'total_seconds() / 86400 — supports same-day',
  tenant_id                 STRING    NOT NULL,
  pipeline_run_id           STRING,
  generated_at              TIMESTAMP NOT NULL
)
COMMENT 'ADT event feed with 30-day readmission flag. Uses total_seconds() for same-day accuracy.';

CREATE TABLE IF NOT EXISTS test.fhir_gold.analytics_adt_events LIKE dev.fhir_gold.analytics_adt_events;
CREATE TABLE IF NOT EXISTS prod.fhir_gold.analytics_adt_events LIKE dev.fhir_gold.analytics_adt_events;

-- -----------------------------------------------------------------------------
-- export_uscdi_v3_patient
-- TEFCA / QHIN-ready USCDI v3 export records
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dev.fhir_gold.export_uscdi_v3_patient (
  export_id             STRING    NOT NULL,
  patient_key           STRING    NOT NULL,
  uscdi_version         STRING              COMMENT 'v3',
  export_payload        STRING    NOT NULL  COMMENT 'FHIR R4 JSON — USCDI v3 compliant',
  export_datetime       TIMESTAMP NOT NULL,
  qhin_ready            BOOLEAN             COMMENT 'True when all required USCDI elements present',
  missing_elements      ARRAY<STRING>           COMMENT 'USCDI elements absent from this record',
  tenant_id             STRING    NOT NULL,
  pipeline_run_id       STRING
)
COMMENT 'USCDI v3 export records. TEFCA / QHIN ready. Missing elements tracked for gap analysis.';

CREATE TABLE IF NOT EXISTS test.fhir_gold.export_uscdi_v3_patient LIKE dev.fhir_gold.export_uscdi_v3_patient;
CREATE TABLE IF NOT EXISTS prod.fhir_gold.export_uscdi_v3_patient LIKE dev.fhir_gold.export_uscdi_v3_patient;


-- =============================================================================
-- GOLD VIEWS (RLS-enforced)
-- Tenant applications ALWAYS query these views — never base Gold tables directly
-- patient_key = SHA-256(UMPI); raw UMPI never exposed in Gold layer
--
-- NOTE: In production, current_user() or session tags replace the placeholder.
-- Replace 'YOUR_TENANT_ID' with your actual RLS mechanism before deploying.
-- =============================================================================

CREATE OR REPLACE VIEW dev.fhir_gold.patient_summary_v
  COMMENT 'Tenant-scoped view of analytics_patient_summary. RLS enforced via tenant_id.'
AS
SELECT *
FROM dev.fhir_gold.analytics_patient_summary
WHERE tenant_id = current_user(); -- replace with session tag / row filter policy in prod

CREATE OR REPLACE VIEW dev.fhir_gold.quality_measures_v
  COMMENT 'Tenant-scoped view of analytics_quality_measures. RLS enforced via tenant_id.'
AS
SELECT *
FROM dev.fhir_gold.analytics_quality_measures
WHERE tenant_id = current_user();

CREATE OR REPLACE VIEW dev.fhir_gold.adt_events_v
  COMMENT 'Tenant-scoped view of analytics_adt_events. RLS enforced via tenant_id.'
AS
SELECT *
FROM dev.fhir_gold.analytics_adt_events
WHERE tenant_id = current_user();

-- Repeat views for test and prod (prod should use Unity Catalog Row Filters instead)
CREATE OR REPLACE VIEW test.fhir_gold.patient_summary_v AS
  SELECT * FROM test.fhir_gold.analytics_patient_summary WHERE tenant_id = current_user();

CREATE OR REPLACE VIEW test.fhir_gold.quality_measures_v AS
  SELECT * FROM test.fhir_gold.analytics_quality_measures WHERE tenant_id = current_user();

CREATE OR REPLACE VIEW test.fhir_gold.adt_events_v AS
  SELECT * FROM test.fhir_gold.analytics_adt_events WHERE tenant_id = current_user();

CREATE OR REPLACE VIEW prod.fhir_gold.patient_summary_v AS
  SELECT * FROM prod.fhir_gold.analytics_patient_summary WHERE tenant_id = current_user();

CREATE OR REPLACE VIEW prod.fhir_gold.quality_measures_v AS
  SELECT * FROM prod.fhir_gold.analytics_quality_measures WHERE tenant_id = current_user();

CREATE OR REPLACE VIEW prod.fhir_gold.adt_events_v AS
  SELECT * FROM prod.fhir_gold.analytics_adt_events WHERE tenant_id = current_user();


-- =============================================================================
-- END OF DDL
-- Total: 3 catalogs, 9 schemas, 19 tables, 9 views
-- =============================================================================
