-- =============================================================================
-- SILVER LAYER: Canonical Clinical Data Model
-- =============================================================================
-- Purpose: Normalized, tenant-tagged clinical data aligned to a canonical
--          schema. All records have a resolved UMPI. All coded values are
--          mapped to standard terminologies (LOINC, SNOMED-CT, RxNorm, ICD-10).
--
-- Design principle: Silver is the single source of truth for clinical data.
--          It is reproducible from Bronze. If national standards change or
--          a mapping error is discovered, Bronze is replayed and Silver is
--          rebuilt. Gold is always derived from Silver.
--
-- Tenant isolation: tenant_id is present on every table. Row-level security
--          is enforced at the Gold layer. Silver is accessible only to the
--          pipeline service account and HDU operators.
--
-- Platform notes:
--   Snowflake: TIMESTAMP_NTZ for all timestamp columns.
--   Databricks: Delta Lake tables with LIQUID CLUSTERING on (tenant_id, umpi).
-- =============================================================================


CREATE SCHEMA IF NOT EXISTS silver;


-- -----------------------------------------------------------------------------
-- silver.master_patient_index
-- The universal patient identity record.
-- Every clinical entity in Silver references umpi, not a local MRN.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.master_patient_index (
    umpi                UUID            DEFAULT gen_random_uuid() PRIMARY KEY,

    -- Confidence metadata
    match_method        VARCHAR(50)     NOT NULL,
                                        -- DETERMINISTIC, PROBABILISTIC, MANUAL
    match_confidence    DECIMAL(5,4),   -- 0.0000 to 1.0000
    is_golden_record    BOOLEAN         DEFAULT TRUE,

    -- Canonical demographics (best available across all source records)
    family_name         VARCHAR(255),
    given_name          VARCHAR(255),
    middle_name         VARCHAR(255),
    birth_date          DATE,
    gender              VARCHAR(20),    -- aligned to FHIR AdministrativeGender
    ssn_last4           CHAR(4),        -- last 4 only; never store full SSN in Silver

    -- Address (canonical; full address history in silver.patient_address_history)
    address_line1       VARCHAR(500),
    city                VARCHAR(255),
    state               CHAR(2),
    postal_code         VARCHAR(20),
    country             CHAR(3)         DEFAULT 'USA',

    -- Audit
    created_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_tenant_id    VARCHAR(100),   -- tenant that first created this UMPI
    merge_history       JSONB           -- audit trail of prior UMPIs merged into this one
);


-- -----------------------------------------------------------------------------
-- silver.patient_identifiers
-- Cross-walk from source MRNs / identifiers to UMPI.
-- Multiple rows per UMPI (one per source identifier).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.patient_identifiers (
    identifier_id       UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    umpi                UUID            NOT NULL REFERENCES silver.master_patient_index(umpi),
    tenant_id           VARCHAR(100)    NOT NULL,
    identifier_system   VARCHAR(500)    NOT NULL,   -- e.g., https://integris-baptist.example.org/mrn
    identifier_type     VARCHAR(50)     NOT NULL,   -- MR, SS, NPI, etc. (HL7 v2-0203)
    identifier_value    VARCHAR(500)    NOT NULL,
    is_active           BOOLEAN         DEFAULT TRUE,
    created_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),

    -- Source provenance
    source_table        VARCHAR(100),   -- bronze.hl7_messages or bronze.fhir_resources
    source_id           UUID            -- FK to bronze record
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_patient_identifiers_unique
    ON silver.patient_identifiers (identifier_system, identifier_value)
    WHERE is_active = TRUE;

CREATE INDEX IF NOT EXISTS idx_patient_identifiers_umpi
    ON silver.patient_identifiers (umpi, tenant_id);


-- -----------------------------------------------------------------------------
-- silver.encounters
-- Normalized encounter records. One row per encounter per tenant.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.encounters (
    encounter_id        UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id           VARCHAR(100)    NOT NULL,
    umpi                UUID            NOT NULL REFERENCES silver.master_patient_index(umpi),

    -- Source identifiers
    source_encounter_id VARCHAR(500)    NOT NULL,   -- visit number from source system
    source_system       VARCHAR(255),

    -- Encounter classification
    encounter_class     VARCHAR(50),    -- IMP (inpatient), AMB, EMER, etc. (v3-ActCode)
    encounter_type_code VARCHAR(50),
    encounter_type_system VARCHAR(255),
    encounter_type_display VARCHAR(500),

    -- Timing
    period_start        TIMESTAMP,
    period_end          TIMESTAMP,

    -- Facility
    facility_name       VARCHAR(500),
    facility_npi        VARCHAR(20),

    -- Attending provider
    attending_provider_npi  VARCHAR(20),
    attending_provider_name VARCHAR(500),

    -- Status
    encounter_status    VARCHAR(50),    -- planned, arrived, in-progress, finished, etc.

    -- Admit/discharge
    admit_source_code   VARCHAR(50),
    discharge_disposition_code VARCHAR(50),

    -- Audit and lineage
    created_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_table        VARCHAR(100),
    source_id           UUID
);

CREATE INDEX IF NOT EXISTS idx_encounters_umpi_tenant
    ON silver.encounters (umpi, tenant_id, period_start);

CREATE INDEX IF NOT EXISTS idx_encounters_tenant_period
    ON silver.encounters (tenant_id, period_start);


-- -----------------------------------------------------------------------------
-- silver.diagnoses
-- ICD-10 coded diagnoses linked to encounters.
-- Both ICD-10 and SNOMED-CT codes stored where available.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.diagnoses (
    diagnosis_id        UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id           VARCHAR(100)    NOT NULL,
    umpi                UUID            NOT NULL REFERENCES silver.master_patient_index(umpi),
    encounter_id        UUID            REFERENCES silver.encounters(encounter_id),

    -- ICD-10 (primary coding system for diagnoses)
    icd10_code          VARCHAR(20),
    icd10_display       VARCHAR(500),

    -- SNOMED-CT (dual-coded where mappable)
    snomed_code         VARCHAR(50),
    snomed_display      VARCHAR(500),

    -- Source local code (before normalization)
    source_code         VARCHAR(100),
    source_code_system  VARCHAR(255),
    source_display      VARCHAR(500),

    -- Diagnosis metadata
    diagnosis_rank      INTEGER,        -- 1 = primary, 2+ = secondary
    diagnosis_use       VARCHAR(50),    -- encounter-diagnosis, problem-list-item, etc.
    clinical_status     VARCHAR(50),    -- active, resolved, inactive
    verification_status VARCHAR(50),    -- confirmed, provisional, differential
    onset_datetime      TIMESTAMP,
    recorded_datetime   TIMESTAMP,

    -- Audit
    created_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_table        VARCHAR(100),
    source_id           UUID
);

CREATE INDEX IF NOT EXISTS idx_diagnoses_umpi_tenant
    ON silver.diagnoses (umpi, tenant_id);

CREATE INDEX IF NOT EXISTS idx_diagnoses_icd10
    ON silver.diagnoses (icd10_code, tenant_id);


-- -----------------------------------------------------------------------------
-- silver.lab_observations
-- Normalized lab results. LOINC is the canonical code system.
-- All values normalized to standard UCUM units.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.lab_observations (
    observation_id      UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id           VARCHAR(100)    NOT NULL,
    umpi                UUID            NOT NULL REFERENCES silver.master_patient_index(umpi),
    encounter_id        UUID            REFERENCES silver.encounters(encounter_id),

    -- LOINC (canonical; required in Silver)
    loinc_code          VARCHAR(20)     NOT NULL,
    loinc_display       VARCHAR(500),

    -- Source local code (before normalization)
    source_code         VARCHAR(100),
    source_code_system  VARCHAR(255),
    source_display      VARCHAR(500),   -- e.g., "A1c", "HgbA1c", "Hemoglobin A1c"

    -- Result
    observation_status  VARCHAR(50),    -- final, preliminary, corrected, cancelled
    value_quantity      DECIMAL(18,6),
    value_unit          VARCHAR(100),   -- UCUM unit code
    value_string        VARCHAR(500),   -- for non-numeric results
    value_codeable_code VARCHAR(50),    -- for coded results (e.g., POS/NEG)
    value_codeable_system VARCHAR(255),

    -- Reference range
    reference_range_low  DECIMAL(18,6),
    reference_range_high DECIMAL(18,6),
    reference_range_unit VARCHAR(100),

    -- Interpretation
    interpretation_code VARCHAR(20),    -- H, L, N, A, etc.
    interpretation_display VARCHAR(100),

    -- Timing
    effective_datetime  TIMESTAMP,
    issued_datetime     TIMESTAMP,

    -- Ordering and performing
    ordering_provider_npi VARCHAR(20),
    performing_lab_name   VARCHAR(500),

    -- Audit
    created_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_table        VARCHAR(100),
    source_id           UUID
);

CREATE INDEX IF NOT EXISTS idx_lab_obs_umpi_loinc
    ON silver.lab_observations (umpi, loinc_code, effective_datetime);

CREATE INDEX IF NOT EXISTS idx_lab_obs_tenant_loinc
    ON silver.lab_observations (tenant_id, loinc_code, effective_datetime);


-- -----------------------------------------------------------------------------
-- silver.medications
-- Normalized medication records. RxNorm is the canonical code system.
-- Covers MedicationRequest, MedicationAdministration, and HL7 RDE/RDS feeds.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.medications (
    medication_id       UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id           VARCHAR(100)    NOT NULL,
    umpi                UUID            NOT NULL REFERENCES silver.master_patient_index(umpi),
    encounter_id        UUID            REFERENCES silver.encounters(encounter_id),

    -- RxNorm (canonical)
    rxnorm_code         VARCHAR(50),
    rxnorm_display      VARCHAR(500),

    -- NDC (secondary; preserved for claims reconciliation)
    ndc_code            VARCHAR(20),

    -- Source local code
    source_code         VARCHAR(100),
    source_code_system  VARCHAR(255),
    source_display      VARCHAR(500),

    -- Medication details
    medication_status   VARCHAR(50),    -- active, completed, stopped, entered-in-error
    medication_intent   VARCHAR(50),    -- order, plan, proposal, filler-order
    dosage_text         VARCHAR(500),
    dosage_value        DECIMAL(18,4),
    dosage_unit         VARCHAR(100),
    route_code          VARCHAR(50),
    route_display       VARCHAR(255),
    frequency_text      VARCHAR(255),

    -- Timing
    authored_on         TIMESTAMP,
    effective_start     TIMESTAMP,
    effective_end       TIMESTAMP,

    -- Provider
    prescriber_npi      VARCHAR(20),

    -- Audit
    created_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_table        VARCHAR(100),
    source_id           UUID
);

CREATE INDEX IF NOT EXISTS idx_medications_umpi_tenant
    ON silver.medications (umpi, tenant_id, effective_start);


-- -----------------------------------------------------------------------------
-- silver.adt_events
-- ADT event stream normalized from HL7 v2 ADT messages.
-- Preserves the event type sequence for care coordination and readmission logic.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.adt_events (
    adt_event_id        UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id           VARCHAR(100)    NOT NULL,
    umpi                UUID            NOT NULL REFERENCES silver.master_patient_index(umpi),
    encounter_id        UUID            REFERENCES silver.encounters(encounter_id),

    -- Event classification
    event_type          VARCHAR(10)     NOT NULL,   -- A01, A02, A03, A04, A08, A11, etc.
    event_type_display  VARCHAR(100),               -- Admit, Transfer, Discharge, etc.
    event_datetime      TIMESTAMP       NOT NULL,

    -- Location
    prior_location      VARCHAR(500),
    current_location    VARCHAR(500),   -- ward^room^bed format from PV1

    -- Facility
    facility_name       VARCHAR(500),
    facility_npi        VARCHAR(20),

    -- Source message metadata
    source_message_id   VARCHAR(255),

    -- Audit
    created_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    source_table        VARCHAR(100),
    source_id           UUID
);

CREATE INDEX IF NOT EXISTS idx_adt_events_umpi_tenant
    ON silver.adt_events (umpi, tenant_id, event_datetime);

CREATE INDEX IF NOT EXISTS idx_adt_events_tenant_event_type
    ON silver.adt_events (tenant_id, event_type, event_datetime);


-- -----------------------------------------------------------------------------
-- silver.normalization_log
-- Audit trail for every terminology mapping applied in Silver.
-- Enables lineage tracing from Silver coded values back to source codes.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.normalization_log (
    log_id              UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id           VARCHAR(100)    NOT NULL,

    -- Source record
    source_table        VARCHAR(100)    NOT NULL,   -- bronze table name
    source_id           UUID            NOT NULL,   -- bronze record PK

    -- Target record
    target_table        VARCHAR(100)    NOT NULL,   -- silver table name
    target_id           UUID,                       -- silver record PK

    -- Mapping details
    mapping_type        VARCHAR(50)     NOT NULL,
                                        -- LOINC_MAP, SNOMED_MAP, RXNORM_MAP,
                                        -- ICD10_MAP, IDENTITY_RESOLUTION, UNIT_CONVERSION
    source_value        VARCHAR(500),
    source_system       VARCHAR(255),
    mapped_value        VARCHAR(500),
    mapped_system       VARCHAR(255),
    mapping_confidence  DECIMAL(5,4),   -- 1.0000 = deterministic lookup
    mapping_method      VARCHAR(100),   -- TERMINOLOGY_SERVER, MANUAL, FUZZY_MATCH

    -- Pipeline metadata
    pipeline_version    VARCHAR(20),
    processed_ts        TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_norm_log_source
    ON silver.normalization_log (source_table, source_id);

CREATE INDEX IF NOT EXISTS idx_norm_log_tenant_type
    ON silver.normalization_log (tenant_id, mapping_type, processed_ts);
