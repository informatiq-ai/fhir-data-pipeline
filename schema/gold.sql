-- =============================================================================
-- GOLD LAYER: Analytics and Reporting
-- =============================================================================
-- Purpose: Pre-aggregated, analytics-ready datasets for quality measures,
--          care coordination, population health, and BI consumption.
--          Row-level security enforces tenant isolation.
--
-- Design principle: Gold is derived entirely from Silver. Nothing in Gold
--          reads directly from Bronze. Gold tables are rebuilt or
--          incrementally refreshed on a defined schedule.
--
-- Tenant isolation:
--   Every Gold table carries tenant_id. RLS policies restrict tenant users
--   to rows where tenant_id matches their session context.
--   HDU operators and analytics users with cross-tenant grants see all rows.
--
-- Platform notes:
--   Snowflake: Implement RLS via row access policies on each table.
--   Databricks: Implement RLS via Unity Catalog row filters.
--   Postgres (local dev): Implement RLS via CREATE POLICY + ENABLE ROW LEVEL SECURITY.
--
-- Semantic layer:
--   Gold tables are the connection target for Power BI and Tableau.
--   Column names are business-friendly and do not expose internal PKs.
-- =============================================================================


CREATE SCHEMA IF NOT EXISTS gold;


-- =============================================================================
-- ROW-LEVEL SECURITY (Postgres implementation for local dev)
-- =============================================================================
-- In production (Snowflake / Databricks), replace with platform-native RLS.
--
-- Pattern: session variable app.current_tenant_id is set at connection time
-- by the application/BI connector. The RLS policy filters accordingly.
-- HDU operator role bypasses via is_hdu_operator() function.
-- =============================================================================

-- Postgres local dev RLS setup (commented out for cross-platform portability):
-- CREATE ROLE tenant_user;
-- CREATE ROLE hdu_operator;
-- CREATE OR REPLACE FUNCTION current_tenant_id() RETURNS TEXT AS $$
--   SELECT current_setting('app.current_tenant_id', TRUE);
-- $$ LANGUAGE SQL STABLE;
-- CREATE OR REPLACE FUNCTION is_hdu_operator() RETURNS BOOLEAN AS $$
--   SELECT pg_has_role(current_user, 'hdu_operator', 'MEMBER');
-- $$ LANGUAGE SQL STABLE;

-- Snowflake row access policy pattern:
-- CREATE OR REPLACE ROW ACCESS POLICY rls_tenant_policy AS (tenant_id VARCHAR)
--   RETURNS BOOLEAN ->
--     IS_ROLE_IN_SESSION('HDU_OPERATOR')
--     OR CURRENT_ROLE() = tenant_id;
--
-- ALTER TABLE gold.<table_name> ADD ROW ACCESS POLICY rls_tenant_policy ON (tenant_id);

-- Databricks Unity Catalog row filter pattern:
-- CREATE FUNCTION hdu_catalog.rls.tenant_filter(tenant_id STRING)
--   RETURN IS_ACCOUNT_GROUP_MEMBER('hdu_operators') OR SESSION_USER() = tenant_id;
-- ALTER TABLE gold.<table_name> SET ROW FILTER hdu_catalog.rls.tenant_filter ON (tenant_id);


-- =============================================================================
-- PATIENT SUMMARY (Population Health)
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.patient_summary (
    -- Tenant and patient identity (no UMPI exposed to BI layer)
    tenant_id               VARCHAR(100)    NOT NULL,
    patient_key             UUID            NOT NULL,   -- hashed UMPI; not raw UMPI

    -- Demographics
    full_name               VARCHAR(500),
    birth_date              DATE,
    age_years               INTEGER,
    gender                  VARCHAR(20),
    state                   CHAR(2),
    postal_code             VARCHAR(20),

    -- Care team attribution
    attributed_pcp_npi      VARCHAR(20),
    attributed_pcp_name     VARCHAR(500),
    attribution_method      VARCHAR(50),    -- ENCOUNTER_FREQUENCY, CLAIMS, MANUAL

    -- Risk stratification
    charlson_index          INTEGER,
    elixhauser_index        INTEGER,
    risk_tier               VARCHAR(20),    -- LOW, MODERATE, HIGH, VERY_HIGH
    risk_score_updated_ts   TIMESTAMP,

    -- Engagement metrics
    total_encounters_12m    INTEGER,
    total_ed_visits_12m     INTEGER,
    total_inpatient_days_12m INTEGER,
    last_encounter_date     DATE,

    -- Chronic condition flags (derived from ICD-10 diagnoses)
    flag_diabetes           BOOLEAN DEFAULT FALSE,
    flag_hypertension       BOOLEAN DEFAULT FALSE,
    flag_heart_failure      BOOLEAN DEFAULT FALSE,
    flag_ckd                BOOLEAN DEFAULT FALSE,
    flag_copd               BOOLEAN DEFAULT FALSE,
    flag_depression         BOOLEAN DEFAULT FALSE,

    -- Data freshness
    as_of_date              DATE            NOT NULL,
    refreshed_ts            TIMESTAMP       NOT NULL DEFAULT NOW(),

    PRIMARY KEY (tenant_id, patient_key, as_of_date)
);

-- RLS policy (Postgres local dev):
-- ALTER TABLE gold.patient_summary ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY tenant_isolation ON gold.patient_summary
--   USING (is_hdu_operator() OR tenant_id = current_tenant_id());


-- =============================================================================
-- QUALITY MEASURES (HEDIS / CMS)
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.quality_measures (
    measure_id              UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id               VARCHAR(100)    NOT NULL,
    patient_key             UUID            NOT NULL,

    -- Measure identification
    measure_code            VARCHAR(50)     NOT NULL,
                                            -- e.g., CDC_HBA1C_CONTROL, CBP, AWC
    measure_name            VARCHAR(255),
    measure_steward         VARCHAR(100),   -- NCQA, CMS, Joint Commission
    measurement_year        INTEGER         NOT NULL,

    -- Measure result
    numerator               BOOLEAN,        -- TRUE = met, FALSE = not met, NULL = excluded
    denominator             BOOLEAN,        -- TRUE = eligible for this measure
    exclusion               BOOLEAN DEFAULT FALSE,
    exclusion_reason        VARCHAR(255),

    -- Supporting evidence (the clinical event that satisfied the measure)
    evidence_encounter_id   UUID,
    evidence_observation_id UUID,
    evidence_date           DATE,
    evidence_value          VARCHAR(255),   -- e.g., "7.2%" for HbA1c

    -- Data freshness
    calculated_ts           TIMESTAMP       NOT NULL DEFAULT NOW(),
    as_of_date              DATE            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_quality_measures_tenant_measure
    ON gold.quality_measures (tenant_id, measure_code, measurement_year);

-- RLS policy:
-- ALTER TABLE gold.quality_measures ENABLE ROW LEVEL SECURITY;
-- CREATE POLICY tenant_isolation ON gold.quality_measures
--   USING (is_hdu_operator() OR tenant_id = current_tenant_id());


-- =============================================================================
-- ADT EVENT STREAM (Care Coordination)
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.adt_event_feed (
    event_id                UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id               VARCHAR(100)    NOT NULL,
    patient_key             UUID            NOT NULL,

    -- Event details
    event_type              VARCHAR(10)     NOT NULL,   -- A01, A02, A03, etc.
    event_type_display      VARCHAR(100),
    event_datetime          TIMESTAMP       NOT NULL,

    -- Location
    facility_name           VARCHAR(500),
    current_location        VARCHAR(500),

    -- Clinical context (denormalized for BI query efficiency)
    primary_diagnosis_icd10 VARCHAR(20),
    primary_diagnosis_display VARCHAR(500),

    -- Attribution
    attributed_pcp_npi      VARCHAR(20),
    attributed_pcp_name     VARCHAR(500),

    -- Readmission flag (30-day window from prior discharge)
    is_readmission_30d      BOOLEAN         DEFAULT FALSE,
    prior_discharge_date    DATE,

    -- Data freshness
    loaded_ts               TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_adt_feed_tenant_datetime
    ON gold.adt_event_feed (tenant_id, event_datetime DESC);

CREATE INDEX IF NOT EXISTS idx_adt_feed_patient_key
    ON gold.adt_event_feed (patient_key, event_datetime DESC);


-- =============================================================================
-- USCDI EXPORT VIEW
-- =============================================================================
-- Aligned to USCDI v3 data classes for TEFCA/QHIN exchange readiness.
-- This view flattens the canonical CDM into the USCDI data element structure.
-- Snowflake/Databricks: materialize as a table with incremental refresh.
-- =============================================================================

CREATE OR REPLACE VIEW gold.uscdi_patient_summary AS
SELECT
    ps.tenant_id,
    ps.patient_key,

    -- USCDI: Patient Demographics
    ps.full_name,
    ps.birth_date,
    ps.gender,
    ps.postal_code,
    ps.state,

    -- USCDI: Encounter Information
    ps.last_encounter_date,
    ps.total_encounters_12m,

    -- USCDI: Problems
    ps.flag_diabetes,
    ps.flag_hypertension,
    ps.flag_heart_failure,
    ps.flag_ckd,
    ps.flag_copd,

    -- USCDI: Health Status / Assessments
    ps.charlson_index,
    ps.risk_tier,

    -- USCDI: Care Team
    ps.attributed_pcp_npi,
    ps.attributed_pcp_name,

    -- Metadata
    ps.as_of_date,
    ps.refreshed_ts

FROM gold.patient_summary ps;

-- RLS on the view inherits from the underlying table in Postgres.
-- In Snowflake, apply the row access policy to the materialized table.


-- =============================================================================
-- CROSS-TENANT ANALYTICS (HDU Operator Only)
-- =============================================================================
-- These views are accessible only to the hdu_operator role.
-- They provide state-wide population health aggregates without exposing PHI.
-- =============================================================================

CREATE OR REPLACE VIEW gold.state_population_metrics AS
SELECT
    as_of_date,
    COUNT(DISTINCT patient_key)                         AS total_patients,
    COUNT(DISTINCT tenant_id)                           AS total_tenants,
    AVG(charlson_index)                                 AS avg_charlson_index,
    SUM(total_ed_visits_12m)                            AS total_ed_visits,
    SUM(total_inpatient_days_12m)                       AS total_inpatient_days,
    SUM(CASE WHEN flag_diabetes THEN 1 ELSE 0 END)      AS patients_with_diabetes,
    SUM(CASE WHEN flag_hypertension THEN 1 ELSE 0 END)  AS patients_with_hypertension,
    SUM(CASE WHEN flag_heart_failure THEN 1 ELSE 0 END) AS patients_with_hf,
    SUM(CASE WHEN risk_tier = 'HIGH' OR risk_tier = 'VERY_HIGH' THEN 1 ELSE 0 END)
                                                        AS high_risk_patients
FROM gold.patient_summary
GROUP BY as_of_date;

-- Access control: restrict to hdu_operator role only
-- Postgres: REVOKE ALL ON gold.state_population_metrics FROM tenant_user;
--           GRANT SELECT ON gold.state_population_metrics TO hdu_operator;
-- Snowflake: GRANT SELECT ON gold.state_population_metrics TO ROLE HDU_OPERATOR;
-- Databricks: GRANT SELECT ON gold.state_population_metrics TO `hdu_operators`;


-- =============================================================================
-- TENANT DATA QUALITY SCORECARD
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.data_quality_scorecard (
    scorecard_id            UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id               VARCHAR(100)    NOT NULL,
    as_of_date              DATE            NOT NULL,

    -- Volume metrics
    total_messages_received INTEGER,
    total_messages_processed INTEGER,
    total_messages_errored  INTEGER,
    processing_error_rate   DECIMAL(5,4),

    -- Identity resolution
    total_patients          INTEGER,
    patients_with_umpi      INTEGER,
    umpi_match_rate         DECIMAL(5,4),
    duplicate_records_found INTEGER,

    -- Terminology coverage
    labs_with_loinc         INTEGER,
    labs_total              INTEGER,
    loinc_coverage_rate     DECIMAL(5,4),
    diagnoses_with_icd10    INTEGER,
    diagnoses_total         INTEGER,
    icd10_coverage_rate     DECIMAL(5,4),
    meds_with_rxnorm        INTEGER,
    meds_total              INTEGER,
    rxnorm_coverage_rate    DECIMAL(5,4),

    -- Data completeness (required fields)
    patients_missing_dob    INTEGER,
    patients_missing_gender INTEGER,
    encounters_missing_provider INTEGER,

    -- Calculated at
    calculated_ts           TIMESTAMP       NOT NULL DEFAULT NOW(),

    PRIMARY KEY (tenant_id, as_of_date)
);
