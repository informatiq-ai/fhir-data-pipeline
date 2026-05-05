-- =============================================================================
-- BRONZE LAYER: Raw Landing Zone
-- =============================================================================
-- Purpose: Immutable storage of source data exactly as received.
--          No transformations. No business logic. No rejections.
--          Every message that enters the pipeline lands here first.
--
-- Design principle: Bronze is a legal audit trail. If a source system
-- sent a malformed message, that malformed message lives here forever.
-- The Silver layer handles the correction, not this one.
--
-- Platform notes:
--   Snowflake: Use VARIANT instead of JSONB. Table clustering on received_ts.
--   Databricks: Use Delta Lake with LIQUID CLUSTERING on received_ts, tenant_id.
--   Postgres (local dev): JSONB works as written.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- bronze.hl7_messages
-- Raw HL7 v2 messages, preserved as received.
-- One row per message envelope (MSH segment defines the envelope).
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.hl7_messages (
    -- Surrogate key
    message_id          UUID            DEFAULT gen_random_uuid() PRIMARY KEY,

    -- Tenant identification (set by ingestion layer from ZTN segment or config)
    tenant_id           VARCHAR(100)    NOT NULL,

    -- Message envelope metadata (parsed from MSH segment only)
    sending_application VARCHAR(255),
    sending_facility    VARCHAR(255),
    message_type        VARCHAR(20),        -- e.g., ADT^A01, ORU^R01
    message_control_id  VARCHAR(255),
    message_datetime    TIMESTAMP,

    -- Feed metadata
    source_system       VARCHAR(255),
    feed_type           VARCHAR(50),        -- ADT, ORU, SIU, VXU
    batch_id            VARCHAR(255),

    -- Raw payload — the full HL7 message, pipe-delimited, untouched
    raw_payload         TEXT            NOT NULL,

    -- Ingestion audit
    received_ts         TIMESTAMP       NOT NULL DEFAULT NOW(),
    ingestion_version   VARCHAR(20),        -- pipeline version that wrote this row
    file_source         VARCHAR(500),       -- SFTP path or MQ topic, if applicable

    -- Processing status (updated by Silver job, never modifies raw_payload)
    processing_status   VARCHAR(50)     DEFAULT 'PENDING',
                                            -- PENDING, PROCESSED, ERROR, SKIPPED
    processing_error    TEXT,
    processed_ts        TIMESTAMP
);

-- Indexes for Silver processing queries
CREATE INDEX IF NOT EXISTS idx_hl7_messages_tenant_status
    ON bronze.hl7_messages (tenant_id, processing_status, received_ts);

CREATE INDEX IF NOT EXISTS idx_hl7_messages_message_type
    ON bronze.hl7_messages (message_type, received_ts);

-- Snowflake equivalent (comment out above, uncomment below):
-- ALTER TABLE bronze.hl7_messages CLUSTER BY (received_ts, tenant_id);

-- Databricks/Delta equivalent:
-- OPTIMIZE bronze.hl7_messages ZORDER BY (tenant_id, received_ts);


-- -----------------------------------------------------------------------------
-- bronze.fhir_resources
-- Raw FHIR R4 resources, stored as JSONB.
-- One row per resource entry within a Bundle (not per Bundle).
-- The full Bundle is preserved in bundle_payload for auditability.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.fhir_resources (
    resource_id         UUID            DEFAULT gen_random_uuid() PRIMARY KEY,

    -- Tenant identification
    tenant_id           VARCHAR(100)    NOT NULL,

    -- Bundle-level metadata
    bundle_id           VARCHAR(255),
    bundle_timestamp    TIMESTAMP,
    bundle_type         VARCHAR(50),        -- transaction, message, collection

    -- Resource-level metadata
    fhir_resource_type  VARCHAR(100)    NOT NULL,   -- Patient, Encounter, Observation...
    fhir_resource_id    VARCHAR(255),               -- resource.id from source
    fhir_version_id     VARCHAR(100),

    -- Raw resource payload — the JSON object for this resource, untouched
    raw_payload         JSONB           NOT NULL,

    -- Full bundle payload (for audit; nullable to avoid duplication on bulk loads)
    bundle_payload      JSONB,

    -- Feed metadata
    source_system       VARCHAR(255),
    feed_type           VARCHAR(50)     DEFAULT 'FHIR_R4',
    batch_id            VARCHAR(255),

    -- Ingestion audit
    received_ts         TIMESTAMP       NOT NULL DEFAULT NOW(),
    ingestion_version   VARCHAR(20),

    -- Processing status
    processing_status   VARCHAR(50)     DEFAULT 'PENDING',
    processing_error    TEXT,
    processed_ts        TIMESTAMP
);

-- Snowflake note: replace JSONB with VARIANT

CREATE INDEX IF NOT EXISTS idx_fhir_resources_tenant_type_status
    ON bronze.fhir_resources (tenant_id, fhir_resource_type, processing_status);

CREATE INDEX IF NOT EXISTS idx_fhir_resources_received_ts
    ON bronze.fhir_resources (received_ts);

-- GIN index for JSONB querying during development/debugging
CREATE INDEX IF NOT EXISTS idx_fhir_resources_payload_gin
    ON bronze.fhir_resources USING GIN (raw_payload);


-- -----------------------------------------------------------------------------
-- bronze.flat_file_batches
-- Metadata table for batch CSV/flat-file ingestion jobs.
-- One row per file received. Row-level data lands in bronze.flat_file_records.
-- Supports the eClinicalWorks / midnight SFTP pattern.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.flat_file_batches (
    batch_id            UUID            DEFAULT gen_random_uuid() PRIMARY KEY,

    tenant_id           VARCHAR(100)    NOT NULL,
    source_system       VARCHAR(255)    NOT NULL,   -- e.g., ECLINICALWORKS, ATHENA
    file_name           VARCHAR(500)    NOT NULL,
    file_path           VARCHAR(1000),              -- SFTP path
    file_size_bytes     BIGINT,
    file_hash           VARCHAR(128),               -- SHA-256 for dedup detection
    record_count        INTEGER,

    received_ts         TIMESTAMP       NOT NULL DEFAULT NOW(),
    processing_status   VARCHAR(50)     DEFAULT 'PENDING',
    processing_error    TEXT,
    processed_ts        TIMESTAMP,
    records_processed   INTEGER,
    records_errored     INTEGER
);


-- -----------------------------------------------------------------------------
-- bronze.flat_file_records
-- Raw rows from CSV/flat-file ingestion, stored as JSONB key-value map.
-- Column names from the CSV header become JSON keys.
-- No data typing, no validation — exactly what the file contained.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.flat_file_records (
    record_id           UUID            DEFAULT gen_random_uuid() PRIMARY KEY,
    batch_id            UUID            NOT NULL REFERENCES bronze.flat_file_batches(batch_id),
    tenant_id           VARCHAR(100)    NOT NULL,
    source_system       VARCHAR(255)    NOT NULL,

    -- Row number in source file (1-indexed, excluding header)
    source_row_number   INTEGER,

    -- Raw row as JSON map of {column_name: raw_string_value}
    raw_payload         JSONB           NOT NULL,

    -- Ingestion audit
    received_ts         TIMESTAMP       NOT NULL DEFAULT NOW(),

    -- Processing status
    processing_status   VARCHAR(50)     DEFAULT 'PENDING',
    processing_error    TEXT,
    processed_ts        TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_flat_file_records_batch_status
    ON bronze.flat_file_records (batch_id, processing_status);

CREATE INDEX IF NOT EXISTS idx_flat_file_records_tenant
    ON bronze.flat_file_records (tenant_id, received_ts);
