# Tenant Isolation Patterns

## The Multi-Tenancy Problem in Healthcare Data

A state HIE serves multiple participating organizations — hospitals, health systems, physician groups, FQHCs — that have competing interests in data privacy and data access. Hospital A's patient records are confidential to Hospital A. A patient's cardiologist at Hospital B should not see their psychiatry records from Clinic C unless a data sharing agreement explicitly permits it. The state HDU operator needs population-level visibility to fulfill public health and quality reporting obligations.

These requirements have to be enforced by the data platform, not by application-layer access controls. Application-layer controls can be bypassed, misconfigured, or forgotten. Platform-level enforcement cannot.

This architecture uses a layered tenant isolation strategy: logical tagging throughout the stack, with row-level security enforcement at the Gold layer where data is consumed.

---

## Tenant Identification

Every record in the pipeline carries a tenant_id — a stable string identifier for the participating organization. This identifier is assigned at ingestion and propagated through every downstream layer without modification.

**HL7 v2 feeds:** Tenant ID is extracted from the custom ZTN segment appended to each message by the interface engine. The ZTN segment carries TENANT_ID, SOURCE_SYSTEM, FEED_TYPE, and BATCH_ID as key-value pairs. If ZTN is absent, tenant_id falls back to the sending facility identifier in MSH.4, then to the default_tenant_id configured for that connection.

**FHIR R4 feeds:** Tenant ID is extracted from Bundle.meta.tag using the HDU tenant tag system URI (`https://oklahoma-hdu.example.org/tags/tenant`). The tag code is the tenant_id value.

**Batch CSV feeds:** Tenant ID is set in the batch job configuration for that feed — there is no in-file mechanism for a CSV to self-identify its tenant. The batch_id in bronze.flat_file_batches carries the tenant association.

The tenant_id assignment at Bronze is authoritative. It cannot be overridden by data content at any downstream layer.

---

## Isolation Strategy by Layer

### Bronze

Bronze isolation is logical only. All tenant data lands in shared tables (bronze.hl7_messages, bronze.fhir_resources, bronze.flat_file_records). The tenant_id column tags every row.

Access to Bronze is restricted to the pipeline service account only. No tenant user, analyst, or BI tool has direct Bronze access. Bronze is for pipeline processing and audit — not consumption.

### Silver

Silver isolation follows the same logical tagging pattern. All tenant data shares the Silver CDM tables. tenant_id is present on every entity.

Access to Silver is restricted to pipeline service accounts and HDU operator roles with explicit grants. Tenant-specific service accounts (used by tenant-facing APIs) do not have direct Silver access.

### Gold

Gold is the consumption layer and where row-level security is enforced. Every Gold table has an RLS policy that evaluates the session's tenant context against the row's tenant_id. A Hospital A session returns only Hospital A rows. An HDU operator session returns all rows.

---

## Row-Level Security Implementation

### Postgres (local development)

```sql
-- Session variable set at connection time by the application layer
SET app.current_tenant_id = 'INTEGRIS_BAPTIST';

-- Helper functions
CREATE OR REPLACE FUNCTION current_tenant_id() RETURNS TEXT AS $$
  SELECT current_setting('app.current_tenant_id', TRUE);
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION is_hdu_operator() RETURNS BOOLEAN AS $$
  SELECT pg_has_role(current_user, 'hdu_operator', 'MEMBER');
$$ LANGUAGE SQL STABLE;

-- RLS policy on a Gold table
ALTER TABLE gold.patient_summary ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON gold.patient_summary
  USING (
    is_hdu_operator()
    OR tenant_id = current_tenant_id()
  );
```

### Snowflake

```sql
-- Row access policy
CREATE OR REPLACE ROW ACCESS POLICY rls_tenant_policy
  AS (tenant_id VARCHAR) RETURNS BOOLEAN ->
    IS_ROLE_IN_SESSION('HDU_OPERATOR')
    OR CURRENT_ROLE() = tenant_id;

-- Apply to each Gold table
ALTER TABLE gold.patient_summary
  ADD ROW ACCESS POLICY rls_tenant_policy ON (tenant_id);

ALTER TABLE gold.quality_measures
  ADD ROW ACCESS POLICY rls_tenant_policy ON (tenant_id);
```

### Databricks Unity Catalog

```sql
-- Row filter function in Unity Catalog
CREATE FUNCTION hdu_catalog.rls.tenant_filter(tenant_id STRING)
  RETURN IS_ACCOUNT_GROUP_MEMBER('hdu_operators')
      OR SESSION_USER() = tenant_id;

-- Apply to each Gold table
ALTER TABLE gold.patient_summary
  SET ROW FILTER hdu_catalog.rls.tenant_filter ON (tenant_id);
```

---

## Cross-Tenant Analytics

Cross-tenant analytics are available only to the HDU operator role and only through purpose-built aggregate views that do not expose individual patient records across organizational boundaries.

The `gold.state_population_metrics` view is the reference implementation of this pattern. It exposes count-level aggregates (total patients, ED visit volumes, chronic condition prevalence) broken down by date without exposing any tenant-specific patient data or PHI.

```sql
-- HDU operator only: revoke from tenant_user, grant to hdu_operator
REVOKE ALL ON gold.state_population_metrics FROM tenant_user;
GRANT SELECT ON gold.state_population_metrics TO hdu_operator;
```

Individual patient-level cross-tenant access — for example, a care coordinator at Hospital A viewing a patient's records from their prior admission at Hospital B — requires an explicit data sharing agreement between the two organizations and a row-level grant scoped to that agreement. The schema supports this through the patient_identifiers crosswalk and UMPI linkage, but the access control implementation is governance-dependent and outside the scope of this reference design.

---

## Schema-Level vs. Row-Level Isolation

This architecture uses row-level isolation (shared tables, tenant_id column, RLS policies) rather than schema-level isolation (one schema per tenant) or database-level isolation (one database per tenant).

The tradeoff is deliberate. Row-level isolation scales to hundreds of tenants without operational overhead, enables cross-tenant analytics for the HDU operator without cross-database queries, and simplifies schema evolution — adding a column to silver.lab_observations affects all tenants simultaneously with a single migration.

Schema-level isolation would provide stronger logical separation at the cost of operational complexity that scales linearly with tenant count. For a state HIE that may onboard dozens to hundreds of provider organizations over time, that operational cost is prohibitive.

The RLS enforcement at Gold makes the logical row-level isolation functionally equivalent to schema-level isolation for tenant users — they cannot see rows that don't belong to them, regardless of the underlying table structure.

---

## HIPAA Considerations

Tenant isolation is a technical control that supports HIPAA's minimum necessary standard and the requirement for appropriate safeguards against impermissible disclosure. It is not itself a HIPAA compliance program.

A production HIE deployment requires: Business Associate Agreements with all participating organizations, audit logging of all data access (who queried what, when), breach notification procedures, and a data sharing governance framework that defines the permitted uses and disclosures for cross-tenant analytics.

The audit logging capability is supported by this architecture through the normalization_log in Silver and the data quality scorecard in Gold. Query-level audit logging is a platform capability (Snowflake Access History, Databricks Audit Logs, Postgres pgaudit) that should be enabled and monitored in production.
