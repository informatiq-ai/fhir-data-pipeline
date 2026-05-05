# Architecture: Oklahoma HDU Reference Design

## Overview

This document describes the full architecture of the `fhir-data-pipeline` reference implementation — a vendor-agnostic, medallion-style clinical data platform designed for state-scale Health Information Exchange (HIE) and Health Data Utility (HDU) deployments.

The architecture solves a specific class of problem: ingesting clinical data from a heterogeneous mix of EHR vendors, normalizing it into a coherent canonical model, resolving patient identity across organizational boundaries, and serving that data to multiple tenants who cannot see each other's records.

This is not a theoretical problem. It is the operational reality of every state HIE today.

---

## The Two Ingestion Realities

Healthcare data arrives in two fundamentally different ways, and an HIE has to handle both.

The first is the modern path: real-time HL7 v2 message streams (ADT, ORU, SIU) and FHIR R4 Bundles from EHR platforms with mature interface engines. These feeds are structured, relatively timely, and well-documented — though they bring their own normalization challenges around local code systems and patient identifier fragmentation.

The second is the batch path: a CSV file dropped on an SFTP server at midnight by a vendor whose API roadmap is perpetually "coming soon." This is not a legacy edge case. A significant portion of the ambulatory market — including platforms like eClinicalWorks in older deployment configurations — still operates this way. Any HIE architecture that doesn't account for this path will have gaps in its participating provider coverage from day one.

This architecture handles both paths through a unified Bronze landing zone, treating the ingestion method as a transport concern that is resolved before clinical normalization begins.

---

## Layer Design

### Bronze: The Immutable Landing Zone

Bronze is where data arrives and stays, exactly as received. No transformations, no business logic, no rejections based on content.

Every HL7 message, every FHIR Bundle, every CSV row lands in Bronze as a raw payload with envelope metadata extracted for routing purposes. The clinical content is not touched.

This design serves two purposes. First, it creates a legal audit trail of exactly what each source system transmitted — critical in a regulated environment where data provenance disputes are real. Second, it enables Silver reprocessing. When terminology standards change, when a mapping error is discovered, or when a new USCDI version requires schema updates, Bronze can be replayed against updated Silver logic without re-pulling from source systems. That capability is operationally invaluable at scale.

The one exception to the no-transformation rule is envelope metadata extraction — parsing the MSH segment of an HL7 message to identify the tenant, message type, and control ID for routing. This is not clinical transformation. It is logistics.

### Silver: The Canonical Clinical Data Model

Silver is where raw data becomes coherent clinical information. Three things happen here, in order:

**Identity resolution comes first.** Before any clinical normalization occurs, every patient record is matched against the Master Patient Index and assigned a Universal Master Patient Index (UMPI). This is non-negotiable sequencing. Clinical normalization across tenants is only meaningful if you know you're talking about the same patient. Running normalization before identity resolution produces a Silver layer where the same patient exists as multiple disconnected records — one per source MRN. That defeats the purpose of a state-scale HIE.

**Semantic normalization comes second.** Local codes are mapped to standard terminologies: LOINC for labs, SNOMED-CT for clinical findings, RxNorm for medications, ICD-10-CM for diagnoses. The critical design constraint here is that these mappings are deterministic table lookups sourced from authoritative terminology releases — not model inferences. The difference between "HgbA1c" from Hospital A and "A1c" from Clinic B resolving to LOINC 4548-4 is a table lookup. It is not a guess. Every mapping is logged in the normalization audit table with its source value, mapped value, confidence score, and mapping method. Unmapped codes produce explicit UNMAPPED records, not silent failures.

**Schema alignment comes third.** Normalized data is written to the Silver CDM tables: encounters, diagnoses, lab observations, medications, ADT events. Every entity carries a tenant_id and is keyed to a UMPI.

### Gold: Analytics and Tenant Isolation

Gold is the consumption layer. It is derived entirely from Silver — nothing in Gold reads Bronze directly.

Two things define the Gold layer. First, pre-aggregation: clinical entities are joined, enriched with derived attributes (risk scores, quality measure flags, chronic condition indicators), and structured for BI consumption. Column names are business-friendly. Joins are pre-computed. The goal is query performance and semantic clarity for analysts and BI tools.

Second, row-level security. Tenant isolation is enforced at the Gold layer through RLS policies. A Hospital A analyst queries Gold and sees only Hospital A records. The HDU operator queries Gold with a cross-tenant grant and sees aggregated state-wide population data. This is enforced at the platform level — not in application code, where it can be bypassed.

---

## Identity Resolution Design

The MPI is the most consequential component in a multi-tenant HIE. Getting it wrong produces duplicate records that corrupt quality measures, risk scores, and care coordination workflows. Getting it right is what makes cross-organizational clinical data coherent.

This reference implementation uses a deterministic matching hierarchy:

1. Exact MRN + facility NPI (highest confidence — same patient, same organization)
2. Exact identifier system + identifier value (FHIR-native identifier matching)
3. Exact SSN-4 + date of birth + family name (cross-organizational linkage)
4. Exact date of birth + full name + postal code (lower confidence; flagged accordingly)

Records that match on passes 3 or 4 receive a match_confidence score below 1.0, enabling downstream filtering for workflows that require high-confidence identity.

Records that match nothing receive a new UMPI. Every UMPI assignment is logged with its match method and confidence score.

Production note: A state-scale HIE would layer probabilistic matching on top of this deterministic foundation — either a commercial referential matching service (Verato is the common choice) or an open-source Fellegi-Sunter implementation. The deterministic layer handles the clear cases. The probabilistic layer handles transposed names, address changes, and data entry errors. This reference implementation provides the interface contract that a probabilistic layer would fulfill.

---

## Multi-Tenancy Pattern

Tenant isolation is implemented as a combination of logical tagging and platform-enforced RLS.

Every record in Bronze, Silver, and Gold carries a tenant_id. In Bronze, this is set at ingestion time from the ZTN custom segment (HL7) or Bundle.meta.tag (FHIR). In Silver and Gold, it is propagated from the source record.

Row-level security at the Gold layer enforces that a tenant's session context only returns their own records. Cross-tenant analytics are available only to the HDU operator role, and only for aggregate views that do not expose individual patient records across organizational boundaries.

This pattern supports three access tiers: tenant users (single-organization view), tenant analysts (organization-scoped analytics), and HDU operators (cross-tenant population health with appropriate data sharing agreements in place).

---

## Terminology Governance

The terminology service is intentionally deterministic. The design principle is that clinical terminology mapping is a governance function, not a machine learning function.

LLMs have a legitimate role in this architecture: extracting structured information from unstructured clinical text — faxed notes, free-text fields, scanned documents. That is a problem where probabilistic inference adds value because the input is inherently ambiguous.

Mapping "HgbA1c" to LOINC 4548-4 is not an ambiguous problem. It is a table lookup. The table is maintained by the Regenstrief Institute and updated on a defined release cycle. Replacing that lookup with a model introduces unnecessary uncertainty into a process that should be fully auditable.

Every terminology mapping in this architecture is traceable to a specific authoritative source, a specific release version, and a specific log entry in silver.normalization_log.

---

## TEFCA and USCDI Alignment

The Gold layer is structured for USCDI v3 export readiness. The `gold.uscdi_patient_summary` view flattens the canonical CDM into the USCDI data element structure, enabling QHIN-compatible data exchange without additional transformation.

TEFCA readiness requires more than schema alignment — it requires governance documentation, data sharing agreements, and operational processes that are outside the scope of a reference architecture. But the data model is designed to not create unnecessary obstacles to that readiness. USCDI data classes are represented in Silver and surfaced cleanly in Gold.

---

## What This Architecture Does Not Cover

This reference implementation deliberately omits several components that a production HIE would require:

**Probabilistic MPI.** The deterministic matching hierarchy handles the clear cases. Production deployments need a referential matching layer for the ambiguous ones.

**Terminology server integration.** The static lookup tables in the reference implementation need to be replaced with calls to a hosted terminology server (NLM VSAC API or a FHIR TerminologyServer endpoint) loaded from current release files.

**Interface engine configuration.** Real-time HL7 v2 feeds arrive through interface engines (Rhapsody, Mirth Connect, Azure API for FHIR). The parser in this repo handles the message content; the connection and transport layer is infrastructure.

**Operational monitoring.** The data quality scorecard schema is defined in Gold. The jobs that populate it — feed latency monitoring, error rate alerting, LOINC coverage trending — are operational infrastructure not included here.

**PHI encryption at rest.** The schema design assumes platform-level encryption. Implementation details are cloud-platform specific.
