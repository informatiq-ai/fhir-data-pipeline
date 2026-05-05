# Canonical Clinical Data Model

## Purpose

The canonical clinical data model (CDM) is the Silver layer schema — the normalized, terminology-aligned, tenant-tagged representation of clinical data that every downstream analytics and reporting workload reads from.

The CDM is not source-system-specific. It is not Epic's data model or Cerner's data model. It is a vendor-neutral schema aligned to FHIR R4 resource structure and USCDI data class definitions, designed to represent clinical information from any participating EHR without preserving the idiosyncrasies of any particular vendor's implementation.

---

## Design Principles

**UMPI as the universal key.** Every clinical entity in the CDM is linked to a Universal Master Patient Index identifier, not a local MRN. The patient_identifiers table maintains the crosswalk from source identifiers to UMPIs. Downstream consumers never need to reason about source-system patient IDs.

**Terminology alignment is mandatory, not aspirational.** Lab observations require a LOINC code. Diagnoses require ICD-10-CM. Medications require RxNorm. Records that cannot be mapped are written with an explicit UNMAPPED status and flagged in the normalization log — they are not silently dropped and they are not written with a guessed code.

**Source codes are preserved alongside canonical codes.** The source_code, source_code_system, and source_display columns on every CDM entity preserve what the source system actually sent. This enables terminology mapping audit, retrospective remapping when local codes are clarified, and debugging of normalization failures without going back to Bronze.

**Tenant isolation is a first-class design constraint.** tenant_id is present on every table. It is set at ingestion, propagated through normalization, and enforced via RLS at the Gold layer. It is never derived or inferred — it is always explicitly assigned from the source feed configuration.

---

## Entity Reference

### master_patient_index

The golden patient record. One row per resolved patient identity across the entire HDU.

Key fields: umpi (PK), match_method (DETERMINISTIC / PROBABILISTIC / MANUAL), match_confidence, canonical demographics (family_name, given_name, birth_date, gender), merge_history (JSONB audit trail of prior UMPIs merged into this one).

The merge_history field is particularly important for operational trust. When two UMPIs are determined to represent the same patient and merged, the prior UMPI is recorded here. Any clinical entity linked to the retired UMPI is re-keyed to the surviving UMPI. The merge event is auditable.

### patient_identifiers

The crosswalk table. One row per source identifier per patient. Multiple rows per UMPI are expected and normal — a patient seen at three facilities will have three MRN rows, all pointing to the same UMPI.

Key fields: umpi (FK), tenant_id, identifier_system (the URI identifying the source organization's identifier namespace), identifier_type (MR, SS, NPI, etc. per HL7 v2-0203), identifier_value, is_active.

The unique index on (identifier_system, identifier_value) where is_active = TRUE enforces that no two active UMPIs claim the same source identifier. Violations surface merge candidates.

### encounters

One row per clinical encounter per tenant. Encounters are the organizing spine of the CDM — diagnoses, labs, medications, and ADT events all reference an encounter_id where applicable.

The encounter_class field uses the HL7 v3 ActCode vocabulary: IMP (inpatient), AMB (ambulatory), EMER (emergency), VR (virtual). This provides a consistent classification across EHR vendors that use different local encounter type taxonomies.

### diagnoses

ICD-10-CM coded diagnoses linked to encounters. Where available, SNOMED-CT codes are dual-populated from the ICD-10-to-SNOMED map maintained by NLM.

The diagnosis_rank field (1 = primary, 2+ = secondary) is critical for quality measure calculation. HEDIS and CMS measures frequently require primary diagnosis identification. Source systems encode this differently — the CDM normalizes it to a consistent integer rank.

### lab_observations

Normalized lab results with LOINC as the mandatory canonical code. The source_display field captures what the source system called the test — "A1c", "HgbA1c", "Glycated Hemoglobin" — before normalization. This is essential for debugging terminology mapping gaps.

The reference_range columns normalize the normal/abnormal range for the test as performed. Interpretation codes (H, L, N, A) are preserved from the source and provide a faster path to flagging abnormal results than re-deriving from value + reference range.

### medications

RxNorm-coded medication records covering prescriptions (MedicationRequest) and administrations (MedicationAdministration). NDC codes are preserved as a secondary identifier for claims reconciliation workflows.

The medication_intent field (order, plan, proposal, filler-order) distinguishes prescribed medications from administered medications — a distinction that matters for medication reconciliation and adherence analytics.

### adt_events

The ADT event stream normalized from HL7 v2 ADT messages. Each row represents a discrete event: admission (A01), discharge (A03), transfer (A02), registration update (A08), cancel admission (A11).

This table is the foundation for care coordination workflows. Real-time ADT feeds into care management platforms — alerting a care manager when a high-risk patient is admitted or discharged — are sourced from this entity. The event_type sequence for a given encounter_id tells the clinical story of that stay.

### normalization_log

Every terminology mapping applied during Bronze → Silver processing is recorded here. One row per mapping event.

This table answers the auditor's question: "How did LOINC 4548-4 end up on this record when the source system sent 'A1c'?" The answer is in normalization_log: source_value='A1c', mapping_method=TERMINOLOGY_SERVICE, mapping_confidence=1.0, pipeline_version=1.0.0.

It also answers the data quality analyst's question: "What percentage of our labs from Tenant X have LOINC codes?" A query against normalization_log filtered by mapping_type=LOINC_MAP and tenant_id gives that coverage rate directly.

---

## USCDI Data Class Coverage

| USCDI v3 Data Class | CDM Entity | Coverage |
|---|---|---|
| Patient Demographics | master_patient_index | Full |
| Patient Identifiers | patient_identifiers | Full |
| Encounter Information | encounters | Full |
| Problems | diagnoses (problem-list-item use) | Full |
| Diagnoses | diagnoses (encounter-diagnosis use) | Full |
| Laboratory | lab_observations | Full |
| Medications | medications | Full |
| Vital Signs | lab_observations (LOINC vital sign codes) | Full |
| Immunizations | (schema extension point) | Partial |
| Procedures | (schema extension point) | Partial |
| Care Team Members | encounters.attending_provider_npi | Partial |
| Goals | (schema extension point) | Not implemented |
| Health Concerns | (schema extension point) | Not implemented |

Immunizations and Procedures follow the same normalization pattern as lab_observations (CVX codes for immunizations, CPT/SNOMED for procedures) and are straightforward schema extensions using the patterns established here.

---

## Schema Extension Pattern

Adding a new clinical entity to the CDM follows a consistent pattern:

1. Create the Silver table with umpi FK, tenant_id, source code columns, canonical code columns, and a source_table/source_id provenance pair.
2. Add a normalization_log mapping_type constant for the new code system.
3. Add terminology mappings to the TerminologyService for the new code system.
4. Write a Bronze → Silver transform function following the pattern in bronze_to_silver.py.
5. Add Gold layer aggregations and RLS policy.
6. Add unit tests covering mapped, unmapped, and source-coded variants.

The pattern is consistent enough that a new entity (Procedures, Immunizations, Allergies) is a half-day implementation exercise once the Bronze data is landing cleanly.
