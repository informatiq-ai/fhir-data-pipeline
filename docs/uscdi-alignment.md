# USCDI Alignment

## What USCDI Is and Why It Matters for HIE Architecture

The United States Core Data for Interoperability (USCDI) is the federally mandated baseline for health data exchange in the United States. Defined by ONC and updated through a versioned release cycle, USCDI specifies the data classes and data elements that certified health IT systems must be capable of exchanging.

For a state HIE, USCDI alignment is not optional. TEFCA (the Trusted Exchange Framework and Common Agreement) requires participating QHINs and their downstream networks to exchange USCDI-defined data elements. CMS quality reporting programs reference USCDI data classes. State Medicaid programs increasingly require USCDI compliance as a condition of HIE participation agreements.

The practical implication for data architecture is this: if your canonical data model cannot produce a USCDI-compliant export without a transformation layer built specifically for each exchange scenario, you have accumulated technical debt that will compound as USCDI versions advance. The Silver CDM in this architecture is designed so that USCDI data classes map directly to CDM entities — the Gold export view is a join and projection, not a transformation.

---

## USCDI Version Alignment

This reference implementation aligns to USCDI v3, which became effective for ONC Health IT Certification in January 2024. USCDI v4 was published in July 2024 as a draft standard for trial use. The CDM is designed to accommodate v4 additions (notably expanded care team and health status data classes) through the schema extension pattern described in the canonical data model documentation.

---

## Data Class Mapping

### Patient Demographics (USCDI v3)

| USCDI Element | CDM Location | Notes |
|---|---|---|
| First Name | silver.master_patient_index.given_name | |
| Last Name | silver.master_patient_index.family_name | |
| Previous Name | silver.patient_identifiers (name history pattern) | Extension point |
| Middle Name | silver.master_patient_index.middle_name | |
| Suffix | silver.master_patient_index (extension point) | |
| Birth Date | silver.master_patient_index.birth_date | |
| Birth Sex | silver.master_patient_index.gender | Mapped to FHIR AdministrativeGender |
| Gender Identity | silver.master_patient_index (extension point) | USCDI v3 addition |
| Preferred Language | silver.master_patient_index (extension point) | |
| Race | silver.master_patient_index (extension point) | OMB race categories |
| Ethnicity | silver.master_patient_index (extension point) | OMB ethnicity categories |
| Address | silver.master_patient_index (canonical address) | Full history in extension table |
| Phone Number | silver.master_patient_index (extension point) | |
| Email Address | silver.master_patient_index (extension point) | |

### Encounter Information (USCDI v3)

| USCDI Element | CDM Location | Notes |
|---|---|---|
| Encounter Diagnoses | silver.diagnoses (encounter-diagnosis use) | ICD-10-CM required |
| Encounter Type | silver.encounters.encounter_class | v3 ActCode vocabulary |
| Encounter Time | silver.encounters.period_start / period_end | |
| Encounter Location | silver.encounters.facility_name / facility_npi | |
| Encounter Disposition | silver.encounters.discharge_disposition_code | |

### Problems (USCDI v3)

| USCDI Element | CDM Location | Notes |
|---|---|---|
| Problem | silver.diagnoses (problem-list-item use) | |
| Date of Diagnosis | silver.diagnoses.onset_datetime | |
| Problem Status | silver.diagnoses.clinical_status | active / resolved / inactive |

### Medications (USCDI v3)

| USCDI Element | CDM Location | Notes |
|---|---|---|
| Medications | silver.medications | RxNorm required |
| Medication Instructions | silver.medications.dosage_text | |
| Medication Adherence | (extension point — claims linkage) | |

### Laboratory (USCDI v3)

| USCDI Element | CDM Location | Notes |
|---|---|---|
| Tests | silver.lab_observations.loinc_code | LOINC required |
| Values/Results | silver.lab_observations.value_quantity / value_string | |
| Result Interpretation | silver.lab_observations.interpretation_code | |
| Result Reference Range | silver.lab_observations.reference_range_* | |
| Result Status | silver.lab_observations.observation_status | |

### Vital Signs (USCDI v3)

Vital signs are stored in silver.lab_observations using the LOINC vital sign codes. Key LOINC codes:

| Vital Sign | LOINC Code |
|---|---|
| Systolic Blood Pressure | 8480-6 |
| Diastolic Blood Pressure | 8462-4 |
| Heart Rate | 8867-4 |
| Body Weight | 29463-7 |
| Body Height | 8302-2 |
| BMI | 39156-5 |
| Oxygen Saturation | 2708-6 |
| Respiratory Rate | 9279-1 |
| Body Temperature | 8310-5 |

This LOINC-based approach means vital signs and lab results share a single normalized table structure, simplifying query patterns and ensuring consistent terminology governance across both data classes.

---

## USCDI Export View

The `gold.uscdi_patient_summary` view provides a USCDI-aligned projection of the canonical CDM for exchange scenarios. In production, this would be materialized as a Delta Lake or Snowflake table with incremental refresh.

For FHIR-based exchange (required for TEFCA/QHIN participation), the Gold layer data would be serialized back into FHIR R4 resources using a FHIR server or a serialization layer. The CDM is FHIR R4-aligned, so this serialization is a projection and mapping operation, not a structural transformation — Patient resources map to master_patient_index + patient_identifiers, Observation resources map to lab_observations, Condition resources map to diagnoses, and so on.

---

## TEFCA Readiness Considerations

TEFCA participation requires more than USCDI data element coverage. The following are required capabilities that this architecture supports but does not fully implement:

**Individual Access Services (IAS).** Patients must be able to request their own records. The UMPI-keyed CDM supports patient identity resolution for IAS workflows. The access control and patient portal integration are implementation-dependent.

**Treatment.** Provider-to-provider exchange for treatment purposes is the primary TEFCA use case. The ADT event feed and the USCDI export view directly support this use case.

**Payment.** Claims-adjacent data exchange for payment purposes requires linking clinical records to claims data. The medications.ndc_code field and the encounter structure support this linkage. Full claims integration is an extension point.

**Health Care Operations.** Quality measure reporting and population health analytics. The Gold layer quality_measures table and patient_summary table directly support this use case.

**Public Health.** Reportable condition and immunization registry reporting. The diagnoses entity supports ICD-10-CM coded condition reporting. Immunizations are a schema extension point.

**Research.** De-identified or limited data set exchange. De-identification is not implemented in this reference architecture and requires a separate governance and technical framework (Safe Harbor or Expert Determination under HIPAA).
