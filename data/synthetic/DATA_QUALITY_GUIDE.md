# DATA_QUALITY_GUIDE.md — Synthetic Data Quality Issue Registry

Every data quality issue injected across the synthetic batch files is documented
here. Use this as your QA checklist when running the pipeline: each issue should
produce a specific, auditable outcome in the Silver or Gold layer. Nothing should
be silently dropped.

---

## Summary

| File | Issue Type | Count | Pipeline Component | Expected Outcome |
|---|---|---|---|---|
| hl7_adt_batch.txt | Missing PID-5 (patient name) | 10 | HL7 parser → Bronze | `processing_status=PENDING`; name fields null in hl7_messages |
| hl7_adt_batch.txt | Malformed DOB in PID-7 | 10 | HL7 parser → Bronze | `processing_status=PENDING`; message_datetime null or raw value preserved |
| hl7_adt_batch.txt | Invalid gender code in PID-8 | 10 | HL7 parser → Bronze | `processing_status=PENDING`; gender value preserved as-is |
| hl7_adt_batch.txt | Duplicate ADT^A01 messages | 10 | Bronze dedup / MPI | Duplicate rows in hl7_messages; MPI returns same UMPI for both |
| hl7_adt_batch.txt | Missing MSH-4 (sending facility) | 10 | HL7 parser → Bronze | `processing_status=PENDING`; sending_facility null; tenant fallback |
| hl7_oru_batch.txt | Unmapped local lab codes | 10 | Terminology service | UNMAPPED entry in `terminology_unmapped_codes`; loinc_mapped=False |
| hl7_oru_batch.txt | Out-of-range result values | 10 | Bronze ingest / Silver QA | Value preserved in Bronze and Silver; out-of-range flag for QA scorecard |
| hl7_oru_batch.txt | Missing OBX-5 (observation value) | 10 | ORU parser → Bronze | `processing_status=PENDING`; value fields null in lab_observations |
| fhir_bundle_batch.json | Missing Patient.birthDate | 10 | FHIR ingester → MPI | MPI pass 4 (DOB+name+zip) skipped; NEW_RECORD minted; birthDate null |
| fhir_bundle_batch.json | Invalid LOINC codes in Observation | 10 | Bronze→Silver normalization | loinc_mapped=False; SOURCE_LOINC path still writes code; unmapped log entry |
| fhir_bundle_batch.json | Missing Bundle.meta.tag (tenant) | 10 | FHIR ingester → Bronze | tenant_id=default or null; fhir_resources row still written (PENDING) |
| ecw_patients.csv | Blank first_name or last_name | 5 | CSV ingester → Bronze | Row written; name fields null; MPI may degrade to pass 2 |
| ecw_patients.csv | Invalid ICD-10 codes | 5 | Bronze→Silver (Condition) | ICD-10 written as-is; SNOMED map returns UNMAPPED |
| ecw_patients.csv | DOB in wrong format (MM/DD/YYYY) | 5 | CSV ingester / MPI | DOB parse failure; birth_date null; MPI falls back to lower passes |
| ecw_patients.csv | Fully duplicate rows | 5 | Bronze dedup / MPI | Duplicate rows written; MPI returns same UMPI (DETERMINISTIC match) |
| ecw_labs.csv | Local codes with no LOINC mapping | 10 | Terminology service | UNMAPPED in `terminology_unmapped_codes`; loinc_mapped=False |
| ecw_labs.csv | Text result when numeric expected | 10 | CSV ingester / Silver | value_quantity null; value_string populated; preserved for audit |

**Total injected issues: 130**

---

## Detailed Issue Registry

### hl7_adt_batch.txt

**File:** `data/synthetic/hl7_adt_batch.txt`
**Total messages:** 1000 (500 ADT^A01 + 500 ADT^A03)
**Generator:** `generate_synthetic_data.py` → `generate_hl7_adt()`

#### DQ-ADT-001: Missing PID-5 (patient name)
- **Messages affected:** A01 messages for patients at index 10–19 (20 messages in the file)
- **PID-5 value:** `^^^^^` (empty components)
- **Pipeline component:** HL7 parser (`ingestion/hl7_parser.py`)
- **Expected Bronze outcome:** Row written with `processing_status=PENDING`; `sending_application` and facility captured; name fields null
- **MPI impact:** MPI pass 1 (name+DOB+zip) cannot match on name; pass 2 (SSN4) attempted if available
- **QA check:** `SELECT * FROM dev.fhir_bronze.hl7_messages WHERE message_type = 'ADT^A01' AND sending_application IS NOT NULL` — confirm name-related fields are null for affected rows

#### DQ-ADT-002: Malformed DOB in PID-7
- **Messages affected:** A01 messages for patients at index 20–29
- **PID-7 values injected:** `19991399`, `20001432`, `00000000`, `76/12/04`, `UNKNOWN`, `99999999`, `19850230`, `20240631`, `19800000`, `20191301`
- **Pipeline component:** HL7 timestamp parser (`parse_hl7_timestamp()` in `hl7_parser.py`)
- **Expected Bronze outcome:** Row written; `message_datetime` null or raw value preserved (parser handles gracefully)
- **QA check:** Confirm no exceptions thrown; rows land with PENDING status

#### DQ-ADT-003: Invalid gender code in PID-8
- **Messages affected:** A01 messages for patients at index 30–39
- **PID-8 values injected:** `X`, `?`, `9`, `MALE`, `""`, `FEMALE`, `TRANS`, `NB`, `3`, `U1`
- **Pipeline component:** HL7 parser
- **Expected Bronze outcome:** Row written with PENDING status; gender value preserved verbatim
- **Silver impact:** Gender normalization step should flag unrecognized values

#### DQ-ADT-004: Duplicate ADT^A01 messages
- **Messages affected:** A01 messages for patients at index 490–499 are exact copies of A01 messages for patients at index 0–9 (same MSH-10 message control ID, same PID data, same visit number)
- **Pipeline component:** Bronze ingest → MPI → Silver
- **Expected Bronze outcome:** Both rows written to `hl7_messages` (Bronze is append-only)
- **Expected MPI outcome:** Second resolution returns `match_method=DETERMINISTIC` with same UMPI as first
- **QA check:** `SELECT mrn, COUNT(*) FROM hl7_messages WHERE message_type='ADT^A01' GROUP BY mrn HAVING COUNT(*) > 1`

#### DQ-ADT-005: Missing MSH-4 (sending facility)
- **Messages affected:** A01 messages for patients at index 40–49
- **MSH-4 value:** empty string
- **Pipeline component:** HL7 parser → tenant extraction
- **Expected Bronze outcome:** Row written; `sending_facility` null; tenant resolved from ZTN segment (fallback works) or defaults
- **QA check:** Confirm `sending_facility IS NULL` for these rows; verify tenant_id resolved via ZTN

---

### hl7_oru_batch.txt

**File:** `data/synthetic/hl7_oru_batch.txt`
**Total messages:** 500 ORU^R01
**Generator:** `generate_hl7_oru()`

#### DQ-ORU-001: Unmapped local lab codes
- **Messages affected:** Indices 0–9 (10 messages)
- **Codes used:** ECW-HBA1C-001, ECW-GLUC-002, ECW-CREAT-003, ECW-CHOL-004, ECW-CBC-DIFF, ECW-BMP-007, ECW-TSH-008, ECW-PSA-009, ECW-INR-010, ECW-URIC-011
- **OBX code system:** `L` (local) — not `LN` (LOINC)
- **Pipeline component:** Terminology service (`TerminologyService.map_loinc()`)
- **Expected Silver outcome:** `loinc_mapped=False`; `loinc_map_method=UNMAPPED`; entry in `terminology_unmapped_codes`
- **QA check:** `SELECT source_code, COUNT(*) FROM dev.fhir_silver.terminology_unmapped_codes GROUP BY source_code`

#### DQ-ORU-002: Out-of-physiologically-plausible-range result values
- **Messages affected:** Indices 10–19 (10 messages)
- **Example values:** HbA1c=45.2%, glucose=1520 mg/dL, creatinine=28.7 mg/dL, WBC=95.0 K/uL
- **Pipeline component:** Bronze ingest (value preserved); Silver QA scorecard
- **Expected outcome:** Value written to Bronze and Silver unchanged; data quality scorecard flags for review
- **Note:** Bronze is immutable — values are NEVER corrected or rejected at ingest

#### DQ-ORU-003: Missing OBX-5 (observation value)
- **Messages affected:** Indices 20–29 (10 messages)
- **OBX-5 content:** empty string
- **Pipeline component:** HL7 parser → lab observation extraction
- **Expected Silver outcome:** `value_quantity=NULL`; `value_string=NULL`; row written with PENDING status
- **QA check:** `SELECT COUNT(*) FROM dev.fhir_silver.lab_observations WHERE value_quantity IS NULL AND value_string IS NULL`

---

### fhir_bundle_batch.json

**File:** `data/synthetic/fhir_bundle_batch.json`
**Total bundles:** 500 (JSON array)
**Generator:** `generate_fhir_bundles()`

#### DQ-FHIR-001: Missing Patient.birthDate
- **Bundles affected:** Indices 0–9 (10 bundles)
- **Field removed:** `Patient.birthDate`
- **Pipeline component:** FHIR ingester → MPI resolution
- **Expected MPI outcome:** Pass 4 (DOB+name+zip) cannot execute; falls to NEW_RECORD if no other pass matches
- **Expected Silver outcome:** `birth_date=NULL` in `mpi_patient_index`
- **QA check:** `SELECT COUNT(*) FROM dev.fhir_silver.mpi_patient_index WHERE birth_date IS NULL`

#### DQ-FHIR-002: Invalid LOINC codes in Observation.code
- **Bundles affected:** Indices 10–19 (10 bundles)
- **Codes used:** INVALID-9999-1 through NOLOINC-000001 (10 codes, all with `http://loinc.org` system)
- **Pipeline component:** Bronze→Silver normalization (SOURCE_LOINC path in `normalize_fhir_observation()`)
- **Expected Silver outcome:** `loinc_mapped=True` (code accepted from source); `loinc_map_method=SOURCE_LOINC`; invalid code written to `loinc_code` column; normalization_log entry created
- **Note:** The SOURCE_LOINC path trusts the source system. Post-ingest LOINC validation is needed to catch invalid codes.

#### DQ-FHIR-003: Missing Bundle.meta.tag (tenant cannot be resolved)
- **Bundles affected:** Indices 20–29 (10 bundles)
- **Bundle.meta.tag value:** empty array `[]`
- **Pipeline component:** FHIR ingester tenant extraction
- **Expected Bronze outcome:** `tenant_id` falls back to `default_tenant_id` parameter; row written with PENDING status
- **QA check:** Confirm tenant fallback behavior in fhir_ingester; rows land in `fhir_resources`

---

### ecw_patients.csv

**File:** `data/synthetic/ecw_patients.csv`
**Total rows:** 300 (280 unique + 5 duplicates at end; 5 blank-name rows)
**Generator:** `generate_ecw_patients()`

#### DQ-CSV-PAT-001: Blank first_name or last_name
- **Rows affected:** Indices 0–4 (5 rows)
- **Field value:** empty string `""`
- **Pipeline component:** CSV ingester → MPI
- **Expected MPI outcome:** Name-based passes (3 and 4) degrade; SSN4 or identifier passes attempted
- **QA check:** Filter ecw_patients for blank name fields; confirm MPI still assigns UMPI

#### DQ-CSV-PAT-002: Invalid ICD-10 codes
- **Rows affected:** Indices 5–9 (5 rows)
- **Codes used:** ZZ999, BADCODE, 99999, X00.000000, 123
- **Pipeline component:** Bronze→Silver Condition normalization
- **Expected Silver outcome:** ICD-10 code written as-is; SNOMED map returns UNMAPPED; entry in `terminology_unmapped_codes`

#### DQ-CSV-PAT-003: DOB in wrong format (MM/DD/YYYY)
- **Rows affected:** Indices 10–14 (5 rows)
- **Format:** `MM/DD/YYYY` instead of `YYYY-MM-DD`
- **Pipeline component:** CSV ingester → date parsing → MPI
- **Expected outcome:** DOB parse failure; `birth_date=NULL`; MPI pass 4 skipped; row still written

#### DQ-CSV-PAT-004: Fully duplicate rows
- **Rows affected:** Rows 295–299 are exact copies of rows 0–4
- **Pipeline component:** Bronze dedup → MPI
- **Expected Bronze outcome:** Both rows written (Bronze is append-only)
- **Expected MPI outcome:** Duplicate resolved to same UMPI via DETERMINISTIC match (identifier system + value)

---

### ecw_labs.csv

**File:** `data/synthetic/ecw_labs.csv`
**Total rows:** 500
**Generator:** `generate_ecw_labs()`

#### DQ-CSV-LAB-001: Local codes with no LOINC mapping
- **Rows affected:** Indices 0–9 (10 rows)
- **Codes used:** ECW-HBA1C-001, ECW-GLUC-002, ECW-CREAT-003, ECW-CHOL-004, ECW-CBC-DIFF, ECW-BMP-007, ECW-TSH-008, ECW-PSA-009, ECW-INR-010, ECW-URIC-011
- **Pipeline component:** Terminology service (`map_loinc()`)
- **Expected Silver outcome:** `loinc_mapped=False`; `loinc_map_method=UNMAPPED`; entry in `terminology_unmapped_codes`
- **Action required:** Add these codes to the LOINC lookup table in `TerminologyService`

#### DQ-CSV-LAB-002: Text result_value when numeric expected
- **Rows affected:** Indices 10–19 (10 rows)
- **Values:** "See note", "Pending confirmation", "Quantity not sufficient", etc.
- **Pipeline component:** CSV ingester → lab observation extraction
- **Expected Silver outcome:** `value_quantity=NULL` (parse fails gracefully); `value_string` populated with text
- **QA check:** `SELECT COUNT(*) FROM dev.fhir_silver.lab_observations WHERE value_quantity IS NULL AND value_string IS NOT NULL`

---

## Pipeline QA Checklist

After running the full pipeline against these batch files, verify:

- [ ] `hl7_messages`: 1000 rows (500 A01 + 500 A03); no rows missing
- [ ] `hl7_messages` duplicate A01s: 10 patients with 2 identical message_control_id values
- [ ] `fhir_resources`: 500+ rows (Patient + Encounter + Observation + optional Condition per bundle)
- [ ] `mpi_patient_index`: patient count ≤ 500 (patients shared across ingestion paths resolve to same UMPI)
- [ ] `mpi_identity_crosswalk`: entries for all resolved patients across all sources
- [ ] `terminology_unmapped_codes`: at minimum 20 rows (10 ORU + 10 CSV lab unmapped codes)
- [ ] `lab_observations`: rows with `loinc_mapped=False` for all unmapped codes
- [ ] `lab_observations`: rows with `value_quantity IS NULL` for text-value rows and missing OBX-5 rows
- [ ] `mpi_patient_index`: rows with `birth_date IS NULL` for the 10 bundles with missing birthDate
- [ ] Data quality scorecard: out-of-range lab values flagged for the 10 ORU DQ-ORU-002 messages
