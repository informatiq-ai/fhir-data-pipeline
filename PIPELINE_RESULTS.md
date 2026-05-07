# Pipeline Test Results

End-to-end run of the `fhir-data-pipeline` reference implementation against synthetic clinical data. All scripts executed against the samples in `data/synthetic/` with no database connection required.

**Environment:** Python 3.12.13 · pytest 9.0.3  
**Test suite:** 109 tests · 0 failures · 0 skipped  
**Dependencies:** `hl7apy` · `fhir.resources` · `pytest`

---

## Stage 1: HL7 v2 Ingestion (Bronze)

**Script:** `ingestion/hl7_parser.py`  
**Input:** `data/synthetic/hl7_adt_sample.txt` — synthetic ADT^A01 admission message

```
Parse successful.
  tenant_id:       INTEGRIS_BAPTIST
  message_type:    ADT^A01^ADT_A01
  feed_type:       ADT
  message_id:      23630f44-a8f8-4d54-90e8-8ed313ad109e
  control_id:      MSG20240315082301
  message_ts:      2024-03-15T08:23:01
  raw_payload len: 1827 chars
```

**What this demonstrates:**

The parser extracts envelope metadata from the MSH segment (message type, control ID,
timestamp) and tenant identification from the custom ZTN segment. The full raw payload
is preserved exactly as received — no clinical content is touched at this stage.

The ZTN segment (`TENANT_ID=INTEGRIS_BAPTIST`) drives tenant assignment. If ZTN were
absent, the parser falls back to MSH.4 (sending facility), then to a configured
default. This ensures every Bronze record has a tenant_id regardless of whether the
source interface engine appends the custom segment.

The ORU^R01 lab result message (`hl7_oru_sample.txt`) parses identically, with
`feed_type=ORU` extracted from MSH.9.

---

## Stage 2: FHIR R4 Ingestion (Bronze)

**Script:** `ingestion/fhir_ingester.py`  
**Input:** `data/synthetic/fhir_bundle_sample.json` — synthetic FHIR R4 transaction Bundle

```
Ingest successful.
  bundle_id:       bundle-synthetic-001
  resource_count:  4
  skipped_types:   []
  -> Patient                        tenant=INTEGRIS_BAPTIST  status=PENDING
  -> Encounter                      tenant=INTEGRIS_BAPTIST  status=PENDING
  -> Observation                    tenant=INTEGRIS_BAPTIST  status=PENDING
  -> Condition                      tenant=INTEGRIS_BAPTIST  status=PENDING
```

**What this demonstrates:**

The ingester splits the Bundle into individual resource records — one Bronze row per
resource, not per Bundle. This is the correct design for a clinical data platform:
Patient, Encounter, Observation, and Condition have different normalization cadences
and different downstream consumers. Keeping them as atomic records allows each to be
processed, reprocessed, or queried independently.

Tenant identification is extracted from `Bundle.meta.tag` using the HDU tag system
URI. All four resources carry `tenant=INTEGRIS_BAPTIST` inherited from the Bundle
envelope. The `bundle_payload` (full Bundle JSON) is attached to the first resource
record for audit; subsequent records reference only their own resource payload to
avoid row bloat.

`skipped_types: []` confirms that all four resource types in the Bundle (Patient,
Encounter, Observation, Condition) are in the supported set and none were dropped.

---

## Stage 3: Identity Resolution (Silver)

**Script:** `transforms/identity_resolution.py`  
**Input:** Patient resource from `fhir_bundle_sample.json`

```

Resolution result:
  umpi:             dbedb88d-1000-4d48-b838-3efd3b28e915
  match_method:     NEW_RECORD
  match_confidence: 0.0
  is_new_record:    True
  matched_on:       []

Second resolution (should match):
  umpi:             dbedb88d-1000-4d48-b838-3efd3b28e915
  match_method:     DETERMINISTIC
  is_new_record:    False
  ✓ UMPI consistent across resolutions

Total patients in MPI: 1
DEBUG MPI new record minted: umpi=dbedb88d-1000-4d48-b838-3efd3b28e915 tenant=INTEGRIS_BAPTIST
DEBUG MPI match (identifier system+value): umpi=dbedb88d-1000-4d48-b838-3efd3b28e915
```

**What this demonstrates:**

First pass: Carlos Ramirez arrives from INTEGRIS_BAPTIST with MRN-29471. No prior
record exists in the MPI — a new UMPI is minted and all applicable indexes are
populated (MRN+NPI index, identifier system+value index, DOB+name+zip index).

Second pass: The same patient identity resolves to the identical UMPI via the
identifier system+value index (Pass 2 of the matching hierarchy).
`is_new_record=False` and `match_method=DETERMINISTIC` confirm the match is exact
and traceable.

This consistency guarantee — that the same source identity always resolves to the
same UMPI — is the foundation of cross-encounter and cross-tenant clinical coherence.
Every Silver entity (encounter, diagnosis, lab) written after this step is keyed to
the resolved UMPI, not to MRN-29471.

---

## Stage 4: Bronze → Silver Normalization

**Script:** `transforms/bronze_to_silver.py`  
**Input:** Observation resource (HbA1c) from `fhir_bundle_sample.json`

```

Silver lab record:
  loinc_code:       4548-4
  loinc_display:    Hemoglobin A1c/Hemoglobin.total in Blood
  source_display:   HgbA1c
  loinc_mapped:     True
  loinc_map_method: SOURCE_LOINC
  value:            8.2 %
  interpretation:   H
  norm_log_entries: 1
```

**What this demonstrates:**

The synthetic FHIR bundle sends LOINC 4548-4 directly in the Observation coding —
common from Epic and Cerner implementations with mature terminology configuration.
The normalizer accepts the source LOINC and records `loinc_map_method=SOURCE_LOINC`,
indicating no local mapping was required.

The `source_display` field (`HgbA1c`) is preserved alongside the canonical LOINC
display (`Hemoglobin A1c/Hemoglobin.total in Blood`). This is what enables
retrospective analysis of terminology consistency across tenants: how many different
local display strings resolve to the same LOINC code, and which tenants are sending
non-standard displays that require terminology service fallback.

One normalization log entry is written regardless of map method. For the terminology
service fallback path (simulating an eClinicalWorks CSV where the source sends
`"A1c"` with no code system), the log entry records
`mapping_method=TERMINOLOGY_SERVICE`. For an unmapped code, it records
`mapping_method=UNMAPPED` with `mapping_confidence=0.0`. Nothing is silent.

---

## Stage 5: Silver → Gold Analytics

**Script:** `transforms/silver_to_gold.py`  
**Input:** Synthetic Silver entities constructed from the synthetic patient scenario

```

=== Patient Summary ===
  patient_key:        79cb981a7011d07a...
  full_name:          Carlos Ramirez
  age:                49
  charlson_index:     2
  risk_tier:          MODERATE
  flag_diabetes:      True
  flag_hypertension:  True
  flag_heart_failure: False
  encounters_12m:     0
  attributed_pcp:     None

=== Quality Measure: CDC HbA1c Control ===
  denominator:        True
  numerator:          False
  evidence_value:     8.2%
  evidence_date:      2024-03-15
  (HbA1c 8.2% → not in control → numerator=False)

=== ADT Event Feed (2 events) ===
  A01 (Admission   ) 2024-03-15 08:20  readmission=False
  A03 (Discharge   ) 2024-03-18 14:00  readmission=False
```

**What this demonstrates:**

**Patient Summary:** Carlos Ramirez carries three active diagnoses — AMI (I21.9),
Type 2 DM (E11.9), and hypertension (I10). The Charlson Comorbidity Index scores
him at 2 (AMI weight 1 + DM without complications weight 1), placing him in the
MODERATE risk tier. The chronic condition flags surface correctly from the global
ICD-10 value sets: `flag_diabetes=True`, `flag_hypertension=True`,
`flag_heart_failure=False`.

The `patient_key` is a SHA-256 hash of the UMPI — what Gold layer consumers
(BI tools, analysts) receive instead of the raw identifier. The raw UMPI stays
in Silver.

**Quality Measure:** The CDC HbA1c Control measure places Carlos in the denominator
(age 18-75, confirmed diabetes diagnosis). His most recent HbA1c of 8.2% fails the
`<8.0%` threshold — `numerator=False`. In a provider's HEDIS report, this patient
counts against their diabetes control rate. The `evidence_date` and `evidence_value`
fields provide the supporting documentation for the measure calculation, enabling
audit without re-running the full measure logic.

**ADT Event Feed:** The two-event sequence (A01 admission on 2024-03-15, A03
discharge on 2024-03-18) represents a 3-day inpatient stay. `is_readmission_30d=False`
for the admission because there is no prior discharge in the 30-day window. The
readmission flag uses `delta.total_seconds()` rather than `delta.days` to correctly
handle same-day readmissions where a discharge and re-admission occur within the
same calendar day.

---

## Test Suite

**Runner:** `python -m pytest tests/ -v`

```
109 passed in 0.32s
```

| Test Class | Tests | Coverage |
|---|---|---|
| TestHL7Timestamp | 5 | Timestamp parsing including timezone offset stripping |
| TestHL7Parser | 9 | ADT/ORU parsing, malformed handling, batch processing, tenant fallback |
| TestFHIRIngester | 8 | Bundle splitting, tenant extraction, raw payload integrity, error records |
| TestMPIIndex | 6 | UMPI minting, deterministic matching, cross-facility SSN4+DOB match |
| TestTerminologyService | 8 | LOINC/RxNorm/SNOMED mappings, case insensitivity, unmapped handling |
| TestNormalizeFHIRObservation | 5 | SOURCE_LOINC path, terminology fallback, unmapped audit log, value extraction |

Notable test cases:

`test_ssn4_dob_name_match` — validates that a patient presenting at a second facility with a different MRN resolves to the same UMPI via the SSN4 + DOB + family name matching pass. This is the cross-organizational identity linkage that makes a multi-tenant HIE clinically useful.

`test_terminology_service_fallback_for_local_display` — simulates an eClinicalWorks CSV row where the source sends `"A1c"` with no standard code system. The terminology service maps it to LOINC 4548-4 with `loinc_map_method=TERMINOLOGY_SERVICE`. This is the real-world batch ingestion problem: closed vendors that hand you a CSV and call it interoperability.

`test_unmapped_loinc_still_produces_record` — confirms that an observation with no mappable code still produces a Silver record with `loinc_mapped=False` and an explicit UNMAPPED entry in the normalization log. No data is silently dropped; every failure is visible and auditable.

`test_batch_with_one_malformed` — confirms that a malformed HL7 message in a batch does not stop processing of valid messages. Both records land in Bronze: the valid one with `processing_status=PENDING`, the malformed one with `processing_status=ERROR` and the parse exception in `processing_error`. The pipeline never silently discards data.

---

## Running Locally

```bash
pip install hl7apy fhir.resources pytest

# Run full test suite
python -m pytest tests/ -v

# Run individual stage demos
python ingestion/hl7_parser.py
python ingestion/fhir_ingester.py
python transforms/identity_resolution.py
python transforms/bronze_to_silver.py
python transforms/silver_to_gold.py
```

No cloud credentials, no database connection, no environment configuration required. All demos run against the synthetic data in `data/synthetic/`.
