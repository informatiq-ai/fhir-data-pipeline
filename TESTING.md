# TESTING.md — fhir-data-pipeline

Complete test documentation for the `fhir-data-pipeline` reference architecture.
Updated after every session that adds, modifies, or removes tests.

---

## Test Philosophy

Every layer of the pipeline has a failure mode that must be explicitly tested.
The goal is not coverage for its own sake — it is confidence that the three core
guarantees of this architecture hold under edge cases:

1. **Nothing is silently dropped.** Malformed messages land in Bronze with
   `processing_status=ERROR`. Unmapped codes produce explicit UNMAPPED log entries.
2. **Identity resolution is consistent.** The same patient identity always resolves
   to the same UMPI, regardless of which tenant or feed it arrives from.
3. **Terminology mapping is deterministic.** A source code or display string always
   maps to the same canonical code. The mapping is always audited.

---

## Running Tests

```bash
# Full suite (recommended before every commit)
python -m pytest tests/ -v

# Via runner
python run_pipeline.py --tests-only

# Via Makefile
make test

# Single test class
python -m pytest tests/ -v -k TestMPIIndex

# Single test
python -m pytest tests/ -v -k test_ssn4_dob_name_match
```

**Requirements:** `pip install hl7apy fhir.resources pytest`
**No database, no cloud credentials, no environment variables required.**

---

## Test Suite Status

| Status | Count |
|---|---|
| ✅ Passing | 41 |
| ❌ Failing | 0 |
| ⏭ Skipped | 0 |
| 🔲 Planned (not yet written) | See Integration Tests section |

Last confirmed passing: see `PIPELINE_RESULTS.md`

---

## Unit Tests

All unit tests live in `tests/test_transforms.py`.
Fixtures load synthetic data from `data/synthetic/`.

---

### TestHL7Timestamp (5 tests)

Tests for `ingestion/hl7_parser.py` → `parse_hl7_timestamp()`

HL7 v2 timestamps arrive in multiple formats depending on the sending system's
configuration. The parser must handle all of them without raising exceptions.

| Test | Input | Expected | Notes |
|---|---|---|---|
| `test_full_timestamp` | `"20240315082301"` | `"2024-03-15T08:23:01"` | Standard 14-digit format |
| `test_date_only` | `"20240315"` | `"2024-03-15T00:00:00"` | 8-digit date only |
| `test_empty_string` | `""` | `None` | No exception |
| `test_none` | `None` | `None` | No exception |
| `test_with_timezone_offset` | `"20240315082301+0500"` | not None | Timezone offset stripped before parsing |

**Edge case rationale:** `test_with_timezone_offset` — interface engines in
some health systems append a UTC offset to the MSH.7 timestamp. The parser
strips the offset before parsing so the timestamp is stored as naive local time,
consistent with how most HL7 v2 implementations treat message timestamps.

---

### TestHL7Parser (9 tests)

Tests for `ingestion/hl7_parser.py` → `parse_hl7_message()`, `parse_hl7_batch()`

| Test | Scenario | Expected |
|---|---|---|
| `test_parse_adt_success` | Valid ADT^A01 message | `success=True`, record populated |
| `test_adt_tenant_from_ztn` | ZTN segment present | `tenant_id=INTEGRIS_BAPTIST` |
| `test_adt_message_type` | MSH.9 present | `message_type` contains `"ADT"` |
| `test_adt_feed_type` | ADT message | `feed_type=ADT` |
| `test_raw_payload_preserved` | Any valid message | `raw_payload == original string` |
| `test_parse_oru_success` | Valid ORU^R01 message | `success=True`, `feed_type=ORU` |
| `test_malformed_message_returns_error_record` | `"NOT_A_VALID_HL7_MESSAGE"` | `success=False`, `raw_payload` preserved |
| `test_batch_with_one_malformed` | 1 valid + 1 malformed | Both land in Bronze; malformed has `processing_status=ERROR` |
| `test_default_tenant_fallback` | ZTN stripped from message | `tenant_id` falls back to `default_tenant_id` parameter |

**Key design tests:**

`test_raw_payload_preserved` — the raw payload must be byte-for-byte identical
to the input string. This is the Bronze immutability guarantee. If this test
ever fails, the Bronze layer is no longer a legal audit trail.

`test_batch_with_one_malformed` — confirms that a bad message in a batch does
not stop processing of subsequent valid messages. Both records land in Bronze.
The error is visible, not silent.

`test_default_tenant_fallback` — simulates a feed where the interface engine
does not append a ZTN segment. Tenant assignment falls back to the
`default_tenant_id` configured for that connection.

---

### TestFHIRIngester (8 tests)

Tests for `ingestion/fhir_ingester.py` → `ingest_fhir_bundle()`

| Test | Scenario | Expected |
|---|---|---|
| `test_ingest_bundle_success` | Valid FHIR R4 Bundle | `success=True`, records > 0 |
| `test_ingest_produces_one_record_per_resource` | 4-resource Bundle | `resource_count=4` |
| `test_tenant_extracted_from_meta_tag` | Bundle with `meta.tag` | All records have `tenant_id=INTEGRIS_BAPTIST` |
| `test_raw_payload_is_valid_json` | Any valid Bundle | Each record's `raw_payload` parses as JSON |
| `test_raw_payload_is_resource_not_bundle` | Any valid Bundle | `raw_payload.resourceType != "Bundle"` |
| `test_bundle_payload_only_on_first_record` | `store_full_bundle=True` | First record has `bundle_payload`, rest have `None` |
| `test_invalid_json_returns_error_record` | `"{not valid json}"` | `success=False`, error record in Bronze |
| `test_non_bundle_resource_type_fails` | Patient resource (not Bundle) | `success=False` |

**Key design tests:**

`test_raw_payload_is_resource_not_bundle` — confirms that each Bronze row
contains the individual resource JSON, not the full Bundle. Patient, Encounter,
Observation, and Condition have different normalization cadences. Storing them
as atomic records allows each to be processed independently.

`test_bundle_payload_only_on_first_record` — the full Bundle JSON is attached
to the first resource record for audit traceability. Attaching it to every
record would cause significant row bloat for large Bundles.

---

### TestMPIIndex (6 tests)

Tests for `transforms/identity_resolution.py` → `MPIIndex.resolve()`

Identity resolution is the most consequential component in a multi-tenant HIE.
These tests verify all four passes of the deterministic matching hierarchy.

| Test | Scenario | Expected |
|---|---|---|
| `test_new_patient_gets_umpi` | No prior record | `is_new_record=True`, UMPI assigned |
| `test_same_mrn_npi_matches` | Same identity resolved twice | Both resolutions return identical UMPI |
| `test_different_mrn_different_umpi` | Two distinct patients | Different UMPIs assigned |
| `test_ssn4_dob_name_match` | Same patient, different facility MRN | Resolved to same UMPI via SSN4+DOB+name (Pass 3) |
| `test_patient_count_increments` | Two distinct patients added | `patient_count=2` |
| `test_fhir_patient_to_identity` | FHIR Patient resource | Demographics correctly extracted |

**Key design test:**

`test_ssn4_dob_name_match` — the most important MPI test. Simulates the
real-world scenario where the same patient has been seen at two different
facilities and has two different MRNs. The first resolution mints a UMPI via
MRN+NPI (Pass 1). The second resolution arrives with a different MRN and
facility NPI but matching SSN4+DOB+family name — it matches on Pass 3 and
returns the same UMPI.

This is the cross-organizational identity linkage that makes a multi-tenant
HIE clinically useful. Without it, the same patient exists as disconnected
records in the Silver layer.

**Note on `test_different_mrn_different_umpi`:** The two identities must be
genuinely distinct across all four matching passes — different MRN, NPI,
identifier system, SSN4, name, DOB, and postal code. Sharing any one of these
would correctly trigger a match, which is intended behavior but would cause
the test to fail with a misleading error. The test fixture is constructed to
ensure no overlap.

---

### TestTerminologyService (8 tests)

Tests for `transforms/bronze_to_silver.py` → `TerminologyService`

| Test | Input | Expected |
|---|---|---|
| `test_loinc_map_exact_match` | `"HbA1c"` | `("4548-4", "Hemoglobin A1c/Hemoglobin.total in Blood")` |
| `test_loinc_map_case_insensitive` | `"HGBA1C"` vs `"hgba1c"` | Identical result |
| `test_loinc_map_a1c_alias` | `"A1c"` | `("4548-4", ...)` |
| `test_loinc_unmapped_returns_none` | `"SOME_CUSTOM_LOCAL_CODE_XYZZY"` | `None` |
| `test_rxnorm_metformin` | `"metformin"` | RxNorm code, display contains `"metformin"` |
| `test_rxnorm_unmapped_returns_none` | `"DRUG_NOT_IN_TABLE"` | `None` |
| `test_snomed_from_icd10` | `"I21.9"` | `("57054005", "Acute myocardial infarction")` |
| `test_snomed_unmapped_returns_none` | `"Z99.999"` | `None` |

**Key design tests:**

`test_loinc_map_a1c_alias` — this test encodes the canonical HIE terminology
problem. "A1c" is a common local display string in eClinicalWorks CSV exports.
It must map to the same LOINC code (4548-4) as "HgbA1c", "HbA1c",
"Hemoglobin A1c", and "Glycated Hemoglobin." Without this mapping, the same
lab result appears under different codes across tenants and quality measures
break.

`test_loinc_unmapped_returns_none` — unmapped codes must return `None`
explicitly, not a guess. The calling code checks for `None` and writes an
UNMAPPED normalization log entry. If the terminology service returned a
default value instead of `None`, unmapped codes would silently receive
incorrect LOINC codes.

---

### TestNormalizeFHIRObservation (5 tests)

Tests for `transforms/bronze_to_silver.py` → `normalize_fhir_observation()`

| Test | Scenario | Expected |
|---|---|---|
| `test_normalize_hba1c_loinc_from_source` | Source sends LOINC directly | `loinc_code=4548-4`, `loinc_map_method=SOURCE_LOINC` |
| `test_normalize_produces_norm_log_entry` | Any observation | At least 1 norm log entry with `mapping_type=LOINC_MAP` |
| `test_normalize_value_quantity` | Observation with valueQuantity | `value_quantity=8.2`, `value_unit="%"` |
| `test_unmapped_loinc_still_produces_record` | Unknown local code | `loinc_mapped=False`, UNMAPPED entry in norm log |
| `test_terminology_service_fallback_for_local_display` | `"A1c"` with LOCAL system | `loinc_code=4548-4`, `loinc_map_method=TERMINOLOGY_SERVICE` |

**Key design tests:**

`test_unmapped_loinc_still_produces_record` — an observation that cannot be
mapped to LOINC must still produce a Silver record. Dropping unmapped
observations would cause silent data loss that would only be discovered when
a quality measure calculation came up short. The record is written with
`loinc_mapped=False` and the gap is visible in `silver.normalization_log`.

`test_terminology_service_fallback_for_local_display` — the integration of
the terminology service fallback path. Simulates the eClinicalWorks CSV
scenario: source sends `code="A1C"`, `system="LOCAL"`, `display="A1c"`.
The normalization function finds no LOINC in the source coding, falls through
to the terminology service, maps on display text, and writes
`loinc_map_method=TERMINOLOGY_SERVICE`. This is the real-world batch
ingestion problem made testable.

---

## Integration Tests

**Status: Not yet implemented.**

Integration tests wire the full ingestion → normalization → Gold construction
pipeline as a single end-to-end pass and assert on final Gold outputs rather
than intermediate structures.

**Planned file:** `tests/test_integration.py`

### Planned Integration Test Cases

**`test_hl7_adt_to_silver_encounter`**
Parse the synthetic ADT^A01 message → resolve identity → write Silver encounter.
Assert: encounter.tenant_id, encounter.source_encounter_id, UMPI assignment.

**`test_fhir_bundle_to_silver_full`**
Ingest synthetic FHIR Bundle → resolve Patient identity → normalize Observation
and Condition → build Silver records.
Assert: lab_observation.loinc_code, diagnosis.icd10_code, all records share same UMPI.

**`test_cross_tenant_mpi_linkage`**
Same synthetic patient arrives via HL7 from tenant A and FHIR from tenant B
with different MRNs but matching SSN4+DOB+name.
Assert: Both Silver records resolve to the same UMPI.
Assert: patient_identifiers has two rows (one per source) for that UMPI.

**`test_full_pipeline_to_gold`**
Run complete Bronze → Silver → Gold pipeline against all synthetic data.
Assert on GoldPatientSummary: charlson_index, risk_tier, condition flags.
Assert on GoldQualityMeasure: denominator=True, numerator=False for HbA1c 8.2%.
Assert on GoldADTEventFeed: 2 events, is_readmission_30d=False.

**`test_malformed_batch_does_not_block_valid`**
Submit a batch of 5 HL7 messages with 2 malformed.
Assert: 3 valid records reach Silver.
Assert: 2 error records are in Bronze with processing_status=ERROR.
Assert: Silver has no records for the malformed messages.

---

## Deployment Tests

**Status: Not yet implemented. No target environment defined.**

Deployment tests validate that the pipeline runs correctly against a real
cloud data warehouse (Databricks or Snowflake). These tests require environment
credentials stored in GitHub Secrets and are not part of the standard CI run.

When a target environment is defined, add:
- `tests/test_deployment.py` — schema validation, RLS enforcement, incremental load
- `.github/workflows/deploy.yml` — deployment workflow with environment protection rules

---

## Adding New Tests

When adding a new test:

1. Add it to the appropriate class in `tests/test_transforms.py` (unit) or
   the planned `tests/test_integration.py` (integration).
2. Update the test count in the Status table at the top of this file.
3. Add a row to the relevant test class table above.
4. Run `python run_pipeline.py` to regenerate `PIPELINE_RESULTS.md`.
5. Commit all three files together: the test, TESTING.md, and PIPELINE_RESULTS.md.

When removing or renaming a test, update this file in the same commit.
TESTING.md must always reflect the actual state of the test suite.
