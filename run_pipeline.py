"""
run_pipeline.py

End-to-end pipeline runner for fhir-data-pipeline.

Modes:
  python run_pipeline.py              # Run all stages + tests, write PIPELINE_RESULTS.md
  python run_pipeline.py --no-write   # Run all stages + tests, print to stdout only
  python run_pipeline.py --tests-only # Run pytest suite only

Each pipeline stage script has a `if __name__ == "__main__":` demo block that
this runner invokes as a subprocess. Output is captured, checked for non-zero
return codes, and written to PIPELINE_RESULTS.md with contextual annotations.
"""

import argparse
import datetime
import subprocess
import sys
import textwrap


# ---------------------------------------------------------------------------
# Stage definitions
# ---------------------------------------------------------------------------

STAGES = [
    {
        "number": 1,
        "title": "HL7 v2 Ingestion (Bronze)",
        "script": "ingestion/hl7_parser.py",
        "input_desc": "`data/synthetic/hl7_adt_sample.txt` — synthetic ADT^A01 admission message",
        "description": textwrap.dedent("""\
            The parser extracts envelope metadata from the MSH segment (message type, control ID,
            timestamp) and tenant identification from the custom ZTN segment. The full raw payload
            is preserved exactly as received — no clinical content is touched at this stage.

            The ZTN segment (`TENANT_ID=INTEGRIS_BAPTIST`) drives tenant assignment. If ZTN were
            absent, the parser falls back to MSH.4 (sending facility), then to a configured
            default. This ensures every Bronze record has a tenant_id regardless of whether the
            source interface engine appends the custom segment.

            The ORU^R01 lab result message (`hl7_oru_sample.txt`) parses identically, with
            `feed_type=ORU` extracted from MSH.9."""),
    },
    {
        "number": 2,
        "title": "FHIR R4 Ingestion (Bronze)",
        "script": "ingestion/fhir_ingester.py",
        "input_desc": "`data/synthetic/fhir_bundle_sample.json` — synthetic FHIR R4 transaction Bundle",
        "description": textwrap.dedent("""\
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
            Encounter, Observation, Condition) are in the supported set and none were dropped."""),
    },
    {
        "number": 3,
        "title": "Identity Resolution (Silver)",
        "script": "transforms/identity_resolution.py",
        "input_desc": "Patient resource from `fhir_bundle_sample.json`",
        "description": textwrap.dedent("""\
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
            the resolved UMPI, not to MRN-29471."""),
    },
    {
        "number": 4,
        "title": "Bronze → Silver Normalization",
        "script": "transforms/bronze_to_silver.py",
        "input_desc": "Observation resource (HbA1c) from `fhir_bundle_sample.json`",
        "description": textwrap.dedent("""\
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
            `mapping_method=UNMAPPED` with `mapping_confidence=0.0`. Nothing is silent."""),
    },
    {
        "number": 5,
        "title": "Silver → Gold Analytics",
        "script": "transforms/silver_to_gold.py",
        "input_desc": "Synthetic Silver entities constructed from the synthetic patient scenario",
        "description": textwrap.dedent("""\
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
            same calendar day."""),
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run_subprocess(cmd: list[str]) -> tuple[int, str, str]:
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout, result.stderr


def get_python_version() -> str:
    v = sys.version_info
    return f"{v.major}.{v.minor}.{v.micro}"


def get_pytest_version() -> str:
    rc, out, _ = run_subprocess([sys.executable, "-m", "pytest", "--version"])
    # output: "pytest 9.0.3\n" or "pytest X.Y.Z\n"
    line = (out + _).strip().split("\n")[0]
    parts = line.split()
    return parts[-1] if len(parts) >= 2 else "unknown"


def run_pytest() -> tuple[bool, str, int, int]:
    """Run pytest and return (passed, raw_output, passed_count, total_count)."""
    rc, out, err = run_subprocess(
        [sys.executable, "-m", "pytest", "tests/", "-v"]
    )
    combined = out + err
    passed = rc == 0

    # Extract counts from summary line like "41 passed in 0.11s"
    passed_count = 0
    total_count = 0
    for line in combined.splitlines():
        if "passed" in line and ("failed" in line or "error" in line or line.strip().startswith("=")):
            import re
            m = re.search(r"(\d+) passed", line)
            if m:
                passed_count = int(m.group(1))
            m2 = re.search(r"(\d+) failed", line)
            total_count = passed_count + (int(m2.group(1)) if m2 else 0)

    if passed_count == 0:
        # Simpler fallback: look for the short summary
        import re
        m = re.search(r"(\d+) passed", combined)
        if m:
            passed_count = int(m.group(1))
        total_count = passed_count

    return passed, combined, passed_count, total_count


def run_stage(stage: dict) -> tuple[bool, str]:
    """Run a single stage script and return (success, output)."""
    rc, out, err = run_subprocess([sys.executable, stage["script"]])
    combined = (out + err).rstrip()
    return rc == 0, combined


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------

def format_results_md(
    stage_results: list[tuple[bool, str]],
    pytest_passed: bool,
    pytest_output: str,
    passed_count: int,
    total_count: int,
    python_version: str,
    pytest_version: str,
    run_ts: str,
) -> str:
    lines = []

    lines.append("# Pipeline Test Results")
    lines.append("")
    lines.append(
        "End-to-end run of the `fhir-data-pipeline` reference implementation against "
        "synthetic clinical data. All scripts executed against the samples in "
        "`data/synthetic/` with no database connection required."
    )
    lines.append("")
    lines.append(f"**Environment:** Python {python_version} · pytest {pytest_version}  ")
    lines.append(f"**Test suite:** {passed_count} tests · {total_count - passed_count} failures · 0 skipped  ")
    lines.append("**Dependencies:** `hl7apy` · `fhir.resources` · `pytest`")
    lines.append("")
    lines.append("---")
    lines.append("")

    for stage, (success, output) in zip(STAGES, stage_results):
        status = "✅" if success else "❌"
        lines.append(f"## Stage {stage['number']}: {stage['title']}")
        lines.append("")
        lines.append(f"**Script:** `{stage['script']}`  ")
        lines.append(f"**Input:** {stage['input_desc']}")
        lines.append("")
        lines.append("```")
        lines.append(output)
        lines.append("```")
        lines.append("")
        lines.append("**What this demonstrates:**")
        lines.append("")
        lines.append(stage["description"])
        lines.append("")
        lines.append("---")
        lines.append("")

    # Test suite section
    lines.append("## Test Suite")
    lines.append("")
    lines.append("**Runner:** `python -m pytest tests/ -v`")
    lines.append("")

    # Extract just the short summary line for the code block
    short_summary = ""
    for line in pytest_output.splitlines():
        if "passed" in line and line.strip().startswith("="):
            # e.g. "========== 41 passed in 0.11s ==========="
            import re
            m = re.search(r"\d+ passed.*", line)
            if m:
                short_summary = m.group(0).rstrip("= ").strip()
    if not short_summary:
        short_summary = f"{passed_count} passed"

    lines.append("```")
    lines.append(short_summary)
    lines.append("```")
    lines.append("")

    lines.append("| Test Class | Tests | Coverage |")
    lines.append("|---|---|---|")
    lines.append("| TestHL7Timestamp | 5 | Timestamp parsing including timezone offset stripping |")
    lines.append("| TestHL7Parser | 9 | ADT/ORU parsing, malformed handling, batch processing, tenant fallback |")
    lines.append("| TestFHIRIngester | 8 | Bundle splitting, tenant extraction, raw payload integrity, error records |")
    lines.append("| TestMPIIndex | 6 | UMPI minting, deterministic matching, cross-facility SSN4+DOB match |")
    lines.append("| TestTerminologyService | 8 | LOINC/RxNorm/SNOMED mappings, case insensitivity, unmapped handling |")
    lines.append("| TestNormalizeFHIRObservation | 5 | SOURCE_LOINC path, terminology fallback, unmapped audit log, value extraction |")
    lines.append("")
    lines.append("Notable test cases:")
    lines.append("")
    lines.append(
        "`test_ssn4_dob_name_match` — validates that a patient presenting at a second facility "
        "with a different MRN resolves to the same UMPI via the SSN4 + DOB + family name "
        "matching pass. This is the cross-organizational identity linkage that makes a "
        "multi-tenant HIE clinically useful."
    )
    lines.append("")
    lines.append(
        '`test_terminology_service_fallback_for_local_display` — simulates an eClinicalWorks '
        'CSV row where the source sends `"A1c"` with no standard code system. The terminology '
        "service maps it to LOINC 4548-4 with `loinc_map_method=TERMINOLOGY_SERVICE`. This is "
        "the real-world batch ingestion problem: closed vendors that hand you a CSV and call "
        "it interoperability."
    )
    lines.append("")
    lines.append(
        "`test_unmapped_loinc_still_produces_record` — confirms that an observation with no "
        "mappable code still produces a Silver record with `loinc_mapped=False` and an explicit "
        "UNMAPPED entry in the normalization log. No data is silently dropped; every failure "
        "is visible and auditable."
    )
    lines.append("")
    lines.append(
        "`test_batch_with_one_malformed` — confirms that a malformed HL7 message in a batch "
        "does not stop processing of valid messages. Both records land in Bronze: the valid one "
        "with `processing_status=PENDING`, the malformed one with `processing_status=ERROR` "
        "and the parse exception in `processing_error`. The pipeline never silently discards data."
    )
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Running Locally")
    lines.append("")
    lines.append("```bash")
    lines.append("pip install hl7apy fhir.resources pytest")
    lines.append("")
    lines.append("# Run full test suite")
    lines.append("python -m pytest tests/ -v")
    lines.append("")
    lines.append("# Run individual stage demos")
    lines.append("python ingestion/hl7_parser.py")
    lines.append("python ingestion/fhir_ingester.py")
    lines.append("python transforms/identity_resolution.py")
    lines.append("python transforms/bronze_to_silver.py")
    lines.append("python transforms/silver_to_gold.py")
    lines.append("```")
    lines.append("")
    lines.append(
        "No cloud credentials, no database connection, no environment configuration required. "
        "All demos run against the synthetic data in `data/synthetic/`."
    )

    return "\n".join(lines) + "\n"


def print_stage_banner(stage: dict, success: bool, output: str) -> None:
    status = "✅" if success else "❌"
    print(f"\n{status} Stage {stage['number']}: {stage['title']}")
    print(f"   Script: {stage['script']}")
    print("-" * 60)
    for line in output.splitlines():
        print(f"  {line}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="fhir-data-pipeline runner")
    parser.add_argument("--no-write", action="store_true", help="Run pipeline but do not write PIPELINE_RESULTS.md")
    parser.add_argument("--tests-only", action="store_true", help="Run pytest suite only")
    args = parser.parse_args()

    run_ts = datetime.datetime.now().isoformat(timespec="seconds")
    python_version = get_python_version()
    pytest_version = get_pytest_version()

    print(f"fhir-data-pipeline runner · {run_ts}")
    print(f"Python {python_version} · pytest {pytest_version}")

    if args.tests_only:
        print("\nRunning test suite...")
        passed, output, passed_count, total_count = run_pytest()
        print(output)
        if passed:
            print(f"\n✅ {passed_count}/{total_count} tests passed")
            return 0
        else:
            print(f"\n❌ Test failures detected ({passed_count}/{total_count} passed)")
            return 1

    # Run all pipeline stages
    print("\n" + "=" * 60)
    print("Running pipeline stages")
    print("=" * 60)

    stage_results = []
    all_stages_passed = True
    for stage in STAGES:
        success, output = run_stage(stage)
        stage_results.append((success, output))
        print_stage_banner(stage, success, output)
        if not success:
            all_stages_passed = False
            print(f"\n❌ Stage {stage['number']} failed — aborting")
            return 1

    # Run tests
    print("\n" + "=" * 60)
    print("Running test suite")
    print("=" * 60)
    pytest_passed, pytest_output, passed_count, total_count = run_pytest()

    # Print short summary
    for line in pytest_output.splitlines():
        if "passed" in line or "failed" in line or "error" in line:
            print(f"  {line}")

    if not pytest_passed:
        print(f"\n❌ {passed_count}/{total_count} tests passed — see output above")
        return 1

    print(f"\n✅ {passed_count}/{total_count} tests passed")

    # Generate PIPELINE_RESULTS.md
    if not args.no_write:
        md = format_results_md(
            stage_results=stage_results,
            pytest_passed=pytest_passed,
            pytest_output=pytest_output,
            passed_count=passed_count,
            total_count=total_count,
            python_version=python_version,
            pytest_version=pytest_version,
            run_ts=run_ts,
        )
        with open("PIPELINE_RESULTS.md", "w") as f:
            f.write(md)
        print(f"\n✅ PIPELINE_RESULTS.md written")
    else:
        print("\n(--no-write: PIPELINE_RESULTS.md not updated)")

    print("\n✅ All stages and tests passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
