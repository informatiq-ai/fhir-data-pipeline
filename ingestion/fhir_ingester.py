"""
ingestion/fhir_ingester.py

FHIR R4 Bundle ingester for Bronze layer landing.

Responsibilities:
  - Accept a FHIR R4 Bundle (as JSON string or dict)
  - Validate structural integrity (not clinical validity — that is Silver's job)
  - Extract one BronzeFHIRRecord per resource entry
  - Preserve the full raw JSON exactly as received
  - Tag each record with tenant_id from Bundle.meta.tag or config

Design principle: Structural validation at ingest catches malformed JSON
and missing required fields before they pollute Bronze. Clinical validation
(terminology, value ranges, required USCDI elements) happens in Silver.

A bundle that fails structural validation is still landed in Bronze with
processing_status='ERROR' and the full raw payload preserved for triage.

Requires: fhir.resources (pip install fhir.resources)
"""

import json
import logging
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

INGESTION_VERSION = "1.0.0"

# FHIR resource types we expect to process in Silver
SUPPORTED_RESOURCE_TYPES = {
    "Patient",
    "Encounter",
    "Observation",
    "Condition",
    "MedicationRequest",
    "MedicationAdministration",
    "Procedure",
    "AllergyIntolerance",
    "Immunization",
    "DiagnosticReport",
    "DocumentReference",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BronzeFHIRRecord:
    """
    Represents a single row destined for bronze.fhir_resources.
    One record per resource entry in the Bundle.
    """
    resource_id: str
    tenant_id: str
    bundle_id: Optional[str]
    bundle_timestamp: Optional[str]
    bundle_type: Optional[str]
    fhir_resource_type: str
    fhir_resource_id: Optional[str]
    fhir_version_id: Optional[str]
    raw_payload: str                   # JSON string of this resource only
    bundle_payload: Optional[str]      # Full bundle JSON (set on first resource; None on remainder to avoid bloat)
    source_system: Optional[str]
    feed_type: str = "FHIR_R4"
    batch_id: Optional[str] = None
    received_ts: str = ""
    ingestion_version: str = INGESTION_VERSION
    processing_status: str = "PENDING"
    processing_error: Optional[str] = None


@dataclass
class FHIRIngestResult:
    """Result of ingesting a single FHIR Bundle."""
    success: bool
    records: list[BronzeFHIRRecord]
    error: Optional[str]
    bundle_id: Optional[str]
    resource_count: int
    skipped_resource_types: list[str]  # resource types present but not in SUPPORTED set


# ---------------------------------------------------------------------------
# Tenant extraction
# ---------------------------------------------------------------------------

TENANT_TAG_SYSTEM = "https://oklahoma-hdu.example.org/tags/tenant"
SOURCE_TAG_SYSTEM = "https://oklahoma-hdu.example.org/tags/source"


def extract_tenant_from_meta(bundle: dict, default_tenant_id: Optional[str] = None) -> str:
    """
    Extract tenant_id from Bundle.meta.tag using the HDU tenant tag system.
    Falls back to default_tenant_id, then 'UNKNOWN'.
    """
    tags = bundle.get("meta", {}).get("tag", [])
    for tag in tags:
        if tag.get("system") == TENANT_TAG_SYSTEM:
            return tag.get("code", "UNKNOWN")
    return default_tenant_id or "UNKNOWN"


def extract_source_from_meta(bundle: dict) -> Optional[str]:
    """Extract source system tag from Bundle.meta."""
    tags = bundle.get("meta", {}).get("tag", [])
    for tag in tags:
        if tag.get("system") == SOURCE_TAG_SYSTEM:
            return tag.get("code")
    return None


# ---------------------------------------------------------------------------
# Bundle ingester
# ---------------------------------------------------------------------------

def ingest_fhir_bundle(
    raw_bundle: str | dict,
    default_tenant_id: Optional[str] = None,
    batch_id: Optional[str] = None,
    store_full_bundle: bool = True,
) -> FHIRIngestResult:
    """
    Ingest a FHIR R4 Bundle and produce Bronze records.

    Args:
        raw_bundle: JSON string or parsed dict. If string, it is preserved
                    as raw_payload exactly. If dict, it is serialized to JSON.
        default_tenant_id: Fallback tenant_id if not found in Bundle.meta.tag.
        batch_id: Optional batch identifier for traceability.
        store_full_bundle: If True, the first BronzeFHIRRecord includes the
                           full bundle_payload. Subsequent records set it to None
                           to avoid row bloat. Set False to omit entirely.

    Returns:
        FHIRIngestResult with populated records list.
        On structural failure, returns a single error record preserving raw_payload.
    """
    received_ts = datetime.now(timezone.utc).isoformat()

    # Normalize to dict while preserving the original string
    if isinstance(raw_bundle, str):
        raw_bundle_str = raw_bundle
        try:
            bundle = json.loads(raw_bundle)
        except json.JSONDecodeError as exc:
            error_record = _make_error_record(
                raw_payload=raw_bundle,
                tenant_id=default_tenant_id or "UNKNOWN",
                error=f"JSON parse error: {exc}",
                received_ts=received_ts,
                batch_id=batch_id,
            )
            return FHIRIngestResult(
                success=False,
                records=[error_record],
                error=str(exc),
                bundle_id=None,
                resource_count=0,
                skipped_resource_types=[],
            )
    else:
        bundle = raw_bundle
        raw_bundle_str = json.dumps(bundle)

    # Structural validation
    if bundle.get("resourceType") != "Bundle":
        err = f"Expected resourceType 'Bundle', got '{bundle.get('resourceType')}'"
        error_record = _make_error_record(
            raw_payload=raw_bundle_str,
            tenant_id=default_tenant_id or "UNKNOWN",
            error=err,
            received_ts=received_ts,
            batch_id=batch_id,
        )
        return FHIRIngestResult(
            success=False,
            records=[error_record],
            error=err,
            bundle_id=bundle.get("id"),
            resource_count=0,
            skipped_resource_types=[],
        )

    # Extract bundle-level metadata
    bundle_id = bundle.get("id")
    bundle_timestamp = bundle.get("timestamp")
    bundle_type = bundle.get("type")
    tenant_id = extract_tenant_from_meta(bundle, default_tenant_id)
    source_system = extract_source_from_meta(bundle)

    entries = bundle.get("entry", [])
    records = []
    skipped_types = []
    bundle_payload_str = raw_bundle_str if store_full_bundle else None

    for i, entry in enumerate(entries):
        resource = entry.get("resource", {})
        resource_type = resource.get("resourceType")

        if not resource_type:
            logger.warning("Bundle %s entry %d has no resourceType — skipping", bundle_id, i)
            skipped_types.append("UNKNOWN")
            continue

        if resource_type not in SUPPORTED_RESOURCE_TYPES:
            logger.debug("Skipping unsupported resource type: %s", resource_type)
            skipped_types.append(resource_type)
            continue

        # Serialize just this resource as the raw_payload for this row
        resource_raw = json.dumps(resource)

        record = BronzeFHIRRecord(
            resource_id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            bundle_id=bundle_id,
            bundle_timestamp=bundle_timestamp,
            bundle_type=bundle_type,
            fhir_resource_type=resource_type,
            fhir_resource_id=resource.get("id"),
            fhir_version_id=resource.get("meta", {}).get("versionId"),
            raw_payload=resource_raw,
            # Only attach full bundle payload to the first record
            bundle_payload=bundle_payload_str if i == 0 else None,
            source_system=source_system,
            feed_type="FHIR_R4",
            batch_id=batch_id,
            received_ts=received_ts,
            ingestion_version=INGESTION_VERSION,
            processing_status="PENDING",
        )
        records.append(record)

    return FHIRIngestResult(
        success=True,
        records=records,
        error=None,
        bundle_id=bundle_id,
        resource_count=len(records),
        skipped_resource_types=list(set(skipped_types)),
    )


def ingest_fhir_batch(
    bundles: list[str | dict],
    default_tenant_id: Optional[str] = None,
    batch_id: Optional[str] = None,
) -> tuple[list[BronzeFHIRRecord], list[FHIRIngestResult]]:
    """
    Ingest multiple FHIR Bundles.

    Returns:
        (all_records, failures) where all_records includes both successful
        and error records (so nothing is silently dropped), and failures
        contains the FHIRIngestResult objects for failed bundles.
    """
    all_records = []
    failures = []

    for bundle in bundles:
        result = ingest_fhir_bundle(bundle, default_tenant_id=default_tenant_id, batch_id=batch_id)
        all_records.extend(result.records)
        if not result.success:
            failures.append(result)

    return all_records, failures


def records_to_json(records: list[BronzeFHIRRecord]) -> str:
    """Serialize records to JSON array string for bulk insert staging."""
    return json.dumps([asdict(r) for r in records], default=str, indent=2)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _make_error_record(
    raw_payload: str,
    tenant_id: str,
    error: str,
    received_ts: str,
    batch_id: Optional[str] = None,
) -> BronzeFHIRRecord:
    return BronzeFHIRRecord(
        resource_id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        bundle_id=None,
        bundle_timestamp=None,
        bundle_type=None,
        fhir_resource_type="UNKNOWN",
        fhir_resource_id=None,
        fhir_version_id=None,
        raw_payload=raw_payload,
        bundle_payload=None,
        source_system=None,
        batch_id=batch_id,
        received_ts=received_ts,
        ingestion_version=INGESTION_VERSION,
        processing_status="ERROR",
        processing_error=error,
    )


# ---------------------------------------------------------------------------
# CLI demo (runs against synthetic data)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    sample_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "synthetic", "fhir_bundle_sample.json"
    )

    if not os.path.exists(sample_path):
        print(f"Synthetic sample not found at {sample_path}")
        sys.exit(1)

    with open(sample_path, "r") as f:
        raw = f.read()

    result = ingest_fhir_bundle(raw)

    if result.success:
        print(f"Ingest successful.")
        print(f"  bundle_id:       {result.bundle_id}")
        print(f"  resource_count:  {result.resource_count}")
        print(f"  skipped_types:   {result.skipped_resource_types}")
        for rec in result.records:
            print(f"  -> {rec.fhir_resource_type:<30} tenant={rec.tenant_id}  status={rec.processing_status}")
    else:
        print(f"Ingest failed: {result.error}")
        sys.exit(1)
