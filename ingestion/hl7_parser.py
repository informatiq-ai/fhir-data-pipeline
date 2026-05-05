"""
ingestion/hl7_parser.py

HL7 v2 message parser for Bronze layer ingestion.

Responsibilities:
  - Accept a raw HL7 v2 message string (pipe-delimited)
  - Parse MSH segment to extract envelope metadata
  - Parse ZTN custom segment for tenant identification
  - Produce a Bronze-ready record (raw_payload preserved, metadata extracted)
  - Handle malformed messages without silent failure

Design principle: This module never transforms clinical data. It extracts
only the metadata needed to route and track the message. The full raw message
is always preserved exactly as received.

Supports: ADT^A01, ADT^A03, ADT^A08, ORU^R01, SIU^S12, VXU^V04
Requires: hl7apy (pip install hl7apy)
"""

import json
import logging
import re
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BronzeHL7Record:
    """
    Represents a single row destined for bronze.hl7_messages.
    All fields except raw_payload are extracted from the MSH/ZTN segments only.
    """
    message_id: str
    tenant_id: str
    sending_application: Optional[str]
    sending_facility: Optional[str]
    message_type: Optional[str]          # e.g., "ADT^A01"
    message_control_id: Optional[str]
    message_datetime: Optional[str]      # ISO 8601 string
    source_system: Optional[str]
    feed_type: Optional[str]             # ADT, ORU, SIU, VXU
    batch_id: Optional[str]
    raw_payload: str                     # Full original message — never modified
    received_ts: str                     # ISO 8601 string
    ingestion_version: str
    file_source: Optional[str]
    processing_status: str = "PENDING"
    processing_error: Optional[str] = None


@dataclass
class HL7ParseResult:
    """
    Result of parsing a single HL7 message.
    On failure, record is None and error contains the exception detail.
    The raw_payload is always preserved regardless of parse success.
    """
    success: bool
    record: Optional[BronzeHL7Record]
    error: Optional[str]
    raw_payload: str


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

INGESTION_VERSION = "1.0.0"

# HL7 v2 timestamp formats (most to least specific)
_HL7_TS_FORMATS = [
    (14, "%Y%m%d%H%M%S"),
    (12, "%Y%m%d%H%M"),
    (8,  "%Y%m%d"),
]

_FEED_TYPE_MAP = {
    "ADT": "ADT",
    "ORU": "ORU",
    "SIU": "SIU",
    "VXU": "VXU",
    "RDE": "RDE",
    "MDM": "MDM",
}


def parse_hl7_timestamp(raw_ts: str) -> Optional[str]:
    """
    Convert an HL7 v2 timestamp (yyyyMMddHHmmss) to ISO 8601.
    Returns None on parse failure — never raises.
    """
    if not raw_ts:
        return None
    ts_clean = raw_ts.split("+")[0].split("-")[0].strip()  # strip timezone offset
    for length, fmt in _HL7_TS_FORMATS:
        try:
            dt = datetime.strptime(ts_clean[:length], fmt)
            return dt.isoformat()
        except ValueError:
            continue
    logger.warning("Could not parse HL7 timestamp: %s", raw_ts)
    return None


def extract_msh_fields(raw_message: str) -> dict:
    """
    Extract MSH segment fields by position.
    MSH is always the first segment. Field separator is MSH.1 (typically |).
    Component separator is MSH.2[0] (typically ^).

    Returns a dict of field values indexed by MSH position (1-based).
    """
    lines = raw_message.strip().split("\r") if "\r" in raw_message else raw_message.strip().splitlines()
    msh_line = next((l for l in lines if l.startswith("MSH")), None)
    if not msh_line:
        raise ValueError("No MSH segment found in message")

    field_sep = msh_line[3]          # MSH.1
    fields = msh_line.split(field_sep)
    # MSH.1 is the field separator itself; fields[0]='MSH', fields[1]=encoding chars
    # fields[2] = MSH.3 (sending application), etc.
    return {
        "field_sep": field_sep,
        "msh3_sending_app": fields[2] if len(fields) > 2 else None,
        "msh4_sending_facility": fields[3] if len(fields) > 3 else None,
        "msh5_receiving_app": fields[4] if len(fields) > 4 else None,
        "msh6_receiving_facility": fields[5] if len(fields) > 5 else None,
        "msh7_datetime": fields[6] if len(fields) > 6 else None,
        "msh9_message_type": fields[8] if len(fields) > 8 else None,
        "msh10_control_id": fields[9] if len(fields) > 9 else None,
        "msh12_version": fields[11] if len(fields) > 11 else None,
    }


def extract_ztn_fields(raw_message: str, field_sep: str = "|") -> dict:
    """
    Extract the custom ZTN segment used to carry tenant and pipeline metadata.

    Expected format:
        ZTN|TENANT_ID=INTEGRIS_BAPTIST|SOURCE_SYSTEM=EPIC_2024|FEED_TYPE=ADT|...

    Returns a dict of key-value pairs from ZTN fields.
    Returns empty dict if ZTN is absent (tenant_id will fall back to MSH.4).
    """
    lines = raw_message.strip().split("\r") if "\r" in raw_message else raw_message.strip().splitlines()
    ztn_line = next((l for l in lines if l.startswith("ZTN")), None)
    if not ztn_line:
        return {}

    result = {}
    fields = ztn_line.split(field_sep)[1:]  # skip "ZTN"
    for field in fields:
        if "=" in field:
            key, _, value = field.partition("=")
            result[key.strip()] = value.strip()
    return result


def derive_feed_type(message_type: Optional[str]) -> Optional[str]:
    """
    Extract the top-level message type code (e.g., 'ADT' from 'ADT^A01').
    """
    if not message_type:
        return None
    top_level = message_type.split("^")[0].strip()
    return _FEED_TYPE_MAP.get(top_level, top_level)


def parse_hl7_message(
    raw_message: str,
    file_source: Optional[str] = None,
    default_tenant_id: Optional[str] = None,
) -> HL7ParseResult:
    """
    Parse a single HL7 v2 message into a BronzeHL7Record.

    Args:
        raw_message: The complete HL7 v2 message string, pipe-delimited.
        file_source: SFTP path, MQ topic, or other source identifier.
        default_tenant_id: Fallback tenant_id if ZTN segment is absent.
                           In production, set this from the connection config.

    Returns:
        HL7ParseResult with success=True and a populated record,
        or success=False with error detail. raw_payload is always set.
    """
    received_ts = datetime.now(timezone.utc).isoformat()
    message_id = str(uuid.uuid4())

    try:
        msh = extract_msh_fields(raw_message)
        ztn = extract_ztn_fields(raw_message, field_sep=msh["field_sep"])

        # Tenant resolution: ZTN > default_tenant_id > MSH.4
        tenant_id = (
            ztn.get("TENANT_ID")
            or default_tenant_id
            or msh.get("msh4_sending_facility")
            or "UNKNOWN"
        )

        message_type = msh.get("msh9_message_type")
        feed_type = ztn.get("FEED_TYPE") or derive_feed_type(message_type)

        record = BronzeHL7Record(
            message_id=message_id,
            tenant_id=tenant_id,
            sending_application=msh.get("msh3_sending_app"),
            sending_facility=msh.get("msh4_sending_facility"),
            message_type=message_type,
            message_control_id=msh.get("msh10_control_id"),
            message_datetime=parse_hl7_timestamp(msh.get("msh7_datetime") or ""),
            source_system=ztn.get("SOURCE_SYSTEM") or msh.get("msh3_sending_app"),
            feed_type=feed_type,
            batch_id=ztn.get("BATCH_ID"),
            raw_payload=raw_message,           # preserved exactly
            received_ts=received_ts,
            ingestion_version=INGESTION_VERSION,
            file_source=file_source,
            processing_status="PENDING",
        )

        return HL7ParseResult(success=True, record=record, error=None, raw_payload=raw_message)

    except Exception as exc:
        logger.exception("Failed to parse HL7 message (message_id=%s)", message_id)
        return HL7ParseResult(
            success=False,
            record=None,
            error=str(exc),
            raw_payload=raw_message,
        )


def parse_hl7_batch(
    messages: list[str],
    file_source: Optional[str] = None,
    default_tenant_id: Optional[str] = None,
) -> tuple[list[BronzeHL7Record], list[HL7ParseResult]]:
    """
    Parse a list of HL7 messages.

    Returns:
        (successes, failures) where successes is a list of BronzeHL7Record
        and failures is a list of HL7ParseResult with success=False.

    Failed messages are not dropped — they produce an error record in
    bronze.hl7_messages with processing_status='ERROR' so they can be
    triaged and reprocessed.
    """
    successes = []
    failures = []

    for raw in messages:
        result = parse_hl7_message(raw, file_source=file_source, default_tenant_id=default_tenant_id)
        if result.success:
            successes.append(result.record)
        else:
            # Build an error record so the failure is visible in Bronze
            error_record = BronzeHL7Record(
                message_id=str(uuid.uuid4()),
                tenant_id=default_tenant_id or "UNKNOWN",
                sending_application=None,
                sending_facility=None,
                message_type=None,
                message_control_id=None,
                message_datetime=None,
                source_system=None,
                feed_type=None,
                batch_id=None,
                raw_payload=raw,
                received_ts=datetime.now(timezone.utc).isoformat(),
                ingestion_version=INGESTION_VERSION,
                file_source=file_source,
                processing_status="ERROR",
                processing_error=result.error,
            )
            failures.append(result)
            successes.append(error_record)  # still lands in Bronze

    return successes, failures


def records_to_json(records: list[BronzeHL7Record]) -> str:
    """
    Serialize a list of BronzeHL7Record to a JSON array string.
    Useful for writing to a staging file or passing to a bulk insert.
    """
    return json.dumps([asdict(r) for r in records], default=str, indent=2)


# ---------------------------------------------------------------------------
# CLI demo (runs against synthetic data)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    sample_path = os.path.join(
        os.path.dirname(__file__), "..", "data", "synthetic", "hl7_adt_sample.txt"
    )

    if not os.path.exists(sample_path):
        print(f"Synthetic sample not found at {sample_path}")
        sys.exit(1)

    with open(sample_path, "r") as f:
        raw = f.read()

    result = parse_hl7_message(raw, file_source=sample_path)

    if result.success:
        print("Parse successful.")
        print(f"  tenant_id:       {result.record.tenant_id}")
        print(f"  message_type:    {result.record.message_type}")
        print(f"  feed_type:       {result.record.feed_type}")
        print(f"  message_id:      {result.record.message_id}")
        print(f"  control_id:      {result.record.message_control_id}")
        print(f"  message_ts:      {result.record.message_datetime}")
        print(f"  raw_payload len: {len(result.record.raw_payload)} chars")
    else:
        print(f"Parse failed: {result.error}")
        sys.exit(1)
