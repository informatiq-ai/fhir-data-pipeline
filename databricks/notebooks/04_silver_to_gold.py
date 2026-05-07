# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver → Gold Analytics
# MAGIC
# MAGIC **Purpose:** Read normalized Silver CDM tables, run Gold-layer analytics
# MAGIC (patient summary, HEDIS quality measures, ADT event feed, USCDI v3 export),
# MAGIC and write results to Gold Delta tables. All analytics logic delegates to
# MAGIC `transforms/silver_to_gold.py` — no analytics logic lives in this notebook.
# MAGIC
# MAGIC **Reads from:**
# MAGIC - `dev.fhir_silver.clinical_patients`
# MAGIC - `dev.fhir_silver.clinical_encounters`
# MAGIC - `dev.fhir_silver.clinical_observations`
# MAGIC - `dev.fhir_silver.clinical_conditions`
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_gold.analytics_patient_summary`
# MAGIC - `dev.fhir_gold.analytics_quality_measures`
# MAGIC - `dev.fhir_gold.analytics_adt_events`
# MAGIC - `dev.fhir_gold.export_uscdi_v3_patient`
# MAGIC
# MAGIC **Run order:** Notebook 04 of 04. Run after `03_bronze_to_silver.py`.

# COMMAND ----------

import sys
import uuid
import json
from datetime import datetime, date, timezone

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

SRC_CATALOG      = "dev"
SRC_SCHEMA       = "fhir_silver"
TGT_CATALOG      = "dev"
TGT_SCHEMA       = "fhir_gold"
NOTEBOOK_NAME    = "04_silver_to_gold"
PIPELINE_VERSION = "1.0.0"

TBL_CLINICAL_PATIENTS = f"{SRC_CATALOG}.{SRC_SCHEMA}.clinical_patients"
TBL_CLINICAL_ENC      = f"{SRC_CATALOG}.{SRC_SCHEMA}.clinical_encounters"
TBL_CLINICAL_OBS      = f"{SRC_CATALOG}.{SRC_SCHEMA}.clinical_observations"
TBL_CLINICAL_COND     = f"{SRC_CATALOG}.{SRC_SCHEMA}.clinical_conditions"

TBL_GOLD_SUMMARY  = f"{TGT_CATALOG}.{TGT_SCHEMA}.analytics_patient_summary"
TBL_GOLD_MEASURES = f"{TGT_CATALOG}.{TGT_SCHEMA}.analytics_quality_measures"
TBL_GOLD_ADT      = f"{TGT_CATALOG}.{TGT_SCHEMA}.analytics_adt_events"
TBL_GOLD_USCDI    = f"{TGT_CATALOG}.{TGT_SCHEMA}.export_uscdi_v3_patient"

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"source          : {SRC_CATALOG}.{SRC_SCHEMA}")
print(f"target          : {TGT_CATALOG}.{TGT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget: upstream pipeline_run_id
# MAGIC
# MAGIC Silver clinical tables (`clinical_patients`, `clinical_encounters`, etc.) do not
# MAGIC carry a `pipeline_run_id` column — they store normalized records cumulatively.
# MAGIC This widget is retained for orchestration compatibility and audit logging.
# MAGIC All Silver rows are read on every execution.

# COMMAND ----------

dbutils.widgets.text(
    "upstream_pipeline_run_id",
    "",
    "Pipeline Run ID from notebook 03 (audit reference — not used to filter Silver reads)",
)
upstream_run_id = dbutils.widgets.get("upstream_pipeline_run_id").strip()
print(f"upstream_pipeline_run_id : '{upstream_run_id}' (audit reference only)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve repo root and import transform modules

# COMMAND ----------

_nb_path = (
    dbutils.notebook.entry_point
    .getDbutils().notebook().getContext()
    .notebookPath().get()
)
REPO_ROOT = "/Workspace" + _nb_path.rsplit("/databricks/notebooks", 1)[0]
print(f"REPO_ROOT: {REPO_ROOT}")

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from transforms.silver_to_gold import (
    SilverPatient,
    SilverEncounter,
    SilverDiagnosis,
    SilverLabObservation,
    SilverADTEvent,
    build_patient_summary,
    calculate_cdc_hba1c_control,
    build_adt_event_feed,
    hash_umpi,
)

print("silver_to_gold imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Gold catalog, schema, and target tables (idempotent DDL)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {TGT_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TGT_CATALOG}.{TGT_SCHEMA}")

# ── analytics_patient_summary ─────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_GOLD_SUMMARY} (
        patient_key               STRING    NOT NULL COMMENT 'SHA-256(UMPI) — UMPI never exposed in Gold',
        charlson_index            INT                COMMENT 'Charlson Comorbidity Index score',
        elixhauser_index          INT                COMMENT 'Elixhauser index — scaffolded, not yet implemented',
        chronic_condition_flags   MAP<STRING, BOOLEAN> COMMENT 'e.g. {{CHF: true, CKD: false, DIABETES: true}}',
        pcp_npi                   STRING             COMMENT 'Attributed primary care provider NPI',
        pcp_attribution_method    STRING             COMMENT 'plurality | most_recent | manual',
        last_encounter_date       DATE,
        total_encounter_count     BIGINT,
        tenant_id                 STRING    NOT NULL,
        pipeline_run_id           STRING,
        generated_at              TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Patient-level risk summary. Charlson scoring. Rebuilt each pipeline run.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── analytics_quality_measures ────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_GOLD_MEASURES} (
        measure_id                STRING    NOT NULL,
        patient_key               STRING    NOT NULL,
        measure_name              STRING    NOT NULL COMMENT 'HEDIS_CDC_HBAC1_CONTROL | etc.',
        measure_code              STRING             COMMENT 'NCQA measure ID',
        in_denominator            BOOLEAN            COMMENT 'Patient meets denominator criteria',
        in_numerator              BOOLEAN            COMMENT 'Patient meets numerator criteria',
        excluded                  BOOLEAN            COMMENT 'Patient meets an exclusion criterion',
        hba1c_value               DOUBLE             COMMENT 'Most recent HbA1c result (measure-specific)',
        measurement_period_start  DATE,
        measurement_period_end    DATE,
        tenant_id                 STRING    NOT NULL,
        pipeline_run_id           STRING,
        generated_at              TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Quality measure results. HEDIS CDC HbA1c Control is the reference implementation.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── analytics_adt_events ──────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_GOLD_ADT} (
        event_id                    STRING    NOT NULL,
        patient_key                 STRING    NOT NULL,
        event_type                  STRING             COMMENT 'admit | discharge | transfer',
        event_subtype               STRING             COMMENT 'A01 | A02 | A03 | A08 | etc.',
        facility_id                 STRING,
        event_datetime              TIMESTAMP NOT NULL,
        readmission_30day           BOOLEAN            COMMENT 'True if readmitted within 30 days of prior discharge',
        prior_discharge_datetime    TIMESTAMP          COMMENT 'Reference discharge for readmission calc',
        days_since_prior_discharge  DOUBLE             COMMENT 'total_seconds() / 86400 — supports same-day',
        tenant_id                   STRING    NOT NULL,
        pipeline_run_id             STRING,
        generated_at                TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'ADT event feed with 30-day readmission flag. Uses total_seconds() for same-day accuracy.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# ── export_uscdi_v3_patient ───────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_GOLD_USCDI} (
        export_id         STRING    NOT NULL,
        patient_key       STRING    NOT NULL,
        uscdi_version     STRING             COMMENT 'v3',
        export_payload    STRING    NOT NULL COMMENT 'FHIR R4 JSON — USCDI v3 compliant',
        export_datetime   TIMESTAMP NOT NULL,
        qhin_ready        BOOLEAN            COMMENT 'True when all required USCDI elements present',
        missing_elements  ARRAY<STRING>      COMMENT 'USCDI elements absent from this record',
        tenant_id         STRING    NOT NULL,
        pipeline_run_id   STRING
    )
    USING DELTA
    COMMENT 'USCDI v3 export records. TEFCA / QHIN ready. Missing elements tracked for gap analysis.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print("Gold tables verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver tables
# MAGIC
# MAGIC Silver clinical tables are read in full — they do not carry `pipeline_run_id`.
# MAGIC The upstream_pipeline_run_id widget is retained for orchestration compatibility only.

# COMMAND ----------

started_at = datetime.now(timezone.utc)

patient_rows = spark.sql(f"""
    SELECT umpi, first_name, last_name, date_of_birth, gender, state, zip, tenant_id
    FROM {TBL_CLINICAL_PATIENTS}
""").collect()

enc_rows = spark.sql(f"""
    SELECT encounter_id, umpi, tenant_id, encounter_class, status,
           admit_datetime, discharge_datetime, facility_id, attending_provider_npi
    FROM {TBL_CLINICAL_ENC}
""").collect()

obs_rows = spark.sql(f"""
    SELECT observation_id, umpi, tenant_id, encounter_id, loinc_code, loinc_display,
           value_quantity, value_unit, interpretation, observation_datetime, status
    FROM {TBL_CLINICAL_OBS}
""").collect()

cond_rows = spark.sql(f"""
    SELECT condition_id, umpi, tenant_id, encounter_id, icd10_code, icd10_display,
           condition_category, clinical_status, onset_datetime
    FROM {TBL_CLINICAL_COND}
""").collect()

print(f"Silver rows loaded:")
print(f"  clinical_patients     : {len(patient_rows)}")
print(f"  clinical_encounters   : {len(enc_rows)}")
print(f"  clinical_observations : {len(obs_rows)}")
print(f"  clinical_conditions   : {len(cond_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Silver dataclass instances

# COMMAND ----------

def _parse_date(s):
    """Accept date object (from Spark collect) or ISO string."""
    if not s:
        return None
    if isinstance(s, date) and not isinstance(s, datetime):
        return s
    try:
        return date.fromisoformat(str(s)[:10])
    except (ValueError, TypeError):
        return None


def _parse_datetime(s):
    """Accept datetime object (from Spark collect) or ISO string. Always returns naive."""
    if not s:
        return None
    if isinstance(s, datetime):
        return s.replace(tzinfo=None) if s.tzinfo else s
    try:
        raw = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    except (ValueError, TypeError):
        return None


def _parse_hba1c(evidence_value):
    """Parse HbA1c numeric value from evidence string (e.g. '7.2%' → 7.2)."""
    if not evidence_value:
        return None
    try:
        return float(str(evidence_value).strip().rstrip("%"))
    except (ValueError, TypeError):
        return None


# Build SilverPatient list — one per umpi (first row per umpi wins)
patient_by_umpi = {}
for r in patient_rows:
    if r["umpi"] not in patient_by_umpi:
        patient_by_umpi[r["umpi"]] = r

silver_patients = [
    SilverPatient(
        umpi=r["umpi"],
        tenant_id=r["tenant_id"],
        family_name=r["last_name"],
        given_name=r["first_name"],
        birth_date=_parse_date(r["date_of_birth"]),
        gender=r["gender"],
        state=r["state"],
        postal_code=r["zip"],
    )
    for r in patient_by_umpi.values()
]

# Index encounters by umpi; facility_id stored in facility_name for Gold ADT pass-through
enc_by_umpi = {}
for r in enc_rows:
    enc = SilverEncounter(
        encounter_id=r["encounter_id"],
        tenant_id=r["tenant_id"],
        umpi=r["umpi"],
        encounter_class=r["encounter_class"],
        period_start=_parse_datetime(r["admit_datetime"]),
        period_end=_parse_datetime(r["discharge_datetime"]),
        facility_name=r["facility_id"],      # facility_id passed through this slot
        facility_npi=r["facility_id"],
        attending_provider_npi=r["attending_provider_npi"],
        attending_provider_name=None,
        encounter_status=r["status"],
        admit_source_code=None,
        discharge_disposition_code=None,
    )
    enc_by_umpi.setdefault(r["umpi"], []).append(enc)

# Index observations by umpi
obs_by_umpi = {}
for r in obs_rows:
    obs = SilverLabObservation(
        observation_id=r["observation_id"],
        tenant_id=r["tenant_id"],
        umpi=r["umpi"],
        encounter_id=r["encounter_id"],
        loinc_code=r["loinc_code"],
        loinc_display=r["loinc_display"],
        value_quantity=float(r["value_quantity"]) if r["value_quantity"] is not None else None,
        value_unit=r["value_unit"],
        interpretation_code=r["interpretation"],
        effective_datetime=_parse_datetime(r["observation_datetime"]),
        observation_status=r["status"],
    )
    obs_by_umpi.setdefault(r["umpi"], []).append(obs)

# Index conditions by umpi; map condition_category → diagnosis_rank (for Charlson scoring)
CATEGORY_RANK = {"primary": 1, "secondary": 2}

dx_by_umpi = {}
for r in cond_rows:
    dx = SilverDiagnosis(
        diagnosis_id=r["condition_id"],
        tenant_id=r["tenant_id"],
        umpi=r["umpi"],
        encounter_id=r["encounter_id"],
        icd10_code=r["icd10_code"],
        icd10_display=r["icd10_display"],
        diagnosis_rank=CATEGORY_RANK.get(r["condition_category"]) if r["condition_category"] else None,
        clinical_status=r["clinical_status"],
        onset_datetime=_parse_datetime(r["onset_datetime"]),
    )
    dx_by_umpi.setdefault(r["umpi"], []).append(dx)

print(f"Silver instances built for {len(silver_patients)} patient(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold records per patient
# MAGIC
# MAGIC For each patient:
# MAGIC 1. `build_patient_summary` → `analytics_patient_summary` row
# MAGIC 2. `calculate_cdc_hba1c_control` → `analytics_quality_measures` row (if diabetic)
# MAGIC 3. ADT events from admit/discharge datetimes → `analytics_adt_events` rows
# MAGIC 4. Minimal FHIR Patient JSON → `export_uscdi_v3_patient` row

# COMMAND ----------

ADT_TYPE_DISPLAY = {"A01": "admit", "A02": "transfer", "A03": "discharge"}

as_of_today     = date.today()
measurement_year = as_of_today.year
now_ts          = datetime.now(timezone.utc).replace(tzinfo=None)

summary_rows = []
measure_rows = []
adt_rows     = []
uscdi_rows   = []

for patient in silver_patients:
    umpi         = patient.umpi
    encounters   = enc_by_umpi.get(umpi, [])
    observations = obs_by_umpi.get(umpi, [])
    diagnoses    = dx_by_umpi.get(umpi, [])
    patient_key  = hash_umpi(umpi)

    # ── Patient Summary ───────────────────────────────────────────────────────
    summary = build_patient_summary(
        patient=patient,
        encounters=encounters,
        diagnoses=diagnoses,
        as_of_date=as_of_today,
    )

    summary_rows.append({
        "patient_key":             summary.patient_key,
        "charlson_index":          summary.charlson_index,
        "elixhauser_index":        summary.elixhauser_index,
        "chronic_condition_flags": {
            "DIABETES":    summary.flag_diabetes,
            "HYPERTENSION": summary.flag_hypertension,
            "CHF":         summary.flag_heart_failure,
            "CKD":         summary.flag_ckd,
            "COPD":        summary.flag_copd,
            "DEPRESSION":  summary.flag_depression,
        },
        "pcp_npi":                summary.attributed_pcp_npi,
        "pcp_attribution_method": summary.attribution_method,
        "last_encounter_date":    summary.last_encounter_date,
        "total_encounter_count":  summary.total_encounters_12m,
        "tenant_id":              patient.tenant_id,
        "pipeline_run_id":        pipeline_run_id,
        "generated_at":           now_ts,
    })

    # ── CDC HbA1c Control Quality Measure ─────────────────────────────────────
    measure = calculate_cdc_hba1c_control(
        patient=patient,
        diagnoses=diagnoses,
        observations=observations,
        measurement_year=measurement_year,
    )

    if measure is not None:
        measure_rows.append({
            "measure_id":               measure.measure_id,
            "patient_key":              measure.patient_key,
            "measure_name":             measure.measure_name,
            "measure_code":             measure.measure_code,
            "in_denominator":           measure.denominator,
            "in_numerator":             measure.numerator,
            "excluded":                 measure.exclusion,
            "hba1c_value":              _parse_hba1c(measure.evidence_value),
            "measurement_period_start": date(measure.measurement_year, 1, 1),
            "measurement_period_end":   date(measure.measurement_year, 12, 31),
            "tenant_id":                measure.tenant_id,
            "pipeline_run_id":          pipeline_run_id,
            "generated_at":             now_ts,
        })

    # ── ADT Event Feed (derived from encounter admit/discharge datetimes) ──────
    adt_events = []
    for enc in encounters:
        if enc.period_start:
            adt_events.append(SilverADTEvent(
                adt_event_id=str(uuid.uuid4()),
                tenant_id=enc.tenant_id,
                umpi=umpi,
                encounter_id=enc.encounter_id,
                event_type="A01",
                event_datetime=enc.period_start,
                facility_name=enc.facility_name,  # facility_id stored here
                facility_npi=enc.facility_npi,
                current_location=None,
            ))
        if enc.period_end:
            adt_events.append(SilverADTEvent(
                adt_event_id=str(uuid.uuid4()),
                tenant_id=enc.tenant_id,
                umpi=umpi,
                encounter_id=enc.encounter_id,
                event_type="A03",
                event_datetime=enc.period_end,
                facility_name=enc.facility_name,
                facility_npi=enc.facility_npi,
                current_location=None,
            ))

    gold_adt = build_adt_event_feed(
        patient=patient,
        adt_events=adt_events,
        diagnoses=diagnoses,
        attributed_pcp_npi=summary.attributed_pcp_npi,
        attributed_pcp_name=summary.attributed_pcp_name,
    )

    for ev in gold_adt:
        hl7_subtype      = ev.event_type
        event_type_human = ADT_TYPE_DISPLAY.get(hl7_subtype, "other")
        event_dt         = ev.event_datetime.replace(tzinfo=None) if ev.event_datetime and ev.event_datetime.tzinfo else ev.event_datetime

        prior_dt   = None
        days_since = None
        if ev.prior_discharge_date:
            d        = ev.prior_discharge_date
            prior_dt = datetime(d.year, d.month, d.day)
            if event_dt:
                days_since = (event_dt - prior_dt).total_seconds() / 86400

        adt_rows.append({
            "event_id":                   ev.event_id,
            "patient_key":                ev.patient_key,
            "event_type":                 event_type_human,
            "event_subtype":              hl7_subtype,
            "facility_id":                ev.facility_name,   # facility_id passed through facility_name
            "event_datetime":             event_dt,
            "readmission_30day":          ev.is_readmission_30d,
            "prior_discharge_datetime":   prior_dt,
            "days_since_prior_discharge": days_since,
            "tenant_id":                  ev.tenant_id,
            "pipeline_run_id":            pipeline_run_id,
            "generated_at":               now_ts,
        })

    # ── USCDI v3 Export ────────────────────────────────────────────────────────
    missing = []
    if not patient.given_name:
        missing.append("name.given")
    if not patient.family_name:
        missing.append("name.family")
    if not patient.birth_date:
        missing.append("birthDate")
    if not patient.gender:
        missing.append("gender")

    fhir_patient_json = {
        "resourceType": "Patient",
        "id": umpi,
        "name": [{
            "family": patient.family_name,
            "given": [patient.given_name] if patient.given_name else [],
        }],
        "birthDate": patient.birth_date.isoformat() if patient.birth_date else None,
        "gender": patient.gender,
    }

    uscdi_rows.append({
        "export_id":        str(uuid.uuid4()),
        "patient_key":      patient_key,
        "uscdi_version":    "v3",
        "export_payload":   json.dumps(fhir_patient_json),
        "export_datetime":  now_ts,
        "qhin_ready":       len(missing) == 0,
        "missing_elements": missing,
        "tenant_id":        patient.tenant_id,
        "pipeline_run_id":  pipeline_run_id,
    })

print(f"Gold records built:")
print(f"  analytics_patient_summary  : {len(summary_rows)}")
print(f"  analytics_quality_measures : {len(measure_rows)}")
print(f"  analytics_adt_events       : {len(adt_rows)}")
print(f"  export_uscdi_v3_patient    : {len(uscdi_rows)}")

for s in summary_rows:
    print(f"\n  Patient {s['patient_key'][:16]}...")
    print(f"    charlson_index         : {s['charlson_index']}")
    print(f"    chronic_condition_flags: {s['chronic_condition_flags']}")
    print(f"    total_encounter_count  : {s['total_encounter_count']}")

for m in measure_rows:
    print(f"\n  Measure {m['measure_name']}")
    print(f"    in_denominator : {m['in_denominator']}")
    print(f"    in_numerator   : {m['in_numerator']}")
    print(f"    hba1c_value    : {m['hba1c_value']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold tables (PySpark)

# COMMAND ----------

from pyspark.sql.types import (
    ArrayType, BooleanType, DateType, DoubleType, IntegerType,
    LongType, MapType, StringType, StructField, StructType, TimestampType,
)

# Module-level constants — consumed by tests/test_contracts.py for DDL alignment checks.
# Names match databricks/fhir_pipeline_ddl.sql table names.  Do not rename.

# ── analytics_patient_summary (11 cols — DDL order) ───────────────────────────
ANALYTICS_PATIENT_SUMMARY_SCHEMA = StructType([
    StructField("patient_key",             StringType(),                         False),
    StructField("charlson_index",          IntegerType(),                        True),
    StructField("elixhauser_index",        IntegerType(),                        True),
    StructField("chronic_condition_flags", MapType(StringType(), BooleanType()), True),
    StructField("pcp_npi",                 StringType(),                         True),
    StructField("pcp_attribution_method",  StringType(),                         True),
    StructField("last_encounter_date",     DateType(),                           True),
    StructField("total_encounter_count",   LongType(),                           True),
    StructField("tenant_id",              StringType(),                         False),
    StructField("pipeline_run_id",        StringType(),                         True),
    StructField("generated_at",           TimestampType(),                      False),
])

# ── analytics_quality_measures (13 cols — DDL order) ─────────────────────────
ANALYTICS_QUALITY_MEASURES_SCHEMA = StructType([
    StructField("measure_id",               StringType(),    False),
    StructField("patient_key",             StringType(),    False),
    StructField("measure_name",            StringType(),    False),
    StructField("measure_code",            StringType(),    True),
    StructField("in_denominator",          BooleanType(),   True),
    StructField("in_numerator",            BooleanType(),   True),
    StructField("excluded",                BooleanType(),   True),
    StructField("hba1c_value",             DoubleType(),    True),
    StructField("measurement_period_start",DateType(),      True),
    StructField("measurement_period_end",  DateType(),      True),
    StructField("tenant_id",              StringType(),    False),
    StructField("pipeline_run_id",        StringType(),    True),
    StructField("generated_at",           TimestampType(), False),
])

# ── analytics_adt_events (12 cols — DDL order) ────────────────────────────────
ANALYTICS_ADT_EVENTS_SCHEMA = StructType([
    StructField("event_id",                   StringType(),    False),
    StructField("patient_key",               StringType(),    False),
    StructField("event_type",                StringType(),    True),
    StructField("event_subtype",             StringType(),    True),
    StructField("facility_id",               StringType(),    True),
    StructField("event_datetime",            TimestampType(), False),
    StructField("readmission_30day",         BooleanType(),   True),
    StructField("prior_discharge_datetime",  TimestampType(), True),
    StructField("days_since_prior_discharge",DoubleType(),    True),
    StructField("tenant_id",                StringType(),    False),
    StructField("pipeline_run_id",          StringType(),    True),
    StructField("generated_at",             TimestampType(), False),
])

# ── export_uscdi_v3_patient (9 cols — DDL order) ──────────────────────────────
EXPORT_USCDI_V3_PATIENT_SCHEMA = StructType([
    StructField("export_id",        StringType(),            False),
    StructField("patient_key",      StringType(),            False),
    StructField("uscdi_version",    StringType(),            True),
    StructField("export_payload",   StringType(),            False),
    StructField("export_datetime",  TimestampType(),         False),
    StructField("qhin_ready",       BooleanType(),           True),
    StructField("missing_elements", ArrayType(StringType()), True),
    StructField("tenant_id",        StringType(),            False),
    StructField("pipeline_run_id",  StringType(),            True),
])

if summary_rows:
    summary_df = spark.createDataFrame(summary_rows, schema=ANALYTICS_PATIENT_SUMMARY_SCHEMA)
    summary_df.write.insertInto(TBL_GOLD_SUMMARY)
    print(f"Wrote {summary_df.count()} row(s) to {TBL_GOLD_SUMMARY}")
else:
    print(f"No analytics_patient_summary rows to write")

if measure_rows:
    measures_df = spark.createDataFrame(measure_rows, schema=ANALYTICS_QUALITY_MEASURES_SCHEMA)
    measures_df.write.insertInto(TBL_GOLD_MEASURES)
    print(f"Wrote {measures_df.count()} row(s) to {TBL_GOLD_MEASURES}")
else:
    print(f"No analytics_quality_measures rows to write")

if adt_rows:
    adt_df = spark.createDataFrame(adt_rows, schema=ANALYTICS_ADT_EVENTS_SCHEMA)
    adt_df.write.insertInto(TBL_GOLD_ADT)
    print(f"Wrote {adt_df.count()} row(s) to {TBL_GOLD_ADT}")
else:
    print(f"No analytics_adt_events rows to write")

if uscdi_rows:
    uscdi_df = spark.createDataFrame(uscdi_rows, schema=EXPORT_USCDI_V3_PATIENT_SCHEMA)
    uscdi_df.write.insertInto(TBL_GOLD_USCDI)
    print(f"Wrote {uscdi_df.count()} row(s) to {TBL_GOLD_USCDI}")
else:
    print(f"No export_uscdi_v3_patient rows to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — preview Gold output

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            patient_key,
            charlson_index,
            chronic_condition_flags,
            pcp_npi,
            last_encounter_date,
            total_encounter_count,
            tenant_id,
            pipeline_run_id
        FROM {TBL_GOLD_SUMMARY}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        ORDER BY patient_key
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            patient_key,
            measure_name,
            in_denominator,
            in_numerator,
            hba1c_value,
            measurement_period_start,
            measurement_period_end,
            pipeline_run_id
        FROM {TBL_GOLD_MEASURES}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        ORDER BY measure_name
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            patient_key,
            event_type,
            event_subtype,
            facility_id,
            event_datetime,
            readmission_30day,
            days_since_prior_discharge,
            pipeline_run_id
        FROM {TBL_GOLD_ADT}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        ORDER BY event_datetime
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            patient_key,
            uscdi_version,
            qhin_ready,
            missing_elements,
            export_datetime,
            pipeline_run_id
        FROM {TBL_GOLD_USCDI}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        ORDER BY patient_key
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline exit

# COMMAND ----------

completed_at = datetime.now(timezone.utc)
duration_s   = (completed_at - started_at).total_seconds()

print(f"Notebook completed  : {NOTEBOOK_NAME}")
print(f"pipeline_run_id     : {pipeline_run_id}")
print(f"  summary rows      : {len(summary_rows)}")
print(f"  measure rows      : {len(measure_rows)}")
print(f"  adt rows          : {len(adt_rows)}")
print(f"  uscdi rows        : {len(uscdi_rows)}")
print(f"  duration          : {duration_s:.1f}s")

dbutils.notebook.exit(pipeline_run_id)
