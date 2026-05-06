# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Silver → Gold Analytics
# MAGIC
# MAGIC **Purpose:** Read normalized Silver CDM tables, run Gold-layer analytics
# MAGIC (patient summary, HEDIS quality measures, ADT event feed), and write
# MAGIC results to Gold Delta tables. All transforms delegate to
# MAGIC `transforms/silver_to_gold.py` — no analytics logic lives in this notebook.
# MAGIC
# MAGIC **Reads from:**
# MAGIC - `dev.fhir_silver.master_patient_index`
# MAGIC - `dev.fhir_silver.encounters`
# MAGIC - `dev.fhir_silver.lab_observations`
# MAGIC - `dev.fhir_silver.diagnoses`
# MAGIC
# MAGIC **Writes to:**
# MAGIC - `dev.fhir_gold.patient_summary`
# MAGIC - `dev.fhir_gold.quality_measures`
# MAGIC - `dev.fhir_gold.adt_event_feed`
# MAGIC
# MAGIC **Run order:** Notebook 04 of 05. Run after `03_bronze_to_silver.py`,
# MAGIC before `00_run_pipeline.py` (orchestrator).

# COMMAND ----------

import sys
import uuid
from datetime import datetime, date, timezone

# ── pipeline_run_id: generated once per notebook execution ────────────────────
pipeline_run_id = str(uuid.uuid4())

SRC_CATALOG      = "dev"
SRC_SCHEMA       = "fhir_silver"
TGT_CATALOG      = "dev"
TGT_SCHEMA       = "fhir_gold"
NOTEBOOK_NAME    = "04_silver_to_gold"
PIPELINE_VERSION = "1.0.0"

TBL_MPI        = f"{SRC_CATALOG}.{SRC_SCHEMA}.master_patient_index"
TBL_ENCOUNTERS = f"{SRC_CATALOG}.{SRC_SCHEMA}.encounters"
TBL_LAB_OBS    = f"{SRC_CATALOG}.{SRC_SCHEMA}.lab_observations"
TBL_DIAGNOSES  = f"{SRC_CATALOG}.{SRC_SCHEMA}.diagnoses"

TBL_GOLD_SUMMARY  = f"{TGT_CATALOG}.{TGT_SCHEMA}.patient_summary"
TBL_GOLD_MEASURES = f"{TGT_CATALOG}.{TGT_SCHEMA}.quality_measures"
TBL_GOLD_ADT      = f"{TGT_CATALOG}.{TGT_SCHEMA}.adt_event_feed"
TBL_GOLD_AUDIT    = f"{TGT_CATALOG}.{TGT_SCHEMA}.audit_gold_log"

print(f"pipeline_run_id : {pipeline_run_id}")
print(f"source          : {SRC_CATALOG}.{SRC_SCHEMA}")
print(f"target          : {TGT_CATALOG}.{TGT_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget: upstream pipeline_run_id
# MAGIC
# MAGIC When run from the orchestrator (`00_run_pipeline.py`), this widget receives
# MAGIC the `pipeline_run_id` from notebook 03 so only Silver rows from that run
# MAGIC are promoted to Gold. Leave blank to process all Silver rows.

# COMMAND ----------

dbutils.widgets.text(
    "upstream_pipeline_run_id",
    "",
    "Pipeline Run ID from notebook 03 (blank = all Silver rows)",
)
upstream_run_id = dbutils.widgets.get("upstream_pipeline_run_id").strip()

if upstream_run_id:
    silver_filter = f"pipeline_run_id = '{upstream_run_id}'"
    print(f"Filtering Silver by pipeline_run_id: {upstream_run_id}")
else:
    silver_filter = "1=1"
    print("No upstream run ID — processing all Silver rows")

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

# ── patient_summary ───────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_GOLD_SUMMARY} (
        tenant_id                   STRING      NOT NULL,
        patient_key                 STRING      NOT NULL COMMENT 'SHA-256 of UMPI — not the raw UMPI',
        full_name                   STRING,
        birth_date                  STRING      COMMENT 'ISO 8601 date string',
        age_years                   INTEGER,
        gender                      STRING,
        state                       STRING,
        postal_code                 STRING,
        attributed_pcp_npi          STRING,
        attributed_pcp_name         STRING,
        attribution_method          STRING      COMMENT 'ENCOUNTER_FREQUENCY | CLAIMS | MANUAL',
        charlson_index              INTEGER,
        elixhauser_index            INTEGER,
        risk_tier                   STRING      COMMENT 'LOW | MODERATE | HIGH | VERY_HIGH',
        risk_score_updated_ts       STRING,
        total_encounters_12m        INTEGER,
        total_ed_visits_12m         INTEGER,
        total_inpatient_days_12m    INTEGER,
        last_encounter_date         STRING      COMMENT 'ISO 8601 date string',
        flag_diabetes               BOOLEAN,
        flag_hypertension           BOOLEAN,
        flag_heart_failure          BOOLEAN,
        flag_ckd                    BOOLEAN,
        flag_copd                   BOOLEAN,
        flag_depression             BOOLEAN,
        as_of_date                  STRING      NOT NULL COMMENT 'ISO 8601 date string',
        refreshed_ts                STRING      NOT NULL,
        pipeline_run_id             STRING      NOT NULL
    )
    USING DELTA
    COMMENT 'Gold patient summary. One row per patient per as_of_date. RLS enforced at query time.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

# ── quality_measures ──────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_GOLD_MEASURES} (
        measure_id              STRING      NOT NULL COMMENT 'UUID',
        tenant_id               STRING      NOT NULL,
        patient_key             STRING      NOT NULL,
        measure_code            STRING      NOT NULL COMMENT 'e.g. CDC_HBA1C_CONTROL_LESS_8',
        measure_name            STRING,
        measure_steward         STRING      COMMENT 'NCQA | CMS | Joint Commission',
        measurement_year        INTEGER     NOT NULL,
        numerator               BOOLEAN     COMMENT 'TRUE = met; FALSE = not met; NULL = no evidence',
        denominator             BOOLEAN     NOT NULL,
        exclusion               BOOLEAN,
        exclusion_reason        STRING,
        evidence_encounter_id   STRING,
        evidence_observation_id STRING,
        evidence_date           STRING      COMMENT 'ISO 8601 date string',
        evidence_value          STRING      COMMENT 'e.g. 7.2% for HbA1c',
        calculated_ts           STRING      NOT NULL,
        as_of_date              STRING      NOT NULL,
        pipeline_run_id         STRING      NOT NULL
    )
    USING DELTA
    COMMENT 'Gold HEDIS/CMS quality measure results. One row per patient per measure per year.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

# ── adt_event_feed ────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_GOLD_ADT} (
        event_id                    STRING      NOT NULL COMMENT 'UUID',
        tenant_id                   STRING      NOT NULL,
        patient_key                 STRING      NOT NULL,
        event_type                  STRING      NOT NULL COMMENT 'A01 | A02 | A03 | A08 …',
        event_type_display          STRING,
        event_datetime              STRING      NOT NULL COMMENT 'ISO 8601 datetime',
        facility_name               STRING,
        current_location            STRING,
        primary_diagnosis_icd10     STRING,
        primary_diagnosis_display   STRING,
        attributed_pcp_npi          STRING,
        attributed_pcp_name         STRING,
        is_readmission_30d          BOOLEAN     NOT NULL,
        prior_discharge_date        STRING      COMMENT 'ISO 8601 date; NULL if not a readmission',
        loaded_ts                   STRING      NOT NULL,
        pipeline_run_id             STRING      NOT NULL
    )
    USING DELTA
    COMMENT 'Gold ADT event feed for care coordination. Includes 30-day readmission flag.'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )
""")

# ── audit_gold_log ────────────────────────────────────────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TBL_GOLD_AUDIT} (
        log_id                  STRING      NOT NULL,
        pipeline_run_id         STRING      NOT NULL,
        notebook_name           STRING      NOT NULL,
        started_at              TIMESTAMP   NOT NULL,
        completed_at            TIMESTAMP,
        patients_processed      LONG,
        summaries_written       LONG,
        measures_written        LONG,
        adt_events_written      LONG,
        status                  STRING      NOT NULL COMMENT 'COMPLETED | PARTIAL | FAILED',
        error_detail            STRING,
        pipeline_version        STRING,
        created_ts              TIMESTAMP   NOT NULL
    )
    USING DELTA
    COMMENT 'Audit log for Gold layer notebook runs.'
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

print("Gold tables verified")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Silver tables
# MAGIC
# MAGIC Four Silver tables are loaded as collected Python lists for in-memory
# MAGIC processing by the `silver_to_gold` module. The Silver layer is small in
# MAGIC this reference implementation. In production, partition-pruning and
# MAGIC incremental windowing (via the upstream_pipeline_run_id widget) keeps
# MAGIC each batch tractable.

# COMMAND ----------

started_at = datetime.now(timezone.utc)

mpi_rows = spark.sql(f"""
    SELECT umpi, tenant_id, family_name, given_name, birth_date,
           gender, postal_code
    FROM {TBL_MPI}
    WHERE {silver_filter}
""").collect()

enc_rows = spark.sql(f"""
    SELECT encounter_id, tenant_id, umpi, encounter_class,
           period_start, period_end, facility_name, encounter_status
    FROM {TBL_ENCOUNTERS}
    WHERE {silver_filter}
""").collect()

obs_rows = spark.sql(f"""
    SELECT observation_id, tenant_id, umpi, encounter_id,
           loinc_code, loinc_display, value_quantity, value_unit,
           interpretation_code, effective_datetime, observation_status
    FROM {TBL_LAB_OBS}
    WHERE {silver_filter}
""").collect()

dx_rows = spark.sql(f"""
    SELECT diagnosis_id, tenant_id, umpi, encounter_id,
           icd10_code, icd10_display, diagnosis_rank,
           clinical_status, onset_datetime
    FROM {TBL_DIAGNOSES}
    WHERE {silver_filter}
""").collect()

print(f"Silver rows loaded:")
print(f"  master_patient_index : {len(mpi_rows)}")
print(f"  encounters           : {len(enc_rows)}")
print(f"  lab_observations     : {len(obs_rows)}")
print(f"  diagnoses            : {len(dx_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Silver dataclass instances

# COMMAND ----------

def _parse_date(s):
    if not s:
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except (ValueError, TypeError):
        return None


def _parse_datetime(s):
    if not s:
        return None
    try:
        raw = str(s).replace("Z", "+00:00")
        return datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return None


# Build SilverPatient list
silver_patients = [
    SilverPatient(
        umpi=r["umpi"],
        tenant_id=r["tenant_id"],
        family_name=r["family_name"],
        given_name=r["given_name"],
        birth_date=_parse_date(r["birth_date"]),
        gender=r["gender"],
        state=None,          # not captured in this Bronze→Silver pass
        postal_code=r["postal_code"],
    )
    for r in mpi_rows
]

# Index Silver encounters by umpi for per-patient lookup
enc_by_umpi: dict[str, list[SilverEncounter]] = {}
for r in enc_rows:
    enc = SilverEncounter(
        encounter_id=r["encounter_id"],
        tenant_id=r["tenant_id"],
        umpi=r["umpi"],
        encounter_class=r["encounter_class"],
        period_start=_parse_datetime(r["period_start"]),
        period_end=_parse_datetime(r["period_end"]),
        facility_name=r["facility_name"],
        facility_npi=None,
        attending_provider_npi=None,
        attending_provider_name=None,
        encounter_status=r["encounter_status"],
        admit_source_code=None,
        discharge_disposition_code=None,
    )
    enc_by_umpi.setdefault(r["umpi"], []).append(enc)

# Index Silver observations by umpi
obs_by_umpi: dict[str, list[SilverLabObservation]] = {}
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
        interpretation_code=r["interpretation_code"],
        effective_datetime=_parse_datetime(r["effective_datetime"]),
        observation_status=r["observation_status"],
    )
    obs_by_umpi.setdefault(r["umpi"], []).append(obs)

# Index Silver diagnoses by umpi
dx_by_umpi: dict[str, list[SilverDiagnosis]] = {}
for r in dx_rows:
    dx = SilverDiagnosis(
        diagnosis_id=r["diagnosis_id"],
        tenant_id=r["tenant_id"],
        umpi=r["umpi"],
        encounter_id=r["encounter_id"],
        icd10_code=r["icd10_code"],
        icd10_display=r["icd10_display"],
        diagnosis_rank=int(r["diagnosis_rank"]) if r["diagnosis_rank"] is not None else None,
        clinical_status=r["clinical_status"],
        onset_datetime=_parse_datetime(r["onset_datetime"]),
    )
    dx_by_umpi.setdefault(r["umpi"], []).append(dx)

print(f"Silver instances built for {len(silver_patients)} patient(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Gold records per patient
# MAGIC
# MAGIC For each patient in the Silver MPI:
# MAGIC 1. `build_patient_summary` → `patient_summary` row
# MAGIC 2. `calculate_cdc_hba1c_control` → `quality_measures` row (if in denominator)
# MAGIC 3. ADT events reconstructed from encounters → `adt_event_feed` rows
# MAGIC
# MAGIC The ADT events in this reference implementation are derived from encounters
# MAGIC (Admit = period_start, Discharge = period_end) since the FHIR source does
# MAGIC not include MessageHeader ADT events. In production, ADT events come from
# MAGIC the Bronze HL7 v2 `adt_events` Silver table.

# COMMAND ----------

as_of_today = date.today()
measurement_year = as_of_today.year

summary_rows  = []
measure_rows  = []
adt_rows      = []

for patient in silver_patients:
    umpi = patient.umpi
    encounters = enc_by_umpi.get(umpi, [])
    observations = obs_by_umpi.get(umpi, [])
    diagnoses = dx_by_umpi.get(umpi, [])

    # ── Patient Summary ───────────────────────────────────────────────────────
    summary = build_patient_summary(
        patient=patient,
        encounters=encounters,
        diagnoses=diagnoses,
        as_of_date=as_of_today,
    )

    summary_rows.append({
        "tenant_id":                patient.tenant_id,
        "patient_key":              summary.patient_key,
        "full_name":                summary.full_name,
        "birth_date":               summary.birth_date.isoformat() if summary.birth_date else None,
        "age_years":                summary.age_years,
        "gender":                   summary.gender,
        "state":                    summary.state,
        "postal_code":              summary.postal_code,
        "attributed_pcp_npi":       summary.attributed_pcp_npi,
        "attributed_pcp_name":      summary.attributed_pcp_name,
        "attribution_method":       summary.attribution_method,
        "charlson_index":           summary.charlson_index,
        "elixhauser_index":         summary.elixhauser_index,
        "risk_tier":                summary.risk_tier,
        "risk_score_updated_ts":    summary.risk_score_updated_ts,
        "total_encounters_12m":     summary.total_encounters_12m,
        "total_ed_visits_12m":      summary.total_ed_visits_12m,
        "total_inpatient_days_12m": summary.total_inpatient_days_12m,
        "last_encounter_date":      summary.last_encounter_date.isoformat() if summary.last_encounter_date else None,
        "flag_diabetes":            summary.flag_diabetes,
        "flag_hypertension":        summary.flag_hypertension,
        "flag_heart_failure":       summary.flag_heart_failure,
        "flag_ckd":                 summary.flag_ckd,
        "flag_copd":                summary.flag_copd,
        "flag_depression":          summary.flag_depression,
        "as_of_date":               as_of_today.isoformat(),
        "refreshed_ts":             summary.refreshed_ts,
        "pipeline_run_id":          pipeline_run_id,
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
            "tenant_id":                measure.tenant_id,
            "patient_key":              measure.patient_key,
            "measure_code":             measure.measure_code,
            "measure_name":             measure.measure_name,
            "measure_steward":          measure.measure_steward,
            "measurement_year":         measure.measurement_year,
            "numerator":                measure.numerator,
            "denominator":              measure.denominator,
            "exclusion":                measure.exclusion,
            "exclusion_reason":         measure.exclusion_reason,
            "evidence_encounter_id":    measure.evidence_encounter_id,
            "evidence_observation_id":  measure.evidence_observation_id,
            "evidence_date":            measure.evidence_date.isoformat() if measure.evidence_date else None,
            "evidence_value":           measure.evidence_value,
            "calculated_ts":            measure.calculated_ts,
            "as_of_date":               as_of_today.isoformat(),
            "pipeline_run_id":          pipeline_run_id,
        })

    # ── ADT Event Feed (derived from encounters) ───────────────────────────────
    # Reconstruct A01/A03 pairs from encounter period_start/period_end.
    # In production this reads from silver.adt_events which captures HL7 ADT messages.
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
                facility_name=enc.facility_name,
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
        adt_rows.append({
            "event_id":                 ev.event_id,
            "tenant_id":                ev.tenant_id,
            "patient_key":              ev.patient_key,
            "event_type":               ev.event_type,
            "event_type_display":       ev.event_type_display,
            "event_datetime":           ev.event_datetime.isoformat(),
            "facility_name":            ev.facility_name,
            "current_location":         ev.current_location,
            "primary_diagnosis_icd10":  ev.primary_diagnosis_icd10,
            "primary_diagnosis_display": ev.primary_diagnosis_display,
            "attributed_pcp_npi":       ev.attributed_pcp_npi,
            "attributed_pcp_name":      ev.attributed_pcp_name,
            "is_readmission_30d":       ev.is_readmission_30d,
            "prior_discharge_date":     ev.prior_discharge_date.isoformat() if ev.prior_discharge_date else None,
            "loaded_ts":                ev.loaded_ts,
            "pipeline_run_id":          pipeline_run_id,
        })

print(f"Gold records built:")
print(f"  patient_summary  : {len(summary_rows)}")
print(f"  quality_measures : {len(measure_rows)}")
print(f"  adt_event_feed   : {len(adt_rows)}")

for s in summary_rows:
    print(f"\n  Patient {s['patient_key'][:16]}...")
    print(f"    full_name         : {s['full_name']}")
    print(f"    age_years         : {s['age_years']}")
    print(f"    charlson_index    : {s['charlson_index']}")
    print(f"    risk_tier         : {s['risk_tier']}")
    print(f"    flag_diabetes     : {s['flag_diabetes']}")
    print(f"    flag_hypertension : {s['flag_hypertension']}")
    print(f"    encounters_12m    : {s['total_encounters_12m']}")

for m in measure_rows:
    print(f"\n  Measure {m['measure_code']}")
    print(f"    denominator       : {m['denominator']}")
    print(f"    numerator         : {m['numerator']}")
    print(f"    evidence_value    : {m['evidence_value']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Gold tables (PySpark)

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
    LongType, TimestampType
)

# ── patient_summary ───────────────────────────────────────────────────────────
summary_schema = StructType([
    StructField("tenant_id",                StringType(),  False),
    StructField("patient_key",              StringType(),  False),
    StructField("full_name",                StringType(),  True),
    StructField("birth_date",               StringType(),  True),
    StructField("age_years",                IntegerType(), True),
    StructField("gender",                   StringType(),  True),
    StructField("state",                    StringType(),  True),
    StructField("postal_code",              StringType(),  True),
    StructField("attributed_pcp_npi",       StringType(),  True),
    StructField("attributed_pcp_name",      StringType(),  True),
    StructField("attribution_method",       StringType(),  True),
    StructField("charlson_index",           IntegerType(), True),
    StructField("elixhauser_index",         IntegerType(), True),
    StructField("risk_tier",                StringType(),  True),
    StructField("risk_score_updated_ts",    StringType(),  True),
    StructField("total_encounters_12m",     IntegerType(), True),
    StructField("total_ed_visits_12m",      IntegerType(), True),
    StructField("total_inpatient_days_12m", IntegerType(), True),
    StructField("last_encounter_date",      StringType(),  True),
    StructField("flag_diabetes",            BooleanType(), True),
    StructField("flag_hypertension",        BooleanType(), True),
    StructField("flag_heart_failure",       BooleanType(), True),
    StructField("flag_ckd",                 BooleanType(), True),
    StructField("flag_copd",                BooleanType(), True),
    StructField("flag_depression",          BooleanType(), True),
    StructField("as_of_date",              StringType(),  False),
    StructField("refreshed_ts",             StringType(),  False),
    StructField("pipeline_run_id",          StringType(),  False),
])

if summary_rows:
    summary_df = spark.createDataFrame(summary_rows, schema=summary_schema)
    summary_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(TBL_GOLD_SUMMARY)
    print(f"Wrote {summary_df.count()} row(s) to {TBL_GOLD_SUMMARY}")
else:
    print(f"No patient_summary rows to write")

# ── quality_measures ──────────────────────────────────────────────────────────
measures_schema = StructType([
    StructField("measure_id",               StringType(),  False),
    StructField("tenant_id",                StringType(),  False),
    StructField("patient_key",              StringType(),  False),
    StructField("measure_code",             StringType(),  False),
    StructField("measure_name",             StringType(),  True),
    StructField("measure_steward",          StringType(),  True),
    StructField("measurement_year",         IntegerType(), False),
    StructField("numerator",                BooleanType(), True),
    StructField("denominator",              BooleanType(), False),
    StructField("exclusion",                BooleanType(), True),
    StructField("exclusion_reason",         StringType(),  True),
    StructField("evidence_encounter_id",    StringType(),  True),
    StructField("evidence_observation_id",  StringType(),  True),
    StructField("evidence_date",            StringType(),  True),
    StructField("evidence_value",           StringType(),  True),
    StructField("calculated_ts",            StringType(),  False),
    StructField("as_of_date",              StringType(),  False),
    StructField("pipeline_run_id",          StringType(),  False),
])

if measure_rows:
    measures_df = spark.createDataFrame(measure_rows, schema=measures_schema)
    measures_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(TBL_GOLD_MEASURES)
    print(f"Wrote {measures_df.count()} row(s) to {TBL_GOLD_MEASURES}")
else:
    print(f"No quality_measures rows to write")

# ── adt_event_feed ────────────────────────────────────────────────────────────
adt_schema = StructType([
    StructField("event_id",                  StringType(),  False),
    StructField("tenant_id",                 StringType(),  False),
    StructField("patient_key",               StringType(),  False),
    StructField("event_type",                StringType(),  False),
    StructField("event_type_display",        StringType(),  True),
    StructField("event_datetime",            StringType(),  False),
    StructField("facility_name",             StringType(),  True),
    StructField("current_location",          StringType(),  True),
    StructField("primary_diagnosis_icd10",   StringType(),  True),
    StructField("primary_diagnosis_display", StringType(),  True),
    StructField("attributed_pcp_npi",        StringType(),  True),
    StructField("attributed_pcp_name",       StringType(),  True),
    StructField("is_readmission_30d",        BooleanType(), False),
    StructField("prior_discharge_date",      StringType(),  True),
    StructField("loaded_ts",                 StringType(),  False),
    StructField("pipeline_run_id",           StringType(),  False),
])

if adt_rows:
    adt_df = spark.createDataFrame(adt_rows, schema=adt_schema)
    adt_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(TBL_GOLD_ADT)
    print(f"Wrote {adt_df.count()} row(s) to {TBL_GOLD_ADT}")
else:
    print(f"No adt_event_feed rows to write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write audit log

# COMMAND ----------

completed_at = datetime.now(timezone.utc)
duration_s   = (completed_at - started_at).total_seconds()

audit_rows = [{
    "log_id":               str(uuid.uuid4()),
    "pipeline_run_id":      pipeline_run_id,
    "notebook_name":        NOTEBOOK_NAME,
    "started_at":           started_at.replace(tzinfo=None),
    "completed_at":         completed_at.replace(tzinfo=None),
    "patients_processed":   len(silver_patients),
    "summaries_written":    len(summary_rows),
    "measures_written":     len(measure_rows),
    "adt_events_written":   len(adt_rows),
    "status":               "COMPLETED",
    "error_detail":         None,
    "pipeline_version":     PIPELINE_VERSION,
    "created_ts":           completed_at.replace(tzinfo=None),
}]

audit_schema = StructType([
    StructField("log_id",               StringType(),    False),
    StructField("pipeline_run_id",      StringType(),    False),
    StructField("notebook_name",        StringType(),    False),
    StructField("started_at",           TimestampType(), False),
    StructField("completed_at",         TimestampType(), True),
    StructField("patients_processed",   LongType(),      True),
    StructField("summaries_written",    LongType(),      True),
    StructField("measures_written",     LongType(),      True),
    StructField("adt_events_written",   LongType(),      True),
    StructField("status",               StringType(),    False),
    StructField("error_detail",         StringType(),    True),
    StructField("pipeline_version",     StringType(),    True),
    StructField("created_ts",           TimestampType(), False),
])

audit_df = spark.createDataFrame(audit_rows, schema=audit_schema)
audit_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(TBL_GOLD_AUDIT)

print(f"Audit log written  : status=COMPLETED")
print(f"  patients_processed : {audit_rows[0]['patients_processed']}")
print(f"  summaries_written  : {audit_rows[0]['summaries_written']}")
print(f"  measures_written   : {audit_rows[0]['measures_written']}")
print(f"  adt_events_written : {audit_rows[0]['adt_events_written']}")
print(f"  duration           : {duration_s:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — preview Gold output

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            tenant_id,
            patient_key,
            full_name,
            age_years,
            charlson_index,
            risk_tier,
            flag_diabetes,
            flag_hypertension,
            total_encounters_12m,
            as_of_date,
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
            tenant_id,
            patient_key,
            measure_code,
            measurement_year,
            denominator,
            numerator,
            evidence_value,
            evidence_date,
            pipeline_run_id
        FROM {TBL_GOLD_MEASURES}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        ORDER BY measure_code
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            tenant_id,
            patient_key,
            event_type,
            event_type_display,
            event_datetime,
            facility_name,
            is_readmission_30d,
            primary_diagnosis_icd10,
            pipeline_run_id
        FROM {TBL_GOLD_ADT}
        WHERE pipeline_run_id = '{pipeline_run_id}'
        ORDER BY event_datetime
    """)
)
