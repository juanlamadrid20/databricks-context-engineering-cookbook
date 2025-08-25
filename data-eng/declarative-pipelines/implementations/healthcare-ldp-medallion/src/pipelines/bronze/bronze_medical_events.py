"""
Bronze Layer - Medical Events Data Ingestion

Raw medical events ingestion from ADT/Lab systems with clinical data validation.

CRITICAL REQUIREMENTS (from PRP):
- Use exact BRONZE_MEDICAL_EVENTS_SCHEMA with schema enforcement
- Basic validation that patient_id is not null (referential integrity)
- @dlt.expect_all for clinical data validation
- CSV ingestion for medical events from ADT/Lab systems
- Clinical validation rules for encounter data
"""

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp, col, when, lit, length, upper,
    regexp_replace, concat, count, sum as spark_sum,
    to_date, datediff, split, regexp_extract
)
import re

# Import healthcare schemas and utilities
from shared.healthcare_schemas import (
    HealthcareSchemas,
    HealthcareValidationRules,
    HealthcarePipelineUtilities
)

# Environment-aware configuration loading
CATALOG = spark.conf.get("CATALOG", "juan_dev")
SCHEMA = spark.conf.get("SCHEMA", "ctx_eng")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")
VOLUMES_PATH = spark.conf.get("VOLUMES_PATH", "/Volumes/juan_dev/ctx_eng/raw_data")
MAX_FILES_PER_TRIGGER = spark.conf.get("MAX_FILES_PER_TRIGGER", "100")


@dlt.table(
    name="bronze_medical_events",
    comment="Raw medical events data ingestion from ADT/Lab systems with clinical validation",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",  # Clinical audit compliance
        "source.system": "ADT_SYSTEM,LAB_SYSTEM",
        "data.classification": "PHI",
        "clinical.domain": "medical_events",
        "referential.integrity": "patient_id->bronze_patients.patient_id"
    }
)
@dlt.expect_all_or_drop({
    "valid_event_id": "event_id IS NOT NULL AND LENGTH(event_id) >= 5",
    "valid_patient_reference": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5", # CRITICAL: Referential integrity
    "valid_event_date": "event_date IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL AND LENGTH(event_type) > 0",
    "valid_source_system": "source_system IN ('ADT_SYSTEM', 'LAB_SYSTEM')",
    "no_future_events": "event_date <= CURRENT_DATE()"  # Clinical safety: no future events
})
@dlt.expect_all({
    "reasonable_event_dates": "event_date >= '2020-01-01'",  # Events within reasonable timeframe
    "valid_event_types": "event_type IN ('admission', 'discharge', 'lab_result', 'vital_signs', 'clinical_note') OR event_type IS NOT NULL",
    "has_medical_provider": "medical_provider IS NOT NULL AND LENGTH(medical_provider) > 0",
    "clinical_data_present": "event_details IS NOT NULL OR clinical_results IS NOT NULL"
})
def bronze_medical_events() -> DataFrame:
    """
    Raw medical events data ingestion with clinical validation.
    
    Data Sources: ADT_SYSTEM/LAB_SYSTEM → CSV files in Databricks Volumes
    Schema: Uses exact BRONZE_MEDICAL_EVENTS_SCHEMA with enforcement
    Validation: Clinical data validation and referential integrity to patients
    
    Returns:
        DataFrame: Raw medical events data with clinical validation and metadata
    """
    # CRITICAL: Must use .format("cloudFiles") for DLT streaming
    return (
        spark.readStream
        .format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_medical_events")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle ADT/Lab schema changes
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(HealthcareSchemas.BRONZE_MEDICAL_EVENTS_SCHEMA)  # Exact schema enforcement
        .load(f"{VOLUMES_PATH}/medical_events/")
        
        # File metadata handling (NOT deprecated input_file_name())
        .select("*", "_metadata")  # REQUIRED: Explicitly select metadata
        .withColumn("_file_name", col("_metadata.file_name"))
        .withColumn("_file_path", col("_metadata.file_path"))
        .withColumn("_file_size", col("_metadata.file_size"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
        .drop("_metadata")  # Clean up temporary column
        
        # Pipeline metadata
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_version", lit("v1.0"))
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
        
        # Data quality enrichment and validation flags
        .withColumn("_data_quality_flags",
                   concat(
                       when(col("event_id").isNull() | (length(col("event_id")) < 5), 
                            lit("INVALID_EVENT_ID;")).otherwise(lit("")),
                       when(col("patient_id").isNull() | (length(col("patient_id")) < 5), 
                            lit("INVALID_PATIENT_ID;")).otherwise(lit("")),
                       when(col("event_date").isNull(), 
                            lit("MISSING_EVENT_DATE;")).otherwise(lit("")),
                       when(col("event_type").isNull() | (length(col("event_type")) == 0), 
                            lit("MISSING_EVENT_TYPE;")).otherwise(lit(""))
                   ))
        
        # Clinical event type standardization and validation
        .withColumn("event_type_standardized",
                   when(upper(col("event_type")).isin("ADMIT", "ADMISSION", "ADMITTED"), "admission")
                   .when(upper(col("event_type")).isin("DISCHARGE", "DISCHARGED", "RELEASED"), "discharge")
                   .when(upper(col("event_type")).isin("LAB", "LAB_RESULT", "LABORATORY"), "lab_result")
                   .when(upper(col("event_type")).isin("VITALS", "VITAL_SIGNS", "VITAL"), "vital_signs")
                   .when(upper(col("event_type")).isin("NOTE", "CLINICAL_NOTE", "NOTES"), "clinical_note")
                   .otherwise(col("event_type")))
        
        # Clinical event validation
        .withColumn("is_valid_clinical_event",
                   when(col("event_type_standardized").isin(
                        "admission", "discharge", "lab_result", "vital_signs", "clinical_note"), True)
                   .otherwise(False))
        
        # Medical provider validation and standardization
        .withColumn("provider_type",
                   when(upper(col("medical_provider")).like("%DR_%"), "physician")
                   .when(upper(col("medical_provider")).like("%NP_%"), "nurse_practitioner")
                   .when(upper(col("medical_provider")).like("%PA_%"), "physician_assistant")
                   .when(upper(col("medical_provider")).like("%RN_%"), "registered_nurse")
                   .otherwise("unknown"))
        
        # Clinical results parsing and validation
        .withColumn("has_vital_signs",
                   when((col("clinical_results").isNotNull()) &
                        ((upper(col("clinical_results")).like("%BP:%")) |
                         (upper(col("clinical_results")).like("%BLOOD PRESSURE%")) |
                         (upper(col("clinical_results")).like("%HR:%")) |
                         (upper(col("clinical_results")).like("%HEART RATE%"))), True)
                   .otherwise(False))
        
        .withColumn("has_lab_values",
                   when((col("clinical_results").isNotNull()) &
                        ((upper(col("clinical_results")).like("%GLUCOSE%")) |
                         (upper(col("clinical_results")).like("%CHOLESTEROL%")) |
                         (upper(col("clinical_results")).like("%HEMOGLOBIN%")) |
                         (upper(col("clinical_results")).like("%MG/DL%"))), True)
                   .otherwise(False))
        
        # Extract blood pressure values for clinical validation (basic parsing)
        .withColumn("blood_pressure_systolic",
                   when(col("clinical_results").rlike(r"BP:\s*(\d{2,3})/\d{2,3}"),
                        regexp_extract(col("clinical_results"), r"BP:\s*(\d{2,3})/\d{2,3}", 1).cast("integer"))
                   .otherwise(lit(None)))
        
        .withColumn("blood_pressure_diastolic", 
                   when(col("clinical_results").rlike(r"BP:\s*\d{2,3}/(\d{2,3})"),
                        regexp_extract(col("clinical_results"), r"BP:\s*\d{2,3}/(\d{2,3})", 1).cast("integer"))
                   .otherwise(lit(None)))
        
        # Clinical range validation for vital signs
        .withColumn("bp_within_normal_range",
                   when((col("blood_pressure_systolic").between(90, 180)) &
                        (col("blood_pressure_diastolic").between(60, 120)), True)
                   .when((col("blood_pressure_systolic").isNull()) & 
                         (col("blood_pressure_diastolic").isNull()), lit(None))
                   .otherwise(False))
        
        # Event recency calculation (days since event)
        .withColumn("days_since_event",
                   when(col("event_date").isNotNull(),
                        datediff(current_timestamp().cast("date"), 
                                to_date(col("event_date"), "yyyy-MM-dd")))
                   .otherwise(lit(None)))
        
        # Event urgency classification
        .withColumn("event_urgency",
                   when(col("event_type_standardized").isin("admission", "discharge"), "high")
                   .when(col("event_type_standardized") == "vital_signs", "medium")
                   .when(col("event_type_standardized").isin("lab_result", "clinical_note"), "low")
                   .otherwise("unknown"))
        
        # Clinical data completeness scoring
        .withColumn("clinical_completeness_score",
                   (when(col("event_id").isNotNull(), 0.15).otherwise(0) +
                    when(col("patient_id").isNotNull(), 0.15).otherwise(0) +
                    when(col("event_date").isNotNull(), 0.15).otherwise(0) +
                    when(col("event_type").isNotNull(), 0.15).otherwise(0) +
                    when(col("medical_provider").isNotNull(), 0.15).otherwise(0) +
                    when(col("event_details").isNotNull(), 0.125).otherwise(0) +
                    when(col("clinical_results").isNotNull(), 0.125).otherwise(0)))
        
        # Source system specific validation
        .withColumn("source_system_valid",
                   when(col("source_system").isin("ADT_SYSTEM", "LAB_SYSTEM"), True)
                   .otherwise(False))
        
        # Clinical safety flags
        .withColumn("clinical_safety_flags",
                   concat(
                       when((col("blood_pressure_systolic") > 200) | (col("blood_pressure_diastolic") > 120), 
                            lit("HYPERTENSIVE_CRISIS;")).otherwise(lit("")),
                       when((col("blood_pressure_systolic") < 70) | (col("blood_pressure_diastolic") < 40), 
                            lit("HYPOTENSIVE_EMERGENCY;")).otherwise(lit("")),
                       when(col("days_since_event") > 90, 
                            lit("STALE_CLINICAL_DATA;")).otherwise(lit(""))
                   ))
    )


@dlt.table(
    name="bronze_medical_events_quarantine",
    comment="Quarantined medical events that failed clinical data validation",
    table_properties={
        "quality": "bronze_quarantine",
        "delta.enableChangeDataFeed": "true",
        "clinical.validation": "failed"
    }
)
def bronze_medical_events_quarantine() -> DataFrame:
    """
    Quarantine table for medical events that fail clinical validation.
    
    Critical for clinical safety - captures events that may have data integrity issues.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_medical_events_quarantine")
        .schema(HealthcareSchemas.BRONZE_MEDICAL_EVENTS_SCHEMA)
        .load(f"{VOLUMES_PATH}/medical_events/")
        
        # Only include records that would fail our main clinical validations
        .filter(
            (col("event_id").isNull()) |
            (length(col("event_id")) < 5) |
            (col("patient_id").isNull()) |
            (length(col("patient_id")) < 5) |
            (col("event_date").isNull()) |
            (col("event_type").isNull()) |
            (length(col("event_type")) == 0) |
            (~col("source_system").isin("ADT_SYSTEM", "LAB_SYSTEM")) |
            (col("event_date") > current_timestamp().cast("date"))  # Future events
        )
        
        .withColumn("_quarantine_reason",
                   concat(
                       when(col("event_id").isNull() | (length(col("event_id")) < 5), 
                            lit("INVALID_EVENT_ID;")).otherwise(lit("")),
                       when(col("patient_id").isNull() | (length(col("patient_id")) < 5), 
                            lit("INVALID_PATIENT_ID;")).otherwise(lit("")),
                       when(col("event_date").isNull(), 
                            lit("MISSING_EVENT_DATE;")).otherwise(lit("")),
                       when(col("event_type").isNull() | (length(col("event_type")) == 0), 
                            lit("MISSING_EVENT_TYPE;")).otherwise(lit("")),
                       when(~col("source_system").isin("ADT_SYSTEM", "LAB_SYSTEM"), 
                            lit("INVALID_SOURCE_SYSTEM;")).otherwise(lit("")),
                       when(col("event_date") > current_timestamp().cast("date"), 
                            lit("FUTURE_EVENT_DATE;")).otherwise(lit(""))
                   ))
        .withColumn("_quarantine_timestamp", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
        .withColumn("_clinical_safety_flag", lit("QUARANTINED_FOR_CLINICAL_REVIEW"))
    )


# Clinical data quality monitoring view
@dlt.view(name="bronze_medical_events_quality_metrics")
def bronze_medical_events_quality_metrics():
    """
    Real-time clinical data quality metrics for medical events monitoring.
    
    Clinical Focus:
    - Event counts by clinical type and urgency
    - Vital signs validation and clinical range compliance
    - Provider assignment and clinical documentation completeness
    - Clinical safety flag monitoring
    """
    events = dlt.read("bronze_medical_events")
    
    return (
        events
        .groupBy("_pipeline_env", "_file_name", "source_system")
        .agg(
            count("*").alias("total_events"),
            countDistinct("event_id").alias("unique_events"),
            countDistinct("patient_id").alias("unique_patients"),
            spark_sum(when(col("_data_quality_flags") == "", 1).otherwise(0)).alias("clean_records"),
            spark_sum(when(col("is_valid_clinical_event"), 1).otherwise(0)).alias("valid_clinical_events"),
            spark_sum(when(col("has_vital_signs"), 1).otherwise(0)).alias("events_with_vital_signs"),
            spark_sum(when(col("has_lab_values"), 1).otherwise(0)).alias("events_with_lab_values"),
            spark_sum(when(col("bp_within_normal_range"), 1).otherwise(0)).alias("normal_blood_pressure_readings"),
            spark_sum(when(col("provider_type") != "unknown", 1).otherwise(0)).alias("events_with_known_provider"),
            spark_sum(when(col("event_urgency") == "high", 1).otherwise(0)).alias("high_urgency_events"),
            spark_sum(when(col("clinical_safety_flags") != "", 1).otherwise(0)).alias("events_with_safety_flags"),
            avg(col("clinical_completeness_score")).alias("avg_clinical_completeness"),
            avg(col("days_since_event")).alias("avg_event_age_days"),
            max("_ingested_at").alias("last_ingestion_time")
        )
        .withColumn("clinical_data_quality_percentage",
                   (col("clean_records").cast("double") / col("total_events") * 100))
        .withColumn("clinical_event_validation_rate",
                   (col("valid_clinical_events").cast("double") / col("total_events") * 100))
        .withColumn("vital_signs_capture_rate",
                   (col("events_with_vital_signs").cast("double") / col("total_events") * 100))
        .withColumn("lab_values_capture_rate", 
                   (col("events_with_lab_values").cast("double") / col("total_events") * 100))
        .withColumn("provider_assignment_rate",
                   (col("events_with_known_provider").cast("double") / col("total_events") * 100))
        .withColumn("clinical_safety_alert_rate",
                   (col("events_with_safety_flags").cast("double") / col("total_events") * 100))
        .withColumn("events_per_patient",
                   (col("total_events").cast("double") / col("unique_patients")))
        .withColumn("high_urgency_percentage",
                   (col("high_urgency_events").cast("double") / col("total_events") * 100))
    )