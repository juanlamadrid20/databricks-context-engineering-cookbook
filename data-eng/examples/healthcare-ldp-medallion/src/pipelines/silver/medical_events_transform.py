"""
Silver Layer: Medical Events Transformation with Clinical Validation
Implements comprehensive clinical data validation, temporal consistency, and referential integrity with silver_patients.
Ensures all medical events reference valid patients and maintains clinical data quality standards.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, trim, upper, regexp_replace,
    to_date, datediff, from_json, round as spark_round, count, sum as spark_sum,
    avg, min as spark_min, max as spark_max, stddev, broadcast, get_json_object
)
from pyspark.sql.types import MapType, StringType

# Environment-aware configuration with Unity Catalog
CATALOG = spark.conf.get("CATALOG", "juan_dev")
SCHEMA = spark.conf.get("SCHEMA", "data_eng")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")

# Critical path handling pattern (include in all pipeline files)
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = "/".join(notebook_path.split("/")[:-3])  # Go up 3 levels from silver to src
    sys.path.insert(0, f"{base_path}")
except:
    # Fallback to multiple common paths
    possible_paths = ["/Workspace/src", "/databricks/driver/src", "/repos/src"]
    for path in possible_paths:
        if os.path.exists(path):
            sys.path.insert(0, path)
            break

# Import healthcare schemas and constants
try:
    from pipelines.shared.healthcare_schemas import (
        VALID_EVENT_TYPES, VALID_FACILITY_TYPES, TARGET_DATA_QUALITY_SCORE
    )
except ImportError:
    # Fallback constants if import fails
    print("⚠️  Warning: Could not import healthcare schemas, using fallback constants")
    VALID_EVENT_TYPES = ["OFFICE_VISIT", "PROCEDURE", "LAB_TEST", "IMAGING", "EMERGENCY", "SURGERY", "CONSULTATION"]
    VALID_FACILITY_TYPES = ["HOSPITAL", "CLINIC", "LABORATORY", "IMAGING_CENTER", "PHARMACY", "URGENT_CARE"]
    TARGET_DATA_QUALITY_SCORE = 99.5

# Staging view for silver transformation with FK validation
@dlt.view(name="silver_medical_events_staging")
def silver_medical_events_staging():
    """
    Staging view for silver medical events transformation with referential integrity validation.
    Joins with silver_patients to ensure all events reference valid patients.
    """
    return (
        dlt.read("bronze_medical_events")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .filter("_rescued_data IS NULL")  # Filter out malformed records
        .filter("event_id IS NOT NULL AND LENGTH(event_id) >= 5")  # Valid event IDs only
        .filter("patient_id IS NOT NULL AND LENGTH(patient_id) >= 5")  # Valid patient IDs only
        .join(
            broadcast(dlt.read("silver_patients").select("patient_id")),  # FK validation
            "patient_id",
            "inner"  # Only keep events with valid patient references
        )
    )

@dlt.table(
    name="silver_medical_events",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Validated medical events with referential integrity, clinical validation, and temporal consistency",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
        "pipelines.pii.fields": "patient_id,medical_provider,facility_name,vital_signs",  # Mark PII/PHI fields
        "compliance": "HIPAA",
        "data_classification": "PHI",
        "environment": PIPELINE_ENV,
        "business_domain": "clinical_events",
        "referential_integrity": "validated"
    }
)
@dlt.expect_all_or_drop({
    "valid_event_id": "event_id IS NOT NULL AND LENGTH(event_id) >= 5",
    "valid_patient_fk": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "valid_event_date": "event_date_parsed IS NOT NULL",
    "valid_event_type": "event_type_standardized IN ('OFFICE_VISIT', 'PROCEDURE', 'LAB_TEST', 'IMAGING', 'EMERGENCY', 'SURGERY', 'CONSULTATION')",
    "valid_facility_type": "facility_type_standardized IN ('HOSPITAL', 'CLINIC', 'LABORATORY', 'IMAGING_CENTER', 'PHARMACY', 'URGENT_CARE')",
    "valid_provider": "medical_provider_standardized IS NOT NULL"
})
@dlt.expect_all({
    "reasonable_visit_duration": "visit_duration_validated IS NULL OR visit_duration_validated BETWEEN 5 AND 1440",  # 5 min to 24 hours
    "valid_primary_diagnosis": "primary_diagnosis_standardized IS NOT NULL AND LENGTH(primary_diagnosis_standardized) >= 3",
    "recent_event_date": "event_date_parsed >= '2020-01-01'",  # Events within reasonable timeframe
    "temporal_consistency": "event_date_parsed <= CURRENT_DATE()",  # No future events
    "data_quality_threshold": "data_quality_score >= 95.0"
})
def silver_medical_events():
    """
    Transform medical events with comprehensive clinical validation and referential integrity.
    
    CRITICAL IMPLEMENTATIONS:
    - Referential integrity: All events must reference valid patients in silver_patients
    - Clinical validation: Event types, facility types, diagnosis codes
    - Temporal validation: Event dates, visit durations, clinical workflows
    - Provider standardization: Medical provider names and specialties
    """
    
    return (
        dlt.read("silver_medical_events_staging")
        
        # DATE PARSING AND TEMPORAL VALIDATION
        .withColumn("event_date_parsed", 
                   to_date(col("event_date"), "yyyy-MM-dd"))
        
        # EVENT AND FACILITY STANDARDIZATION
        .withColumn("event_type_standardized", 
                   upper(trim(regexp_replace(col("event_type"), "[^A-Z_]", ""))))
        .withColumn("facility_type_standardized", 
                   upper(trim(regexp_replace(col("facility_type"), "[^A-Z_]", ""))))
        .withColumn("event_description_cleaned", 
                   trim(col("event_description")))
        
        # PROVIDER INFORMATION STANDARDIZATION
        .withColumn("medical_provider_standardized", 
                   trim(regexp_replace(col("medical_provider"), "[^a-zA-Z\\s\\.]", "")))
        .withColumn("provider_type_standardized", 
                   trim(col("provider_type")))
        .withColumn("facility_name_standardized", 
                   trim(col("facility_name")))
        
        # CLINICAL DATA STANDARDIZATION
        .withColumn("primary_diagnosis_standardized", 
                   trim(regexp_replace(col("primary_diagnosis"), "[^A-Z0-9.]", "")))
        .withColumn("secondary_diagnosis_standardized", 
                   when(col("secondary_diagnosis").isNotNull() & (col("secondary_diagnosis") != ""),
                        trim(regexp_replace(col("secondary_diagnosis"), "[^A-Z0-9.]", "")))
                   .otherwise(None))
        
        # VISIT DURATION VALIDATION
        .withColumn("visit_duration_validated", 
                   when((col("visit_duration_minutes").cast("int") >= 5) & 
                        (col("visit_duration_minutes").cast("int") <= 1440),
                        col("visit_duration_minutes").cast("int"))
                   .otherwise(None))
        
        # VITAL SIGNS PARSING (JSON format)
        .withColumn("vital_signs_bp_systolic",
                   when(col("vital_signs").isNotNull(),
                        regexp_replace(get_json_object(col("vital_signs"), "$.bp"), "/.*", "").cast("int"))
                   .otherwise(None))
        .withColumn("vital_signs_bp_diastolic",
                   when(col("vital_signs").isNotNull(),
                        regexp_replace(get_json_object(col("vital_signs"), "$.bp"), ".*/", "").cast("int"))
                   .otherwise(None))
        .withColumn("vital_signs_pulse",
                   when(col("vital_signs").isNotNull(),
                        get_json_object(col("vital_signs"), "$.pulse").cast("int"))
                   .otherwise(None))
        .withColumn("vital_signs_temp",
                   when(col("vital_signs").isNotNull(),
                        get_json_object(col("vital_signs"), "$.temp").cast("double"))
                   .otherwise(None))
        
        # MEDICATIONS PARSING (JSON array format)
        .withColumn("medications_count",
                   when((col("medications_prescribed").isNotNull()) & (col("medications_prescribed") != ""),
                        size(split(regexp_replace(col("medications_prescribed"), "[\\[\\]\"]", ""), ",")))
                   .otherwise(0))
        
        # BOOLEAN FLAGS STANDARDIZATION
        .withColumn("follow_up_required_flag", 
                   when(col("follow_up_required").cast("string").isin(["true", "True", "1", "yes", "Yes"]), True)
                   .when(col("follow_up_required").cast("string").isin(["false", "False", "0", "no", "No"]), False)
                   .otherwise(None))
        .withColumn("emergency_flag_validated", 
                   when(col("emergency_flag").cast("string").isin(["true", "True", "1", "yes", "Yes"]), True)
                   .when(col("emergency_flag").cast("string").isin(["false", "False", "0", "no", "No"]), False)
                   .otherwise(False))
        .withColumn("admission_flag_validated", 
                   when(col("admission_flag").cast("string").isin(["true", "True", "1", "yes", "Yes"]), True)
                   .when(col("admission_flag").cast("string").isin(["false", "False", "0", "no", "No"]), False)
                   .otherwise(False))
        
        # CLINICAL CATEGORIZATION
        .withColumn("care_intensity", 
                   when(col("visit_duration_validated") > 240, "HIGH")
                   .when(col("visit_duration_validated") > 60, "MEDIUM")
                   .when(col("visit_duration_validated") > 0, "LOW")
                   .otherwise("UNKNOWN"))
        .withColumn("care_urgency",
                   when(col("emergency_flag_validated") == True, "EMERGENCY")
                   .when(col("admission_flag_validated") == True, "URGENT")
                   .when(col("follow_up_required_flag") == True, "ROUTINE_FOLLOWUP")
                   .otherwise("ROUTINE"))
        .withColumn("clinical_complexity",
                   when((col("secondary_diagnosis_standardized").isNotNull()) & 
                        (col("medications_count") > 2), "COMPLEX")
                   .when(col("medications_count") > 0, "MODERATE")
                   .otherwise("SIMPLE"))
        
        # TEMPORAL ANALYSIS
        .withColumn("days_since_event", 
                   datediff(current_timestamp().cast("date"), col("event_date_parsed")))
        .withColumn("event_recency_category",
                   when(col("days_since_event") <= 30, "RECENT")
                   .when(col("days_since_event") <= 90, "MODERATE")
                   .when(col("days_since_event") <= 365, "OLDER")
                   .otherwise("HISTORICAL"))
        
        # VITAL SIGNS VALIDATION
        .withColumn("vital_signs_normal",
                   when((col("vital_signs_bp_systolic").between(90, 140)) &
                        (col("vital_signs_bp_diastolic").between(60, 90)) &
                        (col("vital_signs_pulse").between(60, 100)) &
                        (col("vital_signs_temp").between(97.0, 100.5)), True)
                   .otherwise(False))
        
        # DATA QUALITY SCORING
        .withColumn("completeness_score",
                   ((when(col("event_id").isNotNull(), 1).otherwise(0) +
                     when(col("patient_id").isNotNull(), 1).otherwise(0) +
                     when(col("event_date_parsed").isNotNull(), 1).otherwise(0) +
                     when(col("event_type_standardized").isNotNull(), 1).otherwise(0) +
                     when(col("medical_provider_standardized").isNotNull(), 1).otherwise(0) +
                     when(col("primary_diagnosis_standardized").isNotNull(), 1).otherwise(0)) / 6.0 * 100))
        .withColumn("validity_score",
                   ((when(col("event_type_standardized").isin(VALID_EVENT_TYPES), 1).otherwise(0) +
                     when(col("facility_type_standardized").isin(VALID_FACILITY_TYPES), 1).otherwise(0) +
                     when(col("visit_duration_validated").isNotNull(), 1).otherwise(0) +
                     when(col("primary_diagnosis_standardized").rlike("^[A-Z][0-9]{2}"), 1).otherwise(0) +  # Basic ICD-10 format
                     when(col("event_date_parsed") <= current_timestamp().cast("date"), 1).otherwise(0)) / 5.0 * 100))
        .withColumn("clinical_quality_score",
                   ((when(col("vital_signs_normal") == True, 1).otherwise(0) +
                     when(col("medications_count") >= 0, 1).otherwise(0) +
                     when(col("care_intensity") != "UNKNOWN", 1).otherwise(0)) / 3.0 * 100))
        .withColumn("data_quality_score",
                   (col("completeness_score") + col("validity_score") + col("clinical_quality_score")) / 3.0)
        
        # AUDIT AND COMPLIANCE METADATA
        .withColumn("processed_at", current_timestamp())
        .withColumn("referential_integrity_validated", lit(True))
        .withColumn("clinical_validation_applied", lit(True))
        .withColumn("temporal_consistency_validated", lit(True))
        .withColumn("data_retention_years", lit(7))  # HIPAA requirement
        .withColumn("_silver_processed_at", current_timestamp())
        
        # SELECT FINAL SILVER LAYER COLUMNS
        .select(
            "event_id",
            "patient_id",  # FK to silver_patients
            "event_date_parsed",
            "event_type_standardized",
            "event_description_cleaned",
            "medical_provider_standardized",
            "provider_type_standardized",
            "facility_name_standardized",
            "facility_type_standardized",
            "primary_diagnosis_standardized",
            "secondary_diagnosis_standardized",
            "vital_signs_bp_systolic",
            "vital_signs_bp_diastolic",
            "vital_signs_pulse",
            "vital_signs_temp",
            "vital_signs_normal",
            "medications_count",
            "visit_duration_validated",
            "follow_up_required_flag",
            "emergency_flag_validated",
            "admission_flag_validated",
            "care_intensity",
            "care_urgency",
            "clinical_complexity",
            "days_since_event",
            "event_recency_category",
            "completeness_score",
            "validity_score",
            "clinical_quality_score",
            "data_quality_score",
            "processed_at",
            "referential_integrity_validated",
            "_ingested_at",
            "_pipeline_env",
            "_file_name",
            "_silver_processed_at"
        )
    )

@dlt.table(
    name="silver_medical_events_orphaned",
    comment="Medical events that failed referential integrity validation - orphaned events without valid patient references"
)
def silver_medical_events_orphaned():
    """
    Quarantine medical events that reference non-existent patients.
    CRITICAL: Maintains audit trail for orphaned events for HIPAA compliance.
    """
    
    return (
        dlt.read("bronze_medical_events")
        .filter("_rescued_data IS NULL")
        .filter("event_id IS NOT NULL AND patient_id IS NOT NULL")
        .join(
            dlt.read("silver_patients").select("patient_id"),
            "patient_id",
            "left_anti"  # Events without matching patients
        )
        .withColumn("orphan_reason", lit("PATIENT_NOT_FOUND"))
        .withColumn("quarantined_at", current_timestamp())
        .withColumn("review_required", lit(True))
        .withColumn("referential_integrity_violation", lit(True))
        .withColumn("audit_preserved", lit(True))
    )

@dlt.table(
    name="silver_medical_events_quality_summary",
    comment="Data quality summary and clinical validation metrics for silver medical events layer"
)
def silver_medical_events_quality_summary():
    """
    Generate comprehensive data quality and clinical validation summary for medical events.
    """
    
    return (
        dlt.read("silver_medical_events")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .groupBy("event_type_standardized", "facility_type_standardized", "care_urgency")
        .agg(
            count("*").alias("total_events"),
            countDistinct("patient_id").alias("unique_patients"),
            avg("visit_duration_validated").alias("avg_visit_duration"),
            avg("medications_count").alias("avg_medications"),
            spark_sum(when(col("vital_signs_normal") == True, 1).otherwise(0)).alias("normal_vitals_count"),
            spark_sum(when(col("follow_up_required_flag") == True, 1).otherwise(0)).alias("followup_required_count"),
            spark_sum(when(col("emergency_flag_validated") == True, 1).otherwise(0)).alias("emergency_events"),
            avg("data_quality_score").alias("avg_data_quality_score"),
            spark_sum(when(col("data_quality_score") >= TARGET_DATA_QUALITY_SCORE, 1).otherwise(0)).alias("high_quality_events"),
            spark_sum(when(col("referential_integrity_validated") == True, 1).otherwise(0)).alias("ri_validated_events")
        )
        .withColumn("quality_sla_met", 
                   col("avg_data_quality_score") >= TARGET_DATA_QUALITY_SCORE)
        .withColumn("quality_rate",
                   (col("high_quality_events") / col("total_events") * 100))
        .withColumn("normal_vitals_rate",
                   (col("normal_vitals_count") / col("total_events") * 100))
        .withColumn("followup_rate",
                   (col("followup_required_count") / col("total_events") * 100))
        .withColumn("emergency_rate",
                   (col("emergency_events") / col("total_events") * 100))
        .withColumn("referential_integrity_rate",
                   (col("ri_validated_events") / col("total_events") * 100))
        .withColumn("assessment_date", current_timestamp())
        .withColumn("target_quality_score", lit(TARGET_DATA_QUALITY_SCORE))
    )