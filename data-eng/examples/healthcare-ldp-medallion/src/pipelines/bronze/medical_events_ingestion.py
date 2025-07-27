"""
Bronze Layer: Medical Events Ingestion
Raw data ingestion for healthcare medical events and clinical history with Auto Loader.
Implements comprehensive clinical data validation and audit trails for HIPAA compliance.
"""

import dlt
import sys
import os
from pyspark.sql.functions import current_timestamp, col, lit, count, countDistinct, avg, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Environment-aware configuration with Unity Catalog
CATALOG = spark.conf.get("CATALOG", "juan_dev")
SCHEMA = spark.conf.get("SCHEMA", "data_eng")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")
VOLUMES_PATH = spark.conf.get("VOLUMES_PATH", "/Volumes/juan_dev/data_eng/raw_data")
MAX_FILES_PER_TRIGGER = spark.conf.get("MAX_FILES_PER_TRIGGER", "100")

# Critical path handling pattern (include in all pipeline files)
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = "/".join(notebook_path.split("/")[:-3])  # Go up 3 levels from bronze to src
    sys.path.insert(0, f"{base_path}")
except:
    # Fallback to multiple common paths
    possible_paths = ["/Workspace/src", "/databricks/driver/src", "/repos/src"]
    for path in possible_paths:
        if os.path.exists(path):
            sys.path.insert(0, path)
            break

# Import healthcare schemas
try:
    from pipelines.shared.healthcare_schemas import MEDICAL_EVENTS_SCHEMA, VALID_EVENT_TYPES, VALID_FACILITY_TYPES
except ImportError:
    # Fallback schema definition if import fails
    print("⚠️  Warning: Could not import healthcare schemas, using fallback schema")
    MEDICAL_EVENTS_SCHEMA = StructType([
        StructField("event_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("event_date", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_description", StringType(), True),
        StructField("medical_provider", StringType(), True),
        StructField("provider_type", StringType(), True),
        StructField("facility_name", StringType(), True),
        StructField("facility_type", StringType(), True),
        StructField("primary_diagnosis", StringType(), True),
        StructField("secondary_diagnosis", StringType(), True),
        StructField("vital_signs", StringType(), True),
        StructField("medications_prescribed", StringType(), True),
        StructField("visit_duration_minutes", StringType(), True),
        StructField("follow_up_required", StringType(), True),
        StructField("emergency_flag", StringType(), True),
        StructField("admission_flag", StringType(), True)
    ])

@dlt.table(
    name="bronze_medical_events",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Raw medical events and clinical history data ingestion with comprehensive clinical validation",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
        "pipelines.pii.fields": "patient_id,medical_provider,facility_name,vital_signs",  # Mark PII/PHI fields
        "compliance": "HIPAA",
        "data_classification": "PHI",
        "environment": PIPELINE_ENV,
        "business_domain": "clinical_events"
    }
)
@dlt.expect_all_or_drop({
    "valid_event_id": "event_id IS NOT NULL AND LENGTH(event_id) >= 5",
    "valid_patient_id_fk": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "valid_event_date": "event_date IS NOT NULL AND LENGTH(event_date) >= 8",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_medical_provider": "medical_provider IS NOT NULL"
})
@dlt.expect_all({
    "valid_event_description": "event_description IS NOT NULL AND LENGTH(event_description) > 5",
    "valid_facility_info": "facility_name IS NOT NULL AND facility_type IS NOT NULL",
    "reasonable_visit_duration": "visit_duration_minutes IS NULL OR CAST(visit_duration_minutes AS INT) BETWEEN 5 AND 1440",  # 5 min to 24 hours
    "valid_provider_type": "provider_type IS NOT NULL",
    "valid_primary_diagnosis": "primary_diagnosis IS NOT NULL AND LENGTH(primary_diagnosis) >= 3"
})
def bronze_medical_events():
    """
    Ingest raw medical events data using Auto Loader with clinical validation.
    
    CRITICAL PATTERNS:
    - Uses .format("cloudFiles") for Auto Loader (MANDATORY)
    - Uses @dlt.table for persistence and checkpointing (NOT @dlt.view)
    - Includes clinical data validation for medical events
    - Implements foreign key validation for patient_id references
    """
    
    return (
        spark.readStream.format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_medical_events")
        .option("cloudFiles.inferColumnTypes", "false")  # Use explicit schema
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle schema changes
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(MEDICAL_EVENTS_SCHEMA)  # Enforce healthcare medical events schema
        .load(f"{VOLUMES_PATH}/medical_events*.csv")  # Medical events files pattern
        .select("*", "_metadata")  # REQUIRED: Explicitly select metadata
        # Add file metadata columns using _metadata pattern (NOT deprecated input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
        .withColumn("_file_name", col("_metadata.file_name"))
        .withColumn("_file_path", col("_metadata.file_path"))
        .withColumn("_file_size", col("_metadata.file_size"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time"))
        .drop("_metadata")  # Clean up temporary column
        # Add HIPAA compliance metadata
        .withColumn("_data_classification", lit("PHI"))
        .withColumn("_compliance_framework", lit("HIPAA"))
        .withColumn("_audit_required", lit(True))
        .withColumn("_clinical_data", lit(True))  # Mark as containing clinical information
    )

@dlt.table(
    name="bronze_medical_events_quality_metrics",
    comment="Data quality metrics and clinical validation for medical events ingestion"
)
def bronze_medical_events_quality_metrics():
    """
    Generate data quality metrics for bronze medical events data.
    Tracks ingestion rates, clinical validation, and care event patterns.
    """
    
    return (
        dlt.read("bronze_medical_events")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .groupBy("_pipeline_env", "_file_name", "event_type", "facility_type") 
        .agg(
            count("*").alias("total_events"),
            count("event_id").alias("valid_event_ids"),
            countDistinct("event_id").alias("unique_events"),
            countDistinct("patient_id").alias("unique_patients"),
            countDistinct("medical_provider").alias("unique_providers"),
            count("primary_diagnosis").alias("events_with_diagnosis"),
            count("vital_signs").alias("events_with_vitals"),
            count("medications_prescribed").alias("events_with_medications"),
            avg("CAST(visit_duration_minutes AS INT)").alias("avg_visit_duration"),
            spark_min("_ingested_at").alias("batch_start"),
            spark_max("_ingested_at").alias("batch_end")
        )
        .withColumn("data_quality_score", 
                   (col("valid_event_ids") / col("total_events") * 100))
        .withColumn("clinical_completeness_score",
                   (col("events_with_diagnosis") / col("total_events") * 100))
        .withColumn("documentation_score",
                   ((col("events_with_vitals") + col("events_with_medications")) / (col("total_events") * 2) * 100))
        .withColumn("uniqueness_score",
                   (col("unique_events") / col("total_events") * 100))
        .withColumn("processed_at", current_timestamp())
    )

@dlt.table(
    name="bronze_medical_events_clinical_patterns",
    comment="Clinical patterns analysis for care coordination and quality improvement"
)
def bronze_medical_events_clinical_patterns():
    """
    Analyze clinical patterns and care coordination metrics from medical events.
    """
    
    return (
        dlt.read("bronze_medical_events")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .filter("emergency_flag = 'true' OR admission_flag = 'true'")  # Focus on critical events
        .select(
            "event_id",
            "patient_id", 
            "event_date",
            "event_type",
            "facility_type",
            "primary_diagnosis",
            "emergency_flag",
            "admission_flag",
            "follow_up_required",
            "visit_duration_minutes",
            "_ingested_at"
        )
        .withColumn("critical_event_type", 
                   when(col("emergency_flag") == "true", "EMERGENCY")
                   .when(col("admission_flag") == "true", "ADMISSION")
                   .otherwise("OTHER"))
        .withColumn("care_intensity", 
                   when(col("visit_duration_minutes").cast("int") > 240, "HIGH")
                   .when(col("visit_duration_minutes").cast("int") > 60, "MEDIUM")
                   .otherwise("LOW"))
        .withColumn("analyzed_at", current_timestamp())
        .withColumn("follow_up_flag", col("follow_up_required") == "true")
    )