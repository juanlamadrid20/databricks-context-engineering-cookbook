"""
Bronze Layer: Patient Demographics Ingestion
Raw data ingestion for healthcare patient demographics with Auto Loader and schema enforcement.
Implements HIPAA-compliant data ingestion with comprehensive audit trails.
"""

import dlt
import sys
import os
from pyspark.sql.functions import current_timestamp, col, lit, count, countDistinct, avg, min as spark_min, max as spark_max, sum as spark_sum
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
    from pipelines.shared.healthcare_schemas import PATIENT_SCHEMA, VALID_REGIONS, VALID_SEX_VALUES
except ImportError:
    # Fallback schema definition if import fails
    print("⚠️  Warning: Could not import healthcare schemas, using fallback schema")
    PATIENT_SCHEMA = StructType([
        StructField("patient_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("age", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("region", StringType(), True),
        StructField("bmi", StringType(), True),
        StructField("smoker", StringType(), True),
        StructField("children", StringType(), True),
        StructField("charges", StringType(), True),
        StructField("insurance_plan", StringType(), True),
        StructField("coverage_start_date", StringType(), True),
        StructField("ssn", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

@dlt.table(
    name="bronze_patients",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Raw patient demographic data ingestion with HIPAA compliance and comprehensive audit trails",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
        "pipelines.pii.fields": "ssn,date_of_birth,first_name,last_name,zip_code",  # Mark PII fields
        "compliance": "HIPAA",
        "data_classification": "PHI",
        "environment": PIPELINE_ENV
    }
)
@dlt.expect_all_or_drop({
    "valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "non_null_demographics": "first_name IS NOT NULL AND last_name IS NOT NULL",
    "valid_age_format": "age IS NOT NULL",
    "valid_sex_format": "sex IS NOT NULL",
    "non_empty_ssn": "ssn IS NOT NULL AND LENGTH(ssn) >= 9"  # Basic SSN presence check
})
@dlt.expect_all({
    "reasonable_age_range": "CAST(age AS INT) BETWEEN 0 AND 120",  # Allow broader range for bronze
    "valid_timestamp": "timestamp IS NOT NULL",
    "valid_bmi_format": "bmi IS NOT NULL",
    "valid_charges_format": "charges IS NOT NULL"
})
def bronze_patients():
    """
    Ingest raw patient demographic data using Auto Loader with schema enforcement.
    
    CRITICAL PATTERNS:
    - Uses .format("cloudFiles") for Auto Loader (MANDATORY)
    - Uses @dlt.table for persistence and checkpointing (NOT @dlt.view)
    - Includes file metadata using _metadata column pattern
    - Implements HIPAA audit fields for compliance
    """
    
    return (
        spark.readStream.format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_patients")
        .option("cloudFiles.inferColumnTypes", "false")  # Use explicit schema
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle schema changes
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(PATIENT_SCHEMA)  # Enforce healthcare schema
        .load(f"{VOLUMES_PATH}/patients*.csv")  # Patient files pattern
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
    )

# Data Quality Monitoring for Bronze Layer
@dlt.table(
    name="bronze_patients_quality_metrics",
    comment="Data quality metrics and monitoring for patient demographics ingestion"
)
def bronze_patients_quality_metrics():
    """
    Generate data quality metrics for bronze patient data.
    Tracks ingestion rates, file processing, and basic validation metrics.
    """
    
    return (
        dlt.read("bronze_patients")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .groupBy("_pipeline_env", "_file_name") 
        .agg(
            count("*").alias("total_records"),
            count("patient_id").alias("valid_patient_ids"),
            count("ssn").alias("records_with_ssn"),
            countDistinct("patient_id").alias("unique_patients"),
            spark_min("_ingested_at").alias("batch_start"),
            spark_max("_ingested_at").alias("batch_end"),
            avg(col("age").cast("int")).alias("avg_age"),
            spark_sum(col("charges").cast("double")).alias("total_charges")
        )
        .withColumn("data_quality_score", 
                   (col("valid_patient_ids") / col("total_records") * 100))
        .withColumn("uniqueness_score",
                   (col("unique_patients") / col("total_records") * 100))
        .withColumn("processed_at", current_timestamp())
    )