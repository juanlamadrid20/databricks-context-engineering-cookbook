"""
Bronze Layer: Insurance Claims Ingestion
Raw data ingestion for healthcare insurance claims with Auto Loader and schema enforcement.
Implements comprehensive claims data validation and audit trails for HIPAA compliance.
"""

import dlt
import sys
import os
from pyspark.sql.functions import current_timestamp, col, lit, count, countDistinct, sum as spark_sum, avg, min as spark_min, max as spark_max
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
    from pipelines.shared.healthcare_schemas import CLAIMS_SCHEMA, VALID_CLAIM_STATUSES, VALID_CLAIM_TYPES
except ImportError:
    # Fallback schema definition if import fails
    print("⚠️  Warning: Could not import healthcare schemas, using fallback schema")
    CLAIMS_SCHEMA = StructType([
        StructField("claim_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("claim_amount", StringType(), True),
        StructField("claim_date", StringType(), True),
        StructField("approval_date", StringType(), True),
        StructField("payment_date", StringType(), True),
        StructField("diagnosis_code", StringType(), True),
        StructField("procedure_code", StringType(), True),
        StructField("diagnosis_description", StringType(), True),
        StructField("procedure_description", StringType(), True),
        StructField("claim_status", StringType(), True),
        StructField("claim_type", StringType(), True),
        StructField("provider_id", StringType(), True),
        StructField("provider_name", StringType(), True),
        StructField("denial_reason", StringType(), True)
    ])

@dlt.table(
    name="bronze_claims",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Raw insurance claims data ingestion with financial validation and audit trails",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
        "pipelines.pii.fields": "patient_id,provider_name",  # Mark PII fields
        "compliance": "HIPAA",
        "data_classification": "PHI",
        "environment": PIPELINE_ENV,
        "business_domain": "insurance_claims"
    }
)
@dlt.expect_all_or_drop({
    "valid_claim_id": "claim_id IS NOT NULL AND LENGTH(claim_id) >= 5",
    "valid_patient_id_fk": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "non_null_claim_amount": "claim_amount IS NOT NULL",
    "valid_claim_date": "claim_date IS NOT NULL AND LENGTH(claim_date) >= 8",
    "valid_claim_status": "claim_status IS NOT NULL"
})
@dlt.expect_all({
    "positive_claim_amount": "CAST(claim_amount AS DOUBLE) > 0",
    "reasonable_claim_amount": "CAST(claim_amount AS DOUBLE) <= 1000000",  # Max $1M claims
    "valid_diagnosis_code": "diagnosis_code IS NOT NULL AND LENGTH(diagnosis_code) >= 3",
    "valid_procedure_code": "procedure_code IS NOT NULL AND LENGTH(procedure_code) >= 3",
    "valid_provider_info": "provider_id IS NOT NULL AND provider_name IS NOT NULL"
})
def bronze_claims():
    """
    Ingest raw insurance claims data using Auto Loader with comprehensive validation.
    
    CRITICAL PATTERNS:
    - Uses .format("cloudFiles") for Auto Loader (MANDATORY)
    - Uses @dlt.table for persistence and checkpointing (NOT @dlt.view)
    - Includes financial data validation for claim amounts
    - Implements foreign key validation for patient_id references
    """
    
    return (
        spark.readStream.format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_claims")
        .option("cloudFiles.inferColumnTypes", "false")  # Use explicit schema
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle schema changes
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(CLAIMS_SCHEMA)  # Enforce healthcare claims schema
        .load(f"{VOLUMES_PATH}/claims*.csv")  # Claims files pattern
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
        .withColumn("_financial_data", lit(True))  # Mark as containing financial information
    )

@dlt.table(
    name="bronze_claims_quality_metrics",
    comment="Data quality metrics and financial validation for insurance claims ingestion"
)
def bronze_claims_quality_metrics():
    """
    Generate data quality metrics for bronze claims data.
    Tracks ingestion rates, financial validation, and claim processing metrics.
    """
    
    return (
        dlt.read("bronze_claims")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .groupBy("_pipeline_env", "_file_name", "claim_status") 
        .agg(
            count("*").alias("total_claims"),
            count("claim_id").alias("valid_claim_ids"),
            countDistinct("claim_id").alias("unique_claims"),
            countDistinct("patient_id").alias("unique_patients"),
            spark_sum("CAST(claim_amount AS DOUBLE)").alias("total_claim_amount"),
            avg("CAST(claim_amount AS DOUBLE)").alias("avg_claim_amount"),
            spark_min("CAST(claim_amount AS DOUBLE)").alias("min_claim_amount"),
            spark_max("CAST(claim_amount AS DOUBLE)").alias("max_claim_amount"),
            count("diagnosis_code").alias("claims_with_diagnosis"),
            count("procedure_code").alias("claims_with_procedure"),
            spark_min("_ingested_at").alias("batch_start"),
            spark_max("_ingested_at").alias("batch_end")
        )
        .withColumn("data_quality_score", 
                   (col("valid_claim_ids") / col("total_claims") * 100))
        .withColumn("clinical_completeness_score",
                   ((col("claims_with_diagnosis") + col("claims_with_procedure")) / (col("total_claims") * 2) * 100))
        .withColumn("uniqueness_score",
                   (col("unique_claims") / col("total_claims") * 100))
        .withColumn("processed_at", current_timestamp())
        .withColumn("financial_validation_passed", 
                   col("min_claim_amount") > 0)  # All amounts should be positive
    )

@dlt.table(
    name="bronze_claims_anomaly_detection",
    comment="Anomaly detection for unusual claim patterns and potential fraud indicators"
)
def bronze_claims_anomaly_detection():
    """
    Detect anomalous claim patterns that may indicate data quality issues or fraud.
    """
    
    return (
        dlt.read("bronze_claims")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .filter("CAST(claim_amount AS DOUBLE) > 50000")  # High-value claims
        .select(
            "claim_id",
            "patient_id", 
            "claim_amount",
            "claim_date",
            "claim_status",
            "diagnosis_code",
            "procedure_code",
            "provider_id",
            "_ingested_at"
        )
        .withColumn("anomaly_type", lit("high_value_claim"))
        .withColumn("anomaly_threshold", lit(50000))
        .withColumn("detected_at", current_timestamp())
        .withColumn("review_required", lit(True))
    )