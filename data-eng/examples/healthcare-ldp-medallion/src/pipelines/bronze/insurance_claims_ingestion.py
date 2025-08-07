"""
Bronze Layer: Insurance Claims Ingestion
Raw data ingestion for healthcare insurance claims with Auto Loader and referential integrity validation.
Implements claims processing with comprehensive audit trails for financial compliance.
"""

import dlt
import sys
import os
from pyspark.sql.functions import current_timestamp, col, lit

# Environment-aware configuration with Unity Catalog
CATALOG = spark.conf.get("CATALOG", "juan_dev")
SCHEMA = spark.conf.get("SCHEMA", "healthcare_data")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")
VOLUMES_PATH = spark.conf.get("VOLUMES_PATH", "/Volumes/juan_dev/healthcare_data/raw_data")
MAX_FILES_PER_TRIGGER = spark.conf.get("MAX_FILES_PER_TRIGGER", "100")

# Critical path handling pattern
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = "/".join(notebook_path.split("/")[:-3])
    sys.path.insert(0, f"{base_path}")
except:
    possible_paths = ["/Workspace/src", "/databricks/driver/src", "/repos/src"]
    for path in possible_paths:
        if os.path.exists(path):
            sys.path.insert(0, path)
            break

# Import healthcare schemas
try:
    from pipelines.shared.healthcare_schemas import (
        CLAIMS_SCHEMA,
        BRONZE_CLAIMS_SCHEMA,
        CLAIMS_DATA_QUALITY_EXPECTATIONS,
        VALID_CLAIM_STATUSES
    )
except ImportError:
    print("⚠️  Warning: Could not import healthcare schemas, using fallback schema")
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    
    # Input schema for CSV files (without metadata fields)
    CLAIMS_SCHEMA = StructType([
        StructField("claim_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("claim_amount", DoubleType(), True),
        StructField("claim_date", StringType(), True),
        StructField("diagnosis_code", StringType(), True),
        StructField("procedure_code", StringType(), True),
        StructField("claim_status", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])
    
    BRONZE_CLAIMS_SCHEMA = StructType([
        StructField("claim_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("claim_amount", DoubleType(), True),
        StructField("claim_date", StringType(), True),
        StructField("diagnosis_code", StringType(), True),
        StructField("procedure_code", StringType(), True),
        StructField("claim_status", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_pipeline_env", StringType(), True),
        StructField("_file_name", StringType(), True),
        StructField("_file_path", StringType(), True),
        StructField("_file_size", StringType(), True),
        StructField("_file_modification_time", StringType(), True),
    ])
    
    CLAIMS_DATA_QUALITY_EXPECTATIONS = {
        "valid_claim_id": "claim_id IS NOT NULL AND LENGTH(claim_id) >= 5",
        "valid_patient_reference": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
        "valid_claim_amount": "claim_amount IS NOT NULL AND claim_amount > 0",
        "valid_claim_date": "claim_date IS NOT NULL",
        "valid_claim_status": "claim_status IS NOT NULL AND claim_status IN ('submitted', 'approved', 'denied', 'paid')"
    }

@dlt.table(
    name="bronze_claims",
    comment="Raw insurance claims data with financial audit trails and referential integrity",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",  # Financial audit requirement
        "data_classification": "Financial",
        "compliance": "SOX,HIPAA"
    }
)
@dlt.expect_all_or_drop(CLAIMS_DATA_QUALITY_EXPECTATIONS)
@dlt.expect_all({
    "financial_reasonableness": "claim_amount BETWEEN 10 AND 100000",
    "temporal_consistency": "claim_date IS NOT NULL",
    "medical_coding": "diagnosis_code IS NOT NULL AND procedure_code IS NOT NULL"
})
def bronze_claims():
    """
    CRITICAL: Uses Auto Loader with .format("cloudFiles") pattern required for DLT.
    Ingests insurance claims CSV files from Databricks Volumes with financial validation.
    """
    return (
        spark.readStream
        .format("cloudFiles")  # CRITICAL: Required for DLT Autoloader
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_claims")
        # .option("cloudFiles.schemaEvolution", "true") # TODO: 
        .option("cloudFiles.inferColumnTypes", "false")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(CLAIMS_SCHEMA)
        .load(f"{VOLUMES_PATH}")
        .filter(col("claim_id").isNotNull() & col("patient_id").isNotNull())
        
        # Add file metadata using _metadata column
        .select("*", "_metadata")
        .withColumn("_file_name", col("_metadata.file_name"))
        .withColumn("_file_path", col("_metadata.file_path"))
        .withColumn("_file_size", col("_metadata.file_size").cast("string"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time").cast("string"))
        .drop("_metadata")
        
        # Add pipeline metadata
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
    )

print("✅ Bronze insurance claims ingestion pipeline loaded successfully")