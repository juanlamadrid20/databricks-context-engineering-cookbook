"""
Bronze Layer: Patient Demographics Ingestion
Raw data ingestion for healthcare patient demographics with Auto Loader and schema enforcement.
Implements HIPAA-compliant data ingestion with comprehensive audit trails.
"""

import dlt
import sys
import os
from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Environment-aware configuration with Unity Catalog
CATALOG = spark.conf.get("CATALOG", "juan_dev")
SCHEMA = spark.conf.get("SCHEMA", "healthcare_data")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")
VOLUMES_PATH = spark.conf.get("VOLUMES_PATH", "/Volumes/juan_dev/healthcare_data/raw_data")
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
    from pipelines.shared.healthcare_schemas import (
        PATIENT_SCHEMA,
        BRONZE_PATIENT_SCHEMA, 
        PATIENT_DATA_QUALITY_EXPECTATIONS,
        VALID_REGIONS, 
        VALID_SEX_VALUES
    )
except ImportError:
    print("⚠️  Warning: Could not import healthcare schemas, using fallback schema")
    # Fallback schema definition if import fails
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
    
    # Input schema for CSV files (without metadata fields)
    PATIENT_SCHEMA = StructType([
        StructField("patient_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("sex", StringType(), True),
        StructField("region", StringType(), True),
        StructField("bmi", DoubleType(), True),
        StructField("smoker", BooleanType(), True),
        StructField("children", IntegerType(), True),
        StructField("charges", DoubleType(), True),
        StructField("insurance_plan", StringType(), True),
        StructField("coverage_start_date", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])
    
    BRONZE_PATIENT_SCHEMA = StructType([
        StructField("patient_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("sex", StringType(), True),
        StructField("region", StringType(), True),
        StructField("bmi", DoubleType(), True),
        StructField("smoker", BooleanType(), True),
        StructField("children", IntegerType(), True),
        StructField("charges", DoubleType(), True),
        StructField("insurance_plan", StringType(), True),
        StructField("coverage_start_date", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_pipeline_env", StringType(), True),
        StructField("_file_name", StringType(), True),
        StructField("_file_path", StringType(), True),
        StructField("_file_size", StringType(), True),
        StructField("_file_modification_time", StringType(), True),
    ])
    
    PATIENT_DATA_QUALITY_EXPECTATIONS = {
        "valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
        "valid_age": "age IS NOT NULL AND age BETWEEN 18 AND 85",
        "valid_sex": "sex IS NOT NULL AND sex IN ('MALE', 'FEMALE')",
        "valid_region": "region IS NOT NULL AND region IN ('NORTHEAST', 'NORTHWEST', 'SOUTHEAST', 'SOUTHWEST')",
        "valid_bmi": "bmi IS NOT NULL AND bmi BETWEEN 16 AND 50",
        "valid_charges": "charges IS NOT NULL AND charges > 0"
    }

@dlt.table(
    name="bronze_patients",
    comment="Raw patient demographics data with HIPAA compliance and audit logging",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
        "pipelines.pii.fields": "first_name,last_name",
        "data_classification": "PHI"
    }
)
@dlt.expect_all_or_drop(PATIENT_DATA_QUALITY_EXPECTATIONS)
@dlt.expect_all({
    "complete_name": "first_name IS NOT NULL AND last_name IS NOT NULL",
    "valid_coverage_date": "coverage_start_date IS NOT NULL",
    "valid_insurance_plan": "insurance_plan IS NOT NULL",
    "reasonable_age": "age BETWEEN 18 AND 80",
    "reasonable_bmi": "bmi BETWEEN 18 AND 40"
})
def bronze_patients():
    """
    CRITICAL: Uses Auto Loader with .format("cloudFiles") pattern required for DLT.
    Ingests patient CSV files from Databricks Volumes with schema enforcement.
    """
    return (
        spark.readStream
        .format("cloudFiles")  # CRITICAL: Required for DLT Autoloader
        .option("cloudFiles.format", "csv")
        .option("header", "true") 
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_patients")
        # .option("cloudFiles.schemaEvolution", "true")
        .option("cloudFiles.inferColumnTypes", "false")  # Use explicit schema
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(PATIENT_SCHEMA)
        .load(f"{VOLUMES_PATH}")
        .filter(col("patient_id").isNotNull())  # Basic non-null filter
        
        # Add file metadata using _metadata column (not deprecated input_file_name)
        .select("*", "_metadata")
        .withColumn("_file_name", col("_metadata.file_name"))
        .withColumn("_file_path", col("_metadata.file_path"))
        .withColumn("_file_size", col("_metadata.file_size").cast("string"))
        .withColumn("_file_modification_time", col("_metadata.file_modification_time").cast("string"))
        .drop("_metadata")  # Clean up metadata column
        
        # Add pipeline metadata
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
    )

print("✅ Bronze patient demographics ingestion pipeline loaded successfully")