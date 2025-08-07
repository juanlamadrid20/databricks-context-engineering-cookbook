"""
Bronze Layer: Medical Events Ingestion
Raw data ingestion for healthcare medical events with Auto Loader and provider validation.
Implements medical events processing with comprehensive clinical audit trails.
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
        MEDICAL_EVENTS_SCHEMA,
        BRONZE_MEDICAL_EVENTS_SCHEMA,
        MEDICAL_EVENTS_DATA_QUALITY_EXPECTATIONS,
        VALID_EVENT_TYPES
    )
except ImportError:
    print("⚠️  Warning: Could not import healthcare schemas, using fallback schema")
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    
    # Input schema for CSV files (without metadata fields)
    MEDICAL_EVENTS_SCHEMA = StructType([
        StructField("event_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("event_date", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("medical_provider", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])
    
    BRONZE_MEDICAL_EVENTS_SCHEMA = StructType([
        StructField("event_id", StringType(), False),
        StructField("patient_id", StringType(), False),
        StructField("event_date", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("medical_provider", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_pipeline_env", StringType(), True),
        StructField("_file_name", StringType(), True),
        StructField("_file_path", StringType(), True),
        StructField("_file_size", StringType(), True),
        StructField("_file_modification_time", StringType(), True),
    ])
    
    MEDICAL_EVENTS_DATA_QUALITY_EXPECTATIONS = {
        "valid_event_id": "event_id IS NOT NULL AND LENGTH(event_id) >= 5",
        "valid_patient_reference": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
        "valid_event_date": "event_date IS NOT NULL",
        "valid_event_type": "event_type IS NOT NULL",
        "valid_medical_provider": "medical_provider IS NOT NULL"
    }

@dlt.table(
    name="bronze_medical_events",
    comment="Raw medical events data with clinical audit trails and provider validation",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",  # Clinical audit requirement
        "data_classification": "Clinical",
        "compliance": "HIPAA"
    }
)
@dlt.expect_all_or_drop(MEDICAL_EVENTS_DATA_QUALITY_EXPECTATIONS)
@dlt.expect_all({
    "temporal_validity": "event_date IS NOT NULL",
    "clinical_context": "event_type IS NOT NULL AND medical_provider IS NOT NULL",
    "provider_identification": "LENGTH(medical_provider) >= 3"
})
def bronze_medical_events():
    """
    CRITICAL: Uses Auto Loader with .format("cloudFiles") pattern required for DLT.
    Ingests medical events CSV files from Databricks Volumes with clinical validation.
    """
    return (
        spark.readStream
        .format("cloudFiles")  # CRITICAL: Required for DLT Autoloader
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_medical_events")
        # .option("cloudFiles.schemaEvolution", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(MEDICAL_EVENTS_SCHEMA)
        .load(f"{VOLUMES_PATH}")
        .filter(col("event_id").isNotNull() & col("patient_id").isNotNull())
        
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

print("✅ Bronze medical events ingestion pipeline loaded successfully")