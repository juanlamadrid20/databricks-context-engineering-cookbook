"""
Bronze Layer - Patient EHR Data Ingestion

Raw patient data ingestion from EHR system with HIPAA compliance and audit logging.

CRITICAL REQUIREMENTS (from PRP):
- Use exact BRONZE_PATIENT_SCHEMA with schema enforcement
- Implement .format("cloudFiles") with CSV ingestion from Volumes
- Include _metadata column handling for file tracking
- @dlt.expect_all for basic patient data validation
- Enable change data feed for HIPAA compliance audit trails
- NO raw SSN storage - must be hashed immediately upon ingestion
"""

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp, col, when, lit, sha2, concat,
    regexp_replace, upper, length, isnan, isnull
)

# Import healthcare schemas and utilities
from shared.healthcare_schemas import (
    HealthcareSchemas, 
    HealthcareValidationRules,
    HealthcarePipelineUtilities
)

# Environment-aware configuration loading - CRITICAL pattern for all pipeline files
CATALOG = spark.conf.get("CATALOG", "juan_dev")
SCHEMA = spark.conf.get("SCHEMA", "ctx_eng")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")
VOLUMES_PATH = spark.conf.get("VOLUMES_PATH", "/Volumes/juan_dev/ctx_eng/raw_data")
MAX_FILES_PER_TRIGGER = spark.conf.get("MAX_FILES_PER_TRIGGER", "100")


@dlt.table(
    name="bronze_patients",
    comment="Raw patient EHR data ingestion with HIPAA compliance and audit logging",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
        "pipelines.pii.fields": "ssn,ssn_hash,patient_id,mrn",  # PII identification
        "compliance.framework": "HIPAA",
        "data.classification": "PHI",  # Protected Health Information
        "source.system": "EHR_SYSTEM"
    }
)
@dlt.expect_all_or_drop({
    "valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "no_test_patients": "NOT(UPPER(last_name) LIKE '%TEST%' OR UPPER(first_name) LIKE '%TEST%')",
    "hipaa_compliant_no_raw_ssn": "ssn IS NULL OR LENGTH(ssn) = 0",  # Ensure no raw SSN after processing
    "valid_source_system": "source_system = 'EHR_SYSTEM'",
    "valid_effective_date": "effective_date IS NOT NULL"
})
@dlt.expect_all({
    "data_freshness": "effective_date >= current_date() - interval 365 days",
    "valid_demographics": "first_name IS NOT NULL AND last_name IS NOT NULL",
    "reasonable_birth_dates": "date_of_birth IS NULL OR (date_of_birth >= '1900-01-01' AND date_of_birth <= current_date())"
})
def bronze_patients() -> DataFrame:
    """
    Raw patient EHR data ingestion with HIPAA compliance.
    
    Data Sources: EHR_SYSTEM → CSV files in Databricks Volumes
    Schema: Uses exact BRONZE_PATIENT_SCHEMA with enforcement
    Compliance: HIPAA PII handling, audit logging, change data feed
    
    Returns:
        DataFrame: Raw patient data with metadata and HIPAA compliance
    """
    # CRITICAL: Must use .format("cloudFiles") for DLT streaming
    return (
        spark.readStream
        .format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_patients")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle EHR schema changes
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(HealthcareSchemas.BRONZE_PATIENT_SCHEMA)  # Exact schema enforcement
        .load(f"{VOLUMES_PATH}/patients/")
        
        # CRITICAL: File metadata handling (NOT deprecated input_file_name())
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
        
        # CRITICAL: HIPAA PII handling - Hash SSN immediately and remove raw value
        .withColumn("ssn_hash", 
                   when(col("ssn").isNotNull() & (length(col("ssn")) > 0), 
                        sha2(concat(col("ssn"), lit(HealthcarePipelineUtilities.get_hipaa_salt())), 256))
                   .otherwise(lit(None)))
        .drop("ssn")  # CRITICAL: Remove raw SSN immediately after hashing
        
        # Data quality enrichment
        .withColumn("_data_quality_flags", 
                   concat(
                       when(col("patient_id").isNull(), lit("MISSING_PATIENT_ID;")).otherwise(lit("")),
                       when(col("first_name").isNull(), lit("MISSING_FIRST_NAME;")).otherwise(lit("")),
                       when(col("last_name").isNull(), lit("MISSING_LAST_NAME;")).otherwise(lit("")),
                       when(col("date_of_birth").isNull(), lit("MISSING_DOB;")).otherwise(lit(""))
                   ))
        
        # Clinical data standardization (basic cleanup for bronze layer)
        .withColumn("gender", 
                   when(upper(col("gender")).isin("M", "MALE"), "Male")
                   .when(upper(col("gender")).isin("F", "FEMALE"), "Female") 
                   .otherwise(col("gender")))
        
        # Emergency contact data validation
        .withColumn("emergency_contact_complete",
                   when((col("emergency_contact_name").isNotNull()) & 
                        (col("emergency_contact_phone").isNotNull()), True)
                   .otherwise(False))
        
        # Provider assignment validation
        .withColumn("has_primary_care_provider",
                   when(col("primary_care_provider").isNotNull(), True)
                   .otherwise(False))
        
        # Insurance validation
        .withColumn("has_insurance_info",
                   when(col("insurance_id").isNotNull(), True)
                   .otherwise(False))
        
        # Clinical safety: Ensure effective_date is not in the future
        .withColumn("effective_date",
                   when(col("effective_date") > current_timestamp(), current_timestamp())
                   .otherwise(col("effective_date")))
    )


@dlt.table(
    name="bronze_patients_quarantine",
    comment="Quarantined patient records that failed data quality expectations",
    table_properties={
        "quality": "bronze_quarantine",
        "delta.enableChangeDataFeed": "true",
        "compliance.framework": "HIPAA"
    }
)
def bronze_patients_quarantine() -> DataFrame:
    """
    Quarantine table for patient records that fail data quality expectations.
    
    This table captures records that don't meet basic validation requirements
    for investigation and potential data source improvements.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv") 
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_patients_quarantine")
        .schema(HealthcareSchemas.BRONZE_PATIENT_SCHEMA)
        .load(f"{VOLUMES_PATH}/patients/")
        
        # Only include records that would fail our main expectations
        .filter(
            (col("patient_id").isNull()) |
            (length(col("patient_id")) < 5) |
            (upper(col("last_name")).like("%TEST%")) |
            (upper(col("first_name")).like("%TEST%")) |
            (col("source_system") != "EHR_SYSTEM") |
            (col("effective_date").isNull())
        )
        
        .withColumn("_quarantine_reason", 
                   concat(
                       when(col("patient_id").isNull() | (length(col("patient_id")) < 5), 
                            lit("INVALID_PATIENT_ID;")).otherwise(lit("")),
                       when(upper(col("last_name")).like("%TEST%") | upper(col("first_name")).like("%TEST%"), 
                            lit("TEST_PATIENT;")).otherwise(lit("")),
                       when(col("source_system") != "EHR_SYSTEM", 
                            lit("INVALID_SOURCE_SYSTEM;")).otherwise(lit("")),
                       when(col("effective_date").isNull(), 
                            lit("MISSING_EFFECTIVE_DATE;")).otherwise(lit(""))
                   ))
        .withColumn("_quarantine_timestamp", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
    )


# Data quality monitoring view
@dlt.view(name="bronze_patients_quality_metrics")
def bronze_patients_quality_metrics():
    """
    Real-time data quality metrics for patient data ingestion monitoring.
    
    Provides insights into:
    - Record counts and ingestion rates
    - Data quality scores and validation failures  
    - HIPAA compliance metrics
    - File processing statistics
    """
    patients = dlt.read("bronze_patients")
    quarantine = dlt.read("bronze_patients_quarantine") 
    
    return (
        patients
        .groupBy("_pipeline_env", "_file_name")
        .agg(
            count("*").alias("total_records"),
            countDistinct("patient_id").alias("unique_patients"),
            sum(when(col("_data_quality_flags") == "", 1).otherwise(0)).alias("clean_records"),
            sum(when(col("ssn_hash").isNotNull(), 1).otherwise(0)).alias("records_with_ssn"),
            sum(when(col("emergency_contact_complete"), 1).otherwise(0)).alias("complete_emergency_contacts"),
            sum(when(col("has_primary_care_provider"), 1).otherwise(0)).alias("patients_with_pcp"),
            sum(when(col("has_insurance_info"), 1).otherwise(0)).alias("patients_with_insurance"),
            max("_ingested_at").alias("last_ingestion_time")
        )
        .withColumn("data_quality_percentage", 
                   (col("clean_records").cast("double") / col("total_records") * 100))
        .withColumn("emergency_contact_completion_rate",
                   (col("complete_emergency_contacts").cast("double") / col("total_records") * 100))
        .withColumn("pcp_assignment_rate",
                   (col("patients_with_pcp").cast("double") / col("total_records") * 100))
        .withColumn("insurance_coverage_rate", 
                   (col("patients_with_insurance").cast("double") / col("total_records") * 100))
    )