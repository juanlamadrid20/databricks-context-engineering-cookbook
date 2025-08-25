"""
Bronze Layer - Insurance Claims Data Ingestion

Raw claims data ingestion from claims system with referential integrity validation.

CRITICAL REQUIREMENTS (from PRP):
- Use exact BRONZE_CLAIMS_SCHEMA with schema enforcement
- Basic validation that patient_id is not null (referential integrity)
- @dlt.expect_all_or_drop for malformed claims
- CSV ingestion with schema evolution for claims data
- Enable change data feed for audit compliance
"""

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp, col, when, lit, length, upper,
    regexp_replace, concat, count, sum as spark_sum
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
    name="bronze_claims",
    comment="Raw insurance claims data ingestion with referential integrity validation",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true", 
        "delta.enableChangeDataFeed": "true",  # Audit compliance
        "source.system": "CLAIMS_SYSTEM",
        "data.classification": "PHI",
        "referential.integrity": "patient_id->bronze_patients.patient_id"
    }
)
@dlt.expect_all_or_drop({
    "valid_claim_id": "claim_id IS NOT NULL AND LENGTH(claim_id) >= 5",
    "valid_patient_reference": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5", # CRITICAL: Referential integrity
    "valid_claim_amount": "claim_amount IS NOT NULL AND LENGTH(claim_amount) > 0",
    "valid_claim_date": "claim_date IS NOT NULL",
    "valid_source_system": "source_system = 'CLAIMS_SYSTEM'",
    "no_negative_amounts": "NOT(claim_amount LIKE '-%')"  # Basic amount validation
})
@dlt.expect_all({
    "reasonable_claim_amounts": "CAST(claim_amount AS DOUBLE) BETWEEN 0 AND 100000",  # Basic range check
    "recent_claims": "claim_date >= '2022-01-01'",  # Claims within reasonable timeframe
    "valid_diagnosis_format": "diagnosis_code IS NULL OR LENGTH(diagnosis_code) >= 3",
    "valid_procedure_format": "procedure_code IS NULL OR LENGTH(procedure_code) = 5"
})
def bronze_claims() -> DataFrame:
    """
    Raw insurance claims data ingestion with referential integrity validation.
    
    Data Sources: CLAIMS_SYSTEM → CSV files in Databricks Volumes
    Schema: Uses exact BRONZE_CLAIMS_SCHEMA with enforcement
    Validation: Basic claim structure and referential integrity to patients
    
    Returns:
        DataFrame: Raw claims data with validation and metadata
    """
    # CRITICAL: Must use .format("cloudFiles") for DLT streaming
    return (
        spark.readStream
        .format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_claims")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle claims schema changes
        .option("cloudFiles.maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(HealthcareSchemas.BRONZE_CLAIMS_SCHEMA)  # Exact schema enforcement
        .load(f"{VOLUMES_PATH}/claims/")
        
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
                       when(col("claim_id").isNull() | (length(col("claim_id")) < 5), 
                            lit("INVALID_CLAIM_ID;")).otherwise(lit("")),
                       when(col("patient_id").isNull() | (length(col("patient_id")) < 5), 
                            lit("INVALID_PATIENT_ID;")).otherwise(lit("")),
                       when(col("claim_amount").isNull() | (length(col("claim_amount")) == 0), 
                            lit("MISSING_CLAIM_AMOUNT;")).otherwise(lit("")),
                       when(col("claim_date").isNull(), 
                            lit("MISSING_CLAIM_DATE;")).otherwise(lit(""))
                   ))
        
        # Basic claim amount validation (convert string to numeric for validation)
        .withColumn("_claim_amount_numeric",
                   when(regexp_replace(col("claim_amount"), "[^0-9.]", "").cast("double").isNotNull(),
                        regexp_replace(col("claim_amount"), "[^0-9.]", "").cast("double"))
                   .otherwise(lit(0.0)))
        
        # Claim status standardization (basic cleanup)
        .withColumn("claim_status_standardized",
                   when(upper(col("claim_status")).isin("SUBMITTED", "SUBMIT", "NEW"), "submitted")
                   .when(upper(col("claim_status")).isin("APPROVED", "APPROVE", "ACCEPTED"), "approved") 
                   .when(upper(col("claim_status")).isin("DENIED", "DENY", "REJECTED"), "denied")
                   .when(upper(col("claim_status")).isin("PAID", "PAYMENT", "COMPLETE"), "paid")
                   .otherwise(col("claim_status")))
        
        # ICD-10 diagnosis code validation (basic format check)
        .withColumn("diagnosis_code_valid",
                   when(col("diagnosis_code").isNull(), True)  # Allow null
                   .when(regexp_replace(col("diagnosis_code"), r"^[A-Z][0-9]{2}\.?[0-9A-Z]{0,4}$", "VALID") == "VALID", True)
                   .otherwise(False))
        
        # CPT procedure code validation (basic format check)
        .withColumn("procedure_code_valid", 
                   when(col("procedure_code").isNull(), True)  # Allow null
                   .when(regexp_replace(col("procedure_code"), r"^[0-9]{5}$", "VALID") == "VALID", True)
                   .otherwise(False))
        
        # Provider validation
        .withColumn("has_provider_info",
                   when(col("provider_id").isNotNull() & (length(col("provider_id")) > 0), True)
                   .otherwise(False))
        
        # Service location validation  
        .withColumn("has_service_location",
                   when(col("service_location").isNotNull() & (length(col("service_location")) > 0), True)
                   .otherwise(False))
        
        # Claim complexity scoring (based on amount and coding)
        .withColumn("claim_complexity_score",
                   when(col("_claim_amount_numeric") > 5000, 3)  # High-cost procedures
                   .when((col("diagnosis_code").isNotNull()) & (col("procedure_code").isNotNull()), 2)  # Coded procedures
                   .when(col("_claim_amount_numeric") > 500, 1)  # Medium-cost procedures
                   .otherwise(0))  # Basic procedures
        
        # Data completeness scoring
        .withColumn("data_completeness_score",
                   (when(col("claim_id").isNotNull(), 0.2).otherwise(0) +
                    when(col("patient_id").isNotNull(), 0.2).otherwise(0) +
                    when(col("claim_amount").isNotNull(), 0.2).otherwise(0) +
                    when(col("claim_date").isNotNull(), 0.2).otherwise(0) +
                    when(col("diagnosis_code").isNotNull() | col("procedure_code").isNotNull(), 0.2).otherwise(0)))
    )


@dlt.table(
    name="bronze_claims_quarantine", 
    comment="Quarantined claims records that failed data quality expectations",
    table_properties={
        "quality": "bronze_quarantine",
        "delta.enableChangeDataFeed": "true"
    }
)
def bronze_claims_quarantine() -> DataFrame:
    """
    Quarantine table for claims records that fail data quality expectations.
    
    Captures problematic claims for investigation and data source improvement.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/bronze_claims_quarantine")
        .schema(HealthcareSchemas.BRONZE_CLAIMS_SCHEMA)
        .load(f"{VOLUMES_PATH}/claims/")
        
        # Only include records that would fail our main expectations
        .filter(
            (col("claim_id").isNull()) |
            (length(col("claim_id")) < 5) |
            (col("patient_id").isNull()) |
            (length(col("patient_id")) < 5) |
            (col("claim_amount").isNull()) |
            (length(col("claim_amount")) == 0) |
            (col("claim_date").isNull()) |
            (col("source_system") != "CLAIMS_SYSTEM") |
            (col("claim_amount").like("-%"))  # Negative amounts
        )
        
        .withColumn("_quarantine_reason",
                   concat(
                       when(col("claim_id").isNull() | (length(col("claim_id")) < 5), 
                            lit("INVALID_CLAIM_ID;")).otherwise(lit("")),
                       when(col("patient_id").isNull() | (length(col("patient_id")) < 5), 
                            lit("INVALID_PATIENT_ID;")).otherwise(lit("")),
                       when(col("claim_amount").isNull() | (length(col("claim_amount")) == 0), 
                            lit("MISSING_CLAIM_AMOUNT;")).otherwise(lit("")),
                       when(col("claim_date").isNull(), 
                            lit("MISSING_CLAIM_DATE;")).otherwise(lit("")),
                       when(col("source_system") != "CLAIMS_SYSTEM", 
                            lit("INVALID_SOURCE_SYSTEM;")).otherwise(lit("")),
                       when(col("claim_amount").like("-%"), 
                            lit("NEGATIVE_CLAIM_AMOUNT;")).otherwise(lit(""))
                   ))
        .withColumn("_quarantine_timestamp", current_timestamp())
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
    )


# Data quality monitoring view
@dlt.view(name="bronze_claims_quality_metrics")
def bronze_claims_quality_metrics():
    """
    Real-time data quality metrics for claims data ingestion monitoring.
    
    Provides insights into:
    - Claim counts and processing rates
    - Referential integrity validation results
    - Medical coding quality (ICD-10/CPT validation)
    - Financial data validation (claim amounts)
    """
    claims = dlt.read("bronze_claims")
    
    return (
        claims
        .groupBy("_pipeline_env", "_file_name")
        .agg(
            count("*").alias("total_claims"),
            countDistinct("claim_id").alias("unique_claims"),
            countDistinct("patient_id").alias("unique_patients"),
            spark_sum(when(col("_data_quality_flags") == "", 1).otherwise(0)).alias("clean_records"),
            spark_sum(when(col("diagnosis_code_valid"), 1).otherwise(0)).alias("valid_diagnosis_codes"),
            spark_sum(when(col("procedure_code_valid"), 1).otherwise(0)).alias("valid_procedure_codes"),
            spark_sum(when(col("has_provider_info"), 1).otherwise(0)).alias("claims_with_provider"),
            spark_sum(when(col("has_service_location"), 1).otherwise(0)).alias("claims_with_location"),
            avg(col("_claim_amount_numeric")).alias("avg_claim_amount"),
            spark_sum(col("_claim_amount_numeric")).alias("total_claim_amount"),
            avg(col("claim_complexity_score")).alias("avg_complexity_score"),
            avg(col("data_completeness_score")).alias("avg_completeness_score"),
            max("_ingested_at").alias("last_ingestion_time")
        )
        .withColumn("data_quality_percentage",
                   (col("clean_records").cast("double") / col("total_claims") * 100))
        .withColumn("diagnosis_coding_rate", 
                   (col("valid_diagnosis_codes").cast("double") / col("total_claims") * 100))
        .withColumn("procedure_coding_rate",
                   (col("valid_procedure_codes").cast("double") / col("total_claims") * 100))
        .withColumn("provider_completion_rate",
                   (col("claims_with_provider").cast("double") / col("total_claims") * 100))
        .withColumn("location_completion_rate", 
                   (col("claims_with_location").cast("double") / col("total_claims") * 100))
        .withColumn("claims_per_patient",
                   (col("total_claims").cast("double") / col("unique_patients")))
    )