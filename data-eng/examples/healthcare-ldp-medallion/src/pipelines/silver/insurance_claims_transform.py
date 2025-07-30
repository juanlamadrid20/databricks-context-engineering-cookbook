"""
Silver Layer: Insurance Claims Transformation with Referential Integrity
Implements comprehensive claims validation, financial data quality, and referential integrity with silver_patients.
Ensures all claims reference valid patients and maintains HIPAA audit compliance.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, trim, upper, regexp_replace,
    to_date, datediff, round as spark_round, count, sum as spark_sum,
    avg, min as spark_min, max as spark_max, stddev, broadcast
)

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
        VALID_CLAIM_STATUSES, VALID_CLAIM_TYPES, TARGET_DATA_QUALITY_SCORE
    )
except ImportError:
    # Fallback constants if import fails
    print("⚠️  Warning: Could not import healthcare schemas, using fallback constants")
    VALID_CLAIM_STATUSES = ["SUBMITTED", "APPROVED", "DENIED", "PAID", "PENDING", "CANCELLED"]
    VALID_CLAIM_TYPES = ["INPATIENT", "OUTPATIENT", "PHARMACY", "DENTAL", "VISION", "MENTAL_HEALTH"]
    TARGET_DATA_QUALITY_SCORE = 99.5

# Staging view for silver transformation with FK validation
@dlt.view(name="silver_claims_staging")
def silver_claims_staging():
    """
    Staging view for silver claims transformation with referential integrity validation.
    Joins with silver_patients to ensure all claims reference valid patients.
    """
    return (
        dlt.read("bronze_claims")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .filter("_rescued_data IS NULL")  # Filter out malformed records
        .filter("claim_id IS NOT NULL AND LENGTH(claim_id) >= 5")  # Valid claim IDs only
        .filter("patient_id IS NOT NULL AND LENGTH(patient_id) >= 5")  # Valid patient IDs only
        .join(
            broadcast(dlt.read("silver_patients").select("patient_id")),  # FK validation
            "patient_id",
            "inner"  # Only keep claims with valid patient references
        )
    )

@dlt.table(
    name="silver_claims",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Validated insurance claims with referential integrity, financial validation, and clinical standardization",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
        "pipelines.pii.fields": "patient_id,provider_name",  # Mark PII fields
        "compliance": "HIPAA",
        "data_classification": "PHI",
        "environment": PIPELINE_ENV,
        "business_domain": "insurance_claims",
        "referential_integrity": "validated"
    }
)
@dlt.expect_all_or_drop({
    "valid_claim_id": "claim_id IS NOT NULL AND LENGTH(claim_id) >= 5",
    "valid_patient_fk": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "positive_claim_amount": "claim_amount_validated > 0",
    "valid_claim_date": "claim_date_parsed IS NOT NULL",
    "valid_claim_status": "claim_status_standardized IN ('SUBMITTED', 'APPROVED', 'DENIED', 'PAID', 'PENDING', 'CANCELLED')",
    "valid_diagnosis_code": "diagnosis_code_standardized IS NOT NULL AND LENGTH(diagnosis_code_standardized) >= 3"
})
@dlt.expect_all({
    "reasonable_claim_amount": "claim_amount_validated <= 1000000",  # Max $1M claims
    "valid_procedure_code": "procedure_code_standardized IS NOT NULL AND LENGTH(procedure_code_standardized) >= 3",
    "valid_provider_info": "provider_id_standardized IS NOT NULL",
    "financial_consistency": "payment_date_parsed IS NULL OR approval_date_parsed IS NOT NULL",  # Can't pay without approval
    "temporal_consistency": "approval_date_parsed IS NULL OR claim_date_parsed <= approval_date_parsed",
    "data_quality_threshold": "data_quality_score >= 95.0"
})
def silver_claims():
    """
    Transform insurance claims with comprehensive validation and referential integrity.
    
    CRITICAL IMPLEMENTATIONS:
    - Referential integrity: All claims must reference valid patients in silver_patients
    - Financial validation: Amounts, dates, and claim processing logic
    - Clinical standardization: ICD-10 and CPT code format validation
    - Audit trail preservation with change data feed
    """
    
    return (
        dlt.read("silver_claims_staging")
        
        # FINANCIAL DATA VALIDATION AND STANDARDIZATION
        .withColumn("claim_amount_validated", 
                   when((col("claim_amount").cast("double") > 0) & 
                        (col("claim_amount").cast("double") <= 1000000),
                        spark_round(col("claim_amount").cast("double"), 2))
                   .otherwise(None))
        
        # DATE PARSING AND VALIDATION - Updated to handle ISO timestamp format
        .withColumn("claim_date_parsed", 
                   to_date(col("claim_date")))
        .withColumn("approval_date_parsed", 
                   when(col("approval_date").isNotNull() & (col("approval_date") != ""),
                        to_date(col("approval_date")))
                   .otherwise(None))
        .withColumn("payment_date_parsed", 
                   when(col("payment_date").isNotNull() & (col("payment_date") != ""),
                        to_date(col("payment_date")))
                   .otherwise(None))
        
        # CLAIM STATUS AND TYPE STANDARDIZATION
        .withColumn("claim_status_standardized", 
                   upper(trim(col("claim_status"))))
        .withColumn("claim_type_standardized", 
                   upper(trim(col("claim_type"))))
        
        # CLINICAL CODE STANDARDIZATION
        .withColumn("diagnosis_code_standardized", 
                   trim(regexp_replace(col("diagnosis_code"), "[^A-Z0-9.]", "")))
        .withColumn("procedure_code_standardized", 
                   trim(regexp_replace(col("procedure_code"), "[^A-Z0-9]", "")))
        .withColumn("diagnosis_description_cleaned", 
                   trim(col("diagnosis_description")))
        .withColumn("procedure_description_cleaned", 
                   trim(col("procedure_description")))
        
        # PROVIDER INFORMATION STANDARDIZATION
        .withColumn("provider_id_standardized", 
                   trim(upper(col("provider_id"))))
        .withColumn("provider_name_standardized", 
                   trim(col("provider_name")))
        .withColumn("denial_reason_standardized", 
                   when(col("denial_reason").isNotNull() & (col("denial_reason") != ""),
                        trim(col("denial_reason")))
                   .otherwise(None))
        
        # BUSINESS LOGIC VALIDATION
        .withColumn("claim_processing_days", 
                   when(col("approval_date_parsed").isNotNull(),
                        datediff(col("approval_date_parsed"), col("claim_date_parsed")))
                   .otherwise(None))
        .withColumn("payment_processing_days", 
                   when(col("payment_date_parsed").isNotNull() & col("approval_date_parsed").isNotNull(),
                        datediff(col("payment_date_parsed"), col("approval_date_parsed")))
                   .otherwise(None))
        .withColumn("total_processing_days", 
                   when(col("payment_date_parsed").isNotNull(),
                        datediff(col("payment_date_parsed"), col("claim_date_parsed")))
                   .otherwise(None))
        
        # CLAIM CATEGORIZATION
        .withColumn("claim_amount_category", 
                   when(col("claim_amount_validated") <= 500, "LOW")
                   .when(col("claim_amount_validated") <= 5000, "MEDIUM") 
                   .when(col("claim_amount_validated") <= 50000, "HIGH")
                   .otherwise("VERY_HIGH"))
        .withColumn("processing_speed_category",
                   when(col("claim_processing_days") <= 7, "FAST")
                   .when(col("claim_processing_days") <= 30, "NORMAL")
                   .when(col("claim_processing_days") <= 60, "SLOW")
                   .otherwise("DELAYED"))
        
        # DATA QUALITY SCORING
        .withColumn("completeness_score",
                   ((when(col("claim_id").isNotNull(), 1).otherwise(0) +
                     when(col("patient_id").isNotNull(), 1).otherwise(0) +
                     when(col("claim_amount_validated").isNotNull(), 1).otherwise(0) +
                     when(col("claim_date_parsed").isNotNull(), 1).otherwise(0) +
                     when(col("diagnosis_code_standardized").isNotNull(), 1).otherwise(0) +
                     when(col("procedure_code_standardized").isNotNull(), 1).otherwise(0)) / 6.0 * 100))
        .withColumn("validity_score",
                   ((when(col("claim_amount_validated") > 0, 1).otherwise(0) +
                     when(col("claim_status_standardized").isin(VALID_CLAIM_STATUSES), 1).otherwise(0) +
                     when(col("claim_type_standardized").isin(VALID_CLAIM_TYPES), 1).otherwise(0) +
                     when(col("diagnosis_code_standardized").rlike("^[A-Z][0-9]{2}"), 1).otherwise(0) +  # Basic ICD-10 format
                     when(col("procedure_code_standardized").rlike("^[0-9]{5}"), 1).otherwise(0)) / 5.0 * 100))  # Basic CPT format
        .withColumn("consistency_score",
                   ((when((col("payment_date_parsed").isNull()) | (col("approval_date_parsed").isNotNull()), 1).otherwise(0) +
                     when((col("approval_date_parsed").isNull()) | (col("claim_date_parsed") <= col("approval_date_parsed")), 1).otherwise(0) +
                     when((col("claim_status_standardized") == "DENIED") | (col("denial_reason_standardized").isNull()), 1).otherwise(0)) / 3.0 * 100))
        .withColumn("data_quality_score",
                   (col("completeness_score") + col("validity_score") + col("consistency_score")) / 3.0)
        
        # AUDIT AND COMPLIANCE METADATA
        .withColumn("processed_at", current_timestamp())
        .withColumn("referential_integrity_validated", lit(True))
        .withColumn("financial_validation_applied", lit(True))
        .withColumn("clinical_codes_standardized", lit(True))
        .withColumn("data_retention_years", lit(7))  # HIPAA requirement
        .withColumn("_silver_processed_at", current_timestamp())
        
        # SELECT FINAL SILVER LAYER COLUMNS
        .select(
            "claim_id",
            "patient_id",  # FK to silver_patients
            "claim_amount_validated",
            "claim_date_parsed",
            "approval_date_parsed", 
            "payment_date_parsed",
            "diagnosis_code_standardized",
            "procedure_code_standardized",
            "diagnosis_description_cleaned",
            "procedure_description_cleaned",
            "claim_status_standardized",
            "claim_type_standardized",
            "provider_id_standardized",
            "provider_name_standardized",
            "denial_reason_standardized",
            "claim_processing_days",
            "payment_processing_days",
            "total_processing_days",
            "claim_amount_category",
            "processing_speed_category",
            "completeness_score",
            "validity_score",
            "consistency_score",
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
    name="silver_claims_orphaned",
    comment="Claims that failed referential integrity validation - orphaned claims without valid patient references"
)
def silver_claims_orphaned():
    """
    Quarantine claims that reference non-existent patients.
    CRITICAL: Maintains audit trail for orphaned claims for HIPAA compliance.
    """
    
    return (
        dlt.read("bronze_claims")
        .filter("_rescued_data IS NULL")
        .filter("claim_id IS NOT NULL AND patient_id IS NOT NULL")
        .join(
            dlt.read("silver_patients").select("patient_id"),
            "patient_id",
            "left_anti"  # Claims without matching patients
        )
        .withColumn("orphan_reason", lit("PATIENT_NOT_FOUND"))
        .withColumn("quarantined_at", current_timestamp())
        .withColumn("review_required", lit(True))
        .withColumn("referential_integrity_violation", lit(True))
        .withColumn("audit_preserved", lit(True))
    )

@dlt.table(
    name="silver_claims_quality_summary",
    comment="Data quality summary and financial metrics for silver claims layer"
)
def silver_claims_quality_summary():
    """
    Generate comprehensive data quality and financial validation summary for claims.
    """
    
    return (
        dlt.read("silver_claims")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .groupBy("claim_status_standardized", "claim_type_standardized")
        .agg(
            count("*").alias("total_claims"),
            spark_sum("claim_amount_validated").alias("total_claim_amount"),
            avg("claim_amount_validated").alias("avg_claim_amount"),
            spark_min("claim_amount_validated").alias("min_claim_amount"),
            spark_max("claim_amount_validated").alias("max_claim_amount"),
            stddev("claim_amount_validated").alias("amount_stddev"),
            avg("claim_processing_days").alias("avg_processing_days"),
            avg("data_quality_score").alias("avg_data_quality_score"),
            spark_sum(when(col("data_quality_score") >= TARGET_DATA_QUALITY_SCORE, 1).otherwise(0)).alias("high_quality_claims"),
            spark_sum(when(col("referential_integrity_validated") == True, 1).otherwise(0)).alias("ri_validated_claims")
        )
        .withColumn("quality_sla_met", 
                   col("avg_data_quality_score") >= TARGET_DATA_QUALITY_SCORE)
        .withColumn("quality_rate",
                   (col("high_quality_claims") / col("total_claims") * 100))
        .withColumn("referential_integrity_rate",
                   (col("ri_validated_claims") / col("total_claims") * 100))
        .withColumn("assessment_date", current_timestamp())
        .withColumn("target_quality_score", lit(TARGET_DATA_QUALITY_SCORE))
    )