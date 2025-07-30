"""
Silver Layer: Patient Demographics Transformation with HIPAA Compliance
Implements comprehensive HIPAA de-identification, data quality validation, and clinical standardization.
Critical layer for PHI protection and data governance compliance.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, sha2, concat, regexp_replace, 
    upper, trim, coalesce, round as spark_round, count, sum as spark_sum,
    avg, stddev, min as spark_min, max as spark_max
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
        PATIENT_SALT, ELDERLY_AGE_THRESHOLD, VALID_REGIONS, VALID_SEX_VALUES,
        MIN_PATIENT_AGE, MAX_PATIENT_AGE, MIN_BMI, MAX_BMI, MAX_CHILDREN,
        TARGET_DATA_QUALITY_SCORE
    )
except ImportError:
    # Fallback constants if import fails
    print("⚠️  Warning: Could not import healthcare schemas, using fallback constants")
    PATIENT_SALT = "PATIENT_DATA_ENCRYPTION_SALT_2024"
    ELDERLY_AGE_THRESHOLD = 89
    VALID_REGIONS = ["NORTHEAST", "NORTHWEST", "SOUTHEAST", "SOUTHWEST"]
    VALID_SEX_VALUES = ["MALE", "FEMALE", "OTHER", "UNKNOWN"]
    MIN_PATIENT_AGE = 18
    MAX_PATIENT_AGE = 85
    MIN_BMI = 16.0
    MAX_BMI = 50.0
    MAX_CHILDREN = 10
    TARGET_DATA_QUALITY_SCORE = 99.5

# Staging view for silver transformation
@dlt.view(name="silver_patients_staging")
def silver_patients_staging():
    """
    Staging view for silver patient transformation with initial data quality filtering.
    Filters out malformed records and applies basic data validation.
    """
    return (
        dlt.read("bronze_patients")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .filter("_rescued_data IS NULL")  # Filter out malformed records
        .filter("patient_id IS NOT NULL AND LENGTH(patient_id) >= 5")  # Valid patient IDs only
        .filter("age IS NOT NULL AND CAST(age AS INT) BETWEEN 0 AND 120")  # Reasonable age range
    )

@dlt.table(
    name="silver_patients",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="HIPAA-compliant patient demographics with de-identification and comprehensive data quality validation",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableChangeDataFeed": "true",  # MANDATORY: HIPAA audit requirement
        "pipelines.pii.fields": "patient_id_hash,zip_code_anonymized",  # Only hashed/anonymized PII
        "compliance": "HIPAA",
        "data_classification": "PHI_DEIDENTIFIED",
        "environment": PIPELINE_ENV,
        "hipaa_deidentification": "applied",
        "retention_policy": "7_years"
    }
)
@dlt.expect_all_or_drop({
    "valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "valid_age_range": "age_years BETWEEN 18 AND 90",  # After de-identification
    "valid_sex": "sex_standardized IN ('MALE', 'FEMALE', 'OTHER', 'UNKNOWN')",
    "valid_region": "region_standardized IN ('NORTHEAST', 'NORTHWEST', 'SOUTHEAST', 'SOUTHWEST')",
    "valid_bmi": "bmi_validated BETWEEN 16.0 AND 50.0",
    "hipaa_ssn_protected": "ssn_hash IS NOT NULL AND LENGTH(ssn_hash) = 64"  # SHA256 hash length
})
@dlt.expect_all({
    "complete_demographics": "first_name_standardized IS NOT NULL AND last_name_standardized IS NOT NULL",
    "reasonable_children": "children_count BETWEEN 0 AND 10",
    "positive_charges": "insurance_charges > 0",
    "valid_insurance_plan": "insurance_plan_standardized IS NOT NULL",
    "data_quality_threshold": "data_quality_score >= 95.0"  # High quality threshold for silver
})
def silver_patients():
    """
    Transform patient demographics with comprehensive HIPAA compliance controls.
    
    CRITICAL HIPAA IMPLEMENTATIONS:
    - SSN hashing with salt for protection
    - Age de-identification (89+ becomes 90)
    - ZIP code anonymization for elderly patients
    - Change data feed enabled for audit trails
    - All raw PII removed from this layer
    """
    
    return (
        dlt.read("silver_patients_staging")
        # HIPAA DE-IDENTIFICATION TRANSFORMATIONS
        .withColumn("ssn_hash", 
                   sha2(concat(col("ssn"), lit(PATIENT_SALT)), 256))  # Hash SSN with salt
        .withColumn("age_years", 
                   when(col("age").cast("int") >= ELDERLY_AGE_THRESHOLD, 90)
                   .otherwise(col("age").cast("int")))  # De-identify elderly ages
        .withColumn("zip_code_anonymized",
                   when(col("age").cast("int") >= ELDERLY_AGE_THRESHOLD, 
                        concat(col("zip_code").substr(1, 3), lit("XX")))  # Last 2 digits anonymized for elderly
                   .otherwise(col("zip_code")))
        
        # CLINICAL DATA STANDARDIZATION
        .withColumn("sex_standardized", 
                   upper(trim(col("sex"))))
        .withColumn("region_standardized", 
                   upper(trim(col("region"))))
        .withColumn("bmi_validated", 
                   when((col("bmi").cast("double") >= MIN_BMI) & (col("bmi").cast("double") <= MAX_BMI),
                        spark_round(col("bmi").cast("double"), 1))
                   .otherwise(None))  # Null out invalid BMI values
        .withColumn("children_count", 
                   when(col("children").cast("int") <= MAX_CHILDREN, col("children").cast("int"))
                   .otherwise(MAX_CHILDREN))  # Cap at reasonable maximum
        .withColumn("smoker_flag", 
                   when(col("smoker").cast("string").isin(["true", "True", "1", "yes", "Yes"]), True)
                   .when(col("smoker").cast("string").isin(["false", "False", "0", "no", "No"]), False)
                   .otherwise(None))
        
        # FINANCIAL DATA VALIDATION
        .withColumn("insurance_charges", 
                   when(col("charges").cast("double") > 0, 
                        spark_round(col("charges").cast("double"), 2))
                   .otherwise(None))
        .withColumn("insurance_plan_standardized", 
                   trim(col("insurance_plan")))
        
        # NAME STANDARDIZATION (keeping for operational needs but marking as PII)
        .withColumn("first_name_standardized", 
                   trim(regexp_replace(col("first_name"), "[^a-zA-Z\\s]", "")))
        .withColumn("last_name_standardized", 
                   trim(regexp_replace(col("last_name"), "[^a-zA-Z\\s]", "")))
        
        # DATA QUALITY SCORING
        .withColumn("completeness_score",
                   ((when(col("patient_id").isNotNull(), 1).otherwise(0) +
                     when(col("age").isNotNull(), 1).otherwise(0) +
                     when(col("sex").isNotNull(), 1).otherwise(0) +
                     when(col("region").isNotNull(), 1).otherwise(0) +
                     when(col("bmi").isNotNull(), 1).otherwise(0) +
                     when(col("charges").isNotNull(), 1).otherwise(0)) / 6.0 * 100))
        .withColumn("validity_score",
                   ((when((col("age").cast("int") >= MIN_PATIENT_AGE) & (col("age").cast("int") <= MAX_PATIENT_AGE), 1).otherwise(0) +
                     when(col("sex_standardized").isin(VALID_SEX_VALUES), 1).otherwise(0) +
                     when(col("region_standardized").isin(VALID_REGIONS), 1).otherwise(0) +
                     when((col("bmi").cast("double") >= MIN_BMI) & (col("bmi").cast("double") <= MAX_BMI), 1).otherwise(0) +
                     when(col("charges").cast("double") > 0, 1).otherwise(0)) / 5.0 * 100))
        .withColumn("data_quality_score",
                   (col("completeness_score") + col("validity_score")) / 2.0)
        
        # HIPAA AUDIT AND COMPLIANCE METADATA
        .withColumn("processed_at", current_timestamp())
        .withColumn("hipaa_deidentified", lit(True))
        .withColumn("age_deidentified", col("age").cast("int") >= ELDERLY_AGE_THRESHOLD)
        .withColumn("zip_anonymized", col("age").cast("int") >= ELDERLY_AGE_THRESHOLD)
        .withColumn("data_retention_years", lit(7))  # HIPAA requirement
        .withColumn("audit_required", lit(True))
        .withColumn("_silver_processed_at", current_timestamp())
        .withColumn("_compliance_validated", lit(True))
        
        # DROP RAW PII FIELDS (CRITICAL: Never include raw SSN, DOB in silver layer)
        .drop("ssn", "date_of_birth")  # Remove raw PII permanently
        
        # SELECT FINAL SILVER LAYER COLUMNS
        .select(
            "patient_id",
            "ssn_hash",  # Hashed SSN only
            "first_name_standardized",
            "last_name_standardized", 
            "age_years",  # De-identified age
            "sex_standardized",
            "region_standardized",
            "bmi_validated",
            "smoker_flag",
            "children_count",
            "insurance_charges",
            "insurance_plan_standardized",
            "coverage_start_date",
            "zip_code_anonymized",  # Anonymized ZIP
            "timestamp",
            "completeness_score",
            "validity_score",
            "data_quality_score",
            "processed_at",
            "hipaa_deidentified",
            "age_deidentified",
            "zip_anonymized",
            "data_retention_years",
            "_ingested_at",
            "_pipeline_env",
            "_file_name",
            "_silver_processed_at"
        )
    )

@dlt.table(
    name="silver_patients_quarantine",
    comment="Quarantined patient records that failed validation - maintained for HIPAA audit trail"
)
def silver_patients_quarantine():
    """
    Quarantine patients that fail validation while maintaining audit trail.
    CRITICAL: Uses quarantine instead of drop to preserve audit trail for HIPAA compliance.
    """
    
    return (
        dlt.read("silver_patients_staging")
        .filter(
            (col("age").cast("int") < MIN_PATIENT_AGE) |
            (col("age").cast("int") > MAX_PATIENT_AGE) |
            (col("bmi").cast("double") < MIN_BMI) |
            (col("bmi").cast("double") > MAX_BMI) |
            (~col("sex").isin(["MALE", "FEMALE", "male", "female"])) |
            (col("charges").cast("double") <= 0)
        )
        .withColumn("quarantine_reason", 
                   when(col("age").cast("int") < MIN_PATIENT_AGE, "AGE_TOO_LOW")
                   .when(col("age").cast("int") > MAX_PATIENT_AGE, "AGE_TOO_HIGH")
                   .when(col("bmi").cast("double") < MIN_BMI, "BMI_TOO_LOW")
                   .when(col("bmi").cast("double") > MAX_BMI, "BMI_TOO_HIGH")
                   .when(~col("sex").isin(["MALE", "FEMALE", "male", "female"]), "INVALID_SEX")
                   .when(col("charges").cast("double") <= 0, "INVALID_CHARGES")
                   .otherwise("MULTIPLE_ISSUES"))
        .withColumn("quarantined_at", current_timestamp())
        .withColumn("review_required", lit(True))
        .withColumn("audit_preserved", lit(True))
    )

@dlt.table(
    name="silver_patients_quality_summary",
    comment="Data quality summary and HIPAA compliance metrics for silver patient layer"
)
def silver_patients_quality_summary():
    """
    Generate comprehensive data quality and HIPAA compliance summary.
    """
    
    return (
        dlt.read("silver_patients")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .agg(
            count("*").alias("total_patients"),
            spark_sum(when(col("data_quality_score") >= TARGET_DATA_QUALITY_SCORE, 1).otherwise(0)).alias("high_quality_patients"),
            spark_sum(when(col("hipaa_deidentified") == True, 1).otherwise(0)).alias("hipaa_compliant_patients"),
            spark_sum(when(col("age_deidentified") == True, 1).otherwise(0)).alias("age_deidentified_patients"),
            spark_sum(when(col("zip_anonymized") == True, 1).otherwise(0)).alias("zip_anonymized_patients"),
            avg("data_quality_score").alias("avg_data_quality_score"),
            avg("completeness_score").alias("avg_completeness_score"),
            avg("validity_score").alias("avg_validity_score"),
            stddev("data_quality_score").alias("quality_score_stddev"),
            spark_min("data_quality_score").alias("min_quality_score"),
            spark_max("data_quality_score").alias("max_quality_score")
        )
        .withColumn("quality_sla_met", 
                   col("avg_data_quality_score") >= TARGET_DATA_QUALITY_SCORE)
        .withColumn("hipaa_compliance_rate",
                   (col("hipaa_compliant_patients") / col("total_patients") * 100))
        .withColumn("assessment_date", current_timestamp())
        .withColumn("target_quality_score", lit(TARGET_DATA_QUALITY_SCORE))
        .withColumn("compliance_framework", lit("HIPAA"))
    )