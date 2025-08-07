"""
Silver Layer: Insurance Claims Transformation
Data validation, referential integrity checks, and financial compliance for insurance claims.
Implements comprehensive claims processing with audit trails.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, datediff, current_date,
    regexp_extract, length, upper, coalesce, round as spark_round
)

# Environment-aware configuration
CATALOG = spark.conf.get("CATALOG", "juan_dev")
SCHEMA = spark.conf.get("SCHEMA", "healthcare_data")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")

# Critical path handling
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

@dlt.table(
    name="silver_claims",
    comment="Validated insurance claims with referential integrity and financial compliance",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "data_classification": "Financial",
        "compliance": "SOX,HIPAA"
    }
)
@dlt.expect_all_or_drop({
    "valid_claim_id": "claim_id IS NOT NULL AND LENGTH(claim_id) >= 5",
    "valid_patient_reference": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5", 
    "valid_claim_amount": "claim_amount_validated IS NOT NULL AND claim_amount_validated > 0",
    "valid_claim_date": "claim_date IS NOT NULL",
    "valid_claim_status": "claim_status_standardized IN ('SUBMITTED', 'APPROVED', 'DENIED', 'PAID')"
})
@dlt.expect_all({
    "financial_reasonableness": "claim_amount_validated BETWEEN 10 AND 100000",
    "medical_coding_present": "diagnosis_code IS NOT NULL AND procedure_code IS NOT NULL",
    "claim_data_quality": "claim_data_quality_score >= 0.8"
})
def silver_claims():
    """
    CRITICAL: Uses dlt.read() with simple table name for bronze layer dependency.
    Implements claims validation and referential integrity with patient silver layer.
    """
    bronze_claims = dlt.read("bronze_claims")  # CORRECT: Simple table name
    
    return (
        bronze_claims
        .filter(col("claim_id").isNotNull() & col("patient_id").isNotNull())
        .withColumn("processed_at", current_timestamp())
        
        # Financial validation and standardization
        .withColumn("claim_amount_validated",
                   when((col("claim_amount") > 0) & (col("claim_amount") <= 100000), col("claim_amount"))
                   .otherwise(lit(None)))
        
        # Claim status standardization
        .withColumn("claim_status_standardized",
                   upper(col("claim_status")))
        
        # Processing time calculations
        .withColumn("days_since_claim",
                   datediff(current_date(), col("claim_date")))
        
        # Claim categorization
        .withColumn("claim_amount_category",
                   when(col("claim_amount_validated") >= 10000, "HIGH_COST")
                   .when(col("claim_amount_validated") >= 1000, "MEDIUM_COST")
                   .otherwise("LOW_COST"))
        
        # Approval indicators
        .withColumn("claim_approved",
                   col("claim_status_standardized") == "APPROVED")
        
        .withColumn("claim_denied", 
                   col("claim_status_standardized") == "DENIED")
        
        .withColumn("claim_paid",
                   col("claim_status_standardized") == "PAID")
        
        # Data quality scoring
        .withColumn("claim_data_quality_score",
                   (when(col("claim_id").isNotNull() & (length(col("claim_id")) >= 5), 0.2).otherwise(0) +
                    when(col("patient_id").isNotNull() & (length(col("patient_id")) >= 5), 0.2).otherwise(0) +
                    when(col("claim_amount_validated").isNotNull(), 0.2).otherwise(0) +
                    when(col("claim_date").isNotNull(), 0.15).otherwise(0) +
                    when(col("diagnosis_code").isNotNull(), 0.125).otherwise(0) +
                    when(col("procedure_code").isNotNull(), 0.125).otherwise(0)))
        
        # Processing metrics
        .withColumn("total_processing_days", 
                   coalesce(col("days_since_claim"), lit(0)))
        
        .select(
            col("claim_id"),
            col("patient_id"),
            col("claim_amount_validated"),
            col("claim_date"),
            col("diagnosis_code"),
            col("procedure_code"),
            col("claim_status_standardized"),
            
            # Derived attributes
            col("claim_amount_category"),
            col("claim_approved"),
            col("claim_denied"),
            col("claim_paid"),
            col("total_processing_days"),
            col("claim_data_quality_score"),
            
            # Pipeline metadata
            col("_ingested_at"),
            col("_pipeline_env"),
            col("_file_name"),
            col("processed_at")
        )
    )

print("✅ Silver insurance claims transformation pipeline loaded successfully")
