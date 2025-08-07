"""
Gold Layer: Claims Fact Table
Comprehensive claims analytics with patient dimension joins and financial metrics.
Supports Claims Cost Analysis, Monthly Financial Performance, and Executive KPI queries.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, year, month, quarter,
    concat, lpad, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    countDistinct, round as spark_round
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
    name="fact_claims",
    comment="Claims fact table with patient dimension joins and comprehensive financial metrics",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "dimensional_model": "claims_financials"
    }
)
def fact_claims():
    """
    CRITICAL: Uses dlt.read() with simple table names for silver layer dependencies.
    Implements claims fact table supporting Queries 2, 4, 7, 8 from business requirements.
    """
    silver_claims = dlt.read("silver_claims")  # CORRECT: Simple table name
    dim_patients = dlt.read("dim_patients")    # CORRECT: Simple table name
    
    return (
        silver_claims.alias("c")
        
        # Join with patient dimension for analytics
        .join(
            dim_patients.filter(col("is_current_record")).alias("p"),
            col("c.patient_id") == col("p.patient_natural_key"),
            "inner"
        )
        
        # Fact table surrogate key
        .withColumn("claim_surrogate_key", col("c.claim_id"))
        .withColumn("claim_natural_key", col("c.claim_id"))
        
        # Patient dimension foreign key
        .withColumn("patient_surrogate_key", col("p.patient_surrogate_key"))
        
        # Time dimension attributes for Query 4 support
        .withColumn("claim_year", year(col("c.claim_date")))
        .withColumn("claim_month", month(col("c.claim_date")))
        .withColumn("claim_quarter", quarter(col("c.claim_date")))
        .withColumn("year_month", concat(col("claim_year"), lit("-"), lpad(col("claim_month"), 2, "0")))
        
        # Financial metrics
        .withColumn("claim_amount", col("c.claim_amount_validated"))
        .withColumn("high_cost_claim_indicator", col("c.claim_amount_validated") > 10000)
        
        # Claim outcome metrics for Query 2 support
        .withColumn("claim_approved", col("c.claim_approved"))
        .withColumn("claim_denied", col("c.claim_denied"))
        .withColumn("claim_paid", col("c.claim_paid"))
        
        # Processing efficiency metrics
        .withColumn("total_processing_days", col("c.total_processing_days"))
        .withColumn("claim_processing_efficiency",
                   when(col("total_processing_days") <= 7, "FAST")
                   .when(col("total_processing_days") <= 14, "STANDARD")
                   .otherwise("SLOW"))
        
        # Risk-adjusted metrics for Query 2 support
        .withColumn("risk_adjusted_amount",
                   when(col("p.health_risk_category") == "HIGH_RISK", col("c.claim_amount_validated") * 0.9)
                   .when(col("p.health_risk_category") == "MODERATE_RISK", col("c.claim_amount_validated") * 0.95)
                   .otherwise(col("c.claim_amount_validated")))
        
        # Medical coding attributes
        .withColumn("diagnosis_code", col("c.diagnosis_code"))
        .withColumn("procedure_code", col("c.procedure_code"))
        .withColumn("medical_coding_complete", 
                   col("diagnosis_code").isNotNull() & col("procedure_code").isNotNull())
        
        # Patient context for analytics
        .withColumn("patient_health_risk_category", col("p.health_risk_category"))
        .withColumn("patient_age_category", col("p.patient_age_category"))
        .withColumn("patient_region", col("p.patient_region"))
        .withColumn("patient_smoking_status", col("p.patient_smoking_status"))
        .withColumn("patient_premium_category", col("p.patient_premium_category"))
        
        # Data quality metrics
        .withColumn("claim_data_quality_score", col("c.claim_data_quality_score"))
        .withColumn("combined_data_quality_score", 
                   spark_round((col("claim_data_quality_score") + col("p.patient_data_quality_score")) / 2, 3))
        
        # Fact table measures and dimensions
        .select(
            # Surrogate keys
            col("claim_surrogate_key"),
            col("claim_natural_key"), 
            col("patient_surrogate_key"),
            
            # Time dimensions
            col("claim_year"),
            col("claim_month"), 
            col("claim_quarter"),
            col("year_month"),
            col("c.claim_date"),
            
            # Financial measures
            col("claim_amount"),
            col("risk_adjusted_amount"),
            col("high_cost_claim_indicator"),
            
            # Claim outcome measures
            col("claim_approved"),
            col("claim_denied"),
            col("claim_paid"),
            col("total_processing_days"),
            col("claim_processing_efficiency"),
            
            # Medical coding dimensions
            col("diagnosis_code"),
            col("procedure_code"),
            col("medical_coding_complete"),
            
            # Patient context dimensions
            col("patient_health_risk_category"),
            col("patient_age_category"),
            col("patient_region"),
            col("patient_smoking_status"), 
            col("patient_premium_category"),
            
            # Data quality measures
            col("claim_data_quality_score"),
            col("combined_data_quality_score"),
            
            # Pipeline metadata
            col("c._pipeline_env"),
            col("c.processed_at").alias("fact_last_updated")
        )
    )

# Monthly claims summary for Query 4 trending analysis
@dlt.table(
    name="fact_claims_monthly_summary", 
    comment="Monthly aggregated claims metrics for financial performance trending",
    table_properties={
        "quality": "gold",
        "dimensional_model": "claims_monthly_trends"
    }
)
def fact_claims_monthly_summary():
    """
    Monthly aggregation of claims for Query 4 financial performance trending.
    """
    fact_claims = dlt.read("fact_claims")  # CORRECT: Simple table name
    
    return (
        fact_claims
        .groupBy("year_month", "claim_quarter", "patient_region")
        .agg(
            # Volume metrics
            count("*").alias("total_claims"),
            countDistinct("patient_surrogate_key").alias("unique_patients"),
            
            # Financial metrics  
            spark_sum("claim_amount").alias("total_claim_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
            spark_max("claim_amount").alias("max_claim_amount"),
            spark_min("claim_amount").alias("min_claim_amount"),
            
            # Outcome metrics
            spark_sum(when(col("claim_approved"), 1).otherwise(0)).alias("approved_claims"),
            spark_sum(when(col("claim_denied"), 1).otherwise(0)).alias("denied_claims"), 
            spark_sum(when(col("claim_paid"), 1).otherwise(0)).alias("paid_claims"),
            
            # Processing metrics
            avg("total_processing_days").alias("avg_processing_days"),
            avg("claim_data_quality_score").alias("avg_data_quality_score"),
            
            # High cost claims
            spark_sum(when(col("high_cost_claim_indicator"), 1).otherwise(0)).alias("high_cost_claims"),
            spark_sum(when(col("high_cost_claim_indicator"), col("claim_amount")).otherwise(0)).alias("high_cost_amount")
        )
        .withColumn("avg_claims_per_patient", 
                   spark_round(col("total_claims") / col("unique_patients"), 2))
        .withColumn("avg_cost_per_patient",
                   spark_round(col("total_claim_amount") / col("unique_patients"), 2))
    )

print("✅ Gold claims fact table pipelines loaded successfully")