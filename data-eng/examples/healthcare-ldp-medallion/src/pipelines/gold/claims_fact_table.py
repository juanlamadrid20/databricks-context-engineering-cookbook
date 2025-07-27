"""
Gold Layer: Claims Fact Table with Analytics
Implements a comprehensive claims fact table with pre-aggregated metrics, 
temporal dimensions, and foreign key relationships to patient dimension.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, coalesce, year, month, dayofweek,
    datediff, round as spark_round, sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, row_number, dense_rank
)
from pyspark.sql.window import Window

# Environment-aware configuration with Unity Catalog
CATALOG = spark.conf.get("CATALOG", "juan_dev")
SCHEMA = spark.conf.get("SCHEMA", "data_eng")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "dev")

# Critical path handling pattern (include in all pipeline files)
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = "/".join(notebook_path.split("/")[:-3])  # Go up 3 levels from gold to src
    sys.path.insert(0, f"{base_path}")
except:
    # Fallback to multiple common paths
    possible_paths = ["/Workspace/src", "/databricks/driver/src", "/repos/src"]
    for path in possible_paths:
        if os.path.exists(path):
            sys.path.insert(0, path)
            break

@dlt.table(
    name="fact_claims",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Claims fact table with pre-aggregated metrics, temporal dimensions, and comprehensive claims analytics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableChangeDataFeed": "true",  # Enable for downstream analytics
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.pii.fields": "patient_natural_key,provider_name_standardized",
        "compliance": "HIPAA",
        "data_classification": "PHI",
        "environment": PIPELINE_ENV,
        "dimensional_model": "fact_table",
        "business_domain": "insurance_claims_analytics"
    }
)
def fact_claims():
    """
    Create claims fact table with comprehensive analytics and dimensional relationships.
    
    CRITICAL IMPLEMENTATIONS:
    - Foreign key relationships to patient dimension
    - Pre-aggregated financial metrics
    - Temporal analysis and seasonality
    - Clinical outcome tracking
    - Performance optimization with partitioning
    """
    
    # Join silver claims with patient dimension for surrogate keys
    silver_claims = dlt.read("silver_claims").alias("sc")
    dim_patients = dlt.read("dim_patients").alias("dp")
    
    # Create patient claim ranking window
    patient_claim_window = Window.partitionBy("patient_id").orderBy("claim_date_parsed")
    
    return (
        silver_claims
        .join(
            dim_patients.filter("is_current_record = true").select(
                "patient_natural_key", 
                "patient_surrogate_key",
                "health_risk_category",
                "patient_age_category",
                "patient_region"
            ),
            silver_claims.patient_id == dim_patients.patient_natural_key,
            "inner"  # Only claims with valid patient dimension records
        )
        
        # FACT TABLE KEYS
        .withColumn("claim_fact_key", 
                   row_number().over(Window.orderBy("claim_id")))
        .withColumn("claim_natural_key", col("claim_id"))
        .withColumn("patient_surrogate_key", col("patient_surrogate_key"))  # FK to dim_patients
        .withColumn("patient_natural_key", col("patient_id"))
        
        # TEMPORAL DIMENSIONS
        .withColumn("claim_date", col("claim_date_parsed"))
        .withColumn("claim_year", year(col("claim_date_parsed")))
        .withColumn("claim_month", month(col("claim_date_parsed")))
        .withColumn("claim_quarter", 
                   when(month(col("claim_date_parsed")).isin([1,2,3]), 1)
                   .when(month(col("claim_date_parsed")).isin([4,5,6]), 2)
                   .when(month(col("claim_date_parsed")).isin([7,8,9]), 3)
                   .otherwise(4))
        .withColumn("claim_day_of_week", dayofweek(col("claim_date_parsed")))
        .withColumn("claim_day_name",
                   when(col("claim_day_of_week") == 1, "SUNDAY")
                   .when(col("claim_day_of_week") == 2, "MONDAY")
                   .when(col("claim_day_of_week") == 3, "TUESDAY")
                   .when(col("claim_day_of_week") == 4, "WEDNESDAY")
                   .when(col("claim_day_of_week") == 5, "THURSDAY")
                   .when(col("claim_day_of_week") == 6, "FRIDAY")
                   .otherwise("SATURDAY"))
        .withColumn("is_weekend", 
                   col("claim_day_of_week").isin([1, 7]))  # Sunday = 1, Saturday = 7
        
        # FINANCIAL METRICS
        .withColumn("claim_amount", col("claim_amount_validated"))
        .withColumn("claim_amount_category", col("claim_amount_category"))
        
        # PROCESSING METRICS
        .withColumn("approval_date", col("approval_date_parsed"))
        .withColumn("payment_date", col("payment_date_parsed"))
        .withColumn("claim_processing_days", col("claim_processing_days"))
        .withColumn("payment_processing_days", col("payment_processing_days"))
        .withColumn("total_processing_days", col("total_processing_days"))
        .withColumn("processing_speed_category", col("processing_speed_category"))
        
        # CLAIM STATUS AND OUTCOMES
        .withColumn("claim_status", col("claim_status_standardized"))
        .withColumn("claim_type", col("claim_type_standardized"))
        .withColumn("claim_approved", 
                   col("claim_status").isin(["APPROVED", "PAID"]))
        .withColumn("claim_denied", 
                   col("claim_status") == "DENIED")
        .withColumn("claim_paid", 
                   col("claim_status") == "PAID")
        .withColumn("denial_reason", col("denial_reason_standardized"))
        
        # CLINICAL DIMENSIONS
        .withColumn("primary_diagnosis_code", col("diagnosis_code_standardized"))
        .withColumn("primary_procedure_code", col("procedure_code_standardized"))
        .withColumn("diagnosis_description", col("diagnosis_description_cleaned"))
        .withColumn("procedure_description", col("procedure_description_cleaned"))
        
        # PROVIDER DIMENSIONS
        .withColumn("provider_id", col("provider_id_standardized"))
        .withColumn("provider_name", col("provider_name_standardized"))
        
        # PATIENT CONTEXT (from dimension)
        .withColumn("patient_health_risk_category", col("health_risk_category"))
        .withColumn("patient_age_category", col("patient_age_category"))
        .withColumn("patient_region", col("patient_region"))
        
        # PATIENT CLAIM ANALYTICS
        .withColumn("patient_claim_sequence", 
                   row_number().over(patient_claim_window))
        .withColumn("is_first_claim", 
                   col("patient_claim_sequence") == 1)
        .withColumn("patient_total_claims_to_date",
                   count("*").over(patient_claim_window.rowsBetween(Window.unboundedPreceding, 0)))
        
        # CLAIM COMPLEXITY SCORING
        .withColumn("claim_complexity_score",
                   # Base complexity calculation
                   (when(col("claim_amount") > 10000, 10).otherwise(0) +
                    when(col("claim_processing_days") > 30, 5).otherwise(0) +
                    when(col("claim_type") == "INPATIENT", 15).otherwise(0) +
                    when(col("claim_type") == "SURGERY", 20).otherwise(0) +
                    when(col("denial_reason").isNotNull(), 10).otherwise(0)))
        .withColumn("claim_complexity_category",
                   when(col("claim_complexity_score") < 10, "SIMPLE")
                   .when(col("claim_complexity_score") < 25, "MODERATE")
                   .when(col("claim_complexity_score") < 40, "COMPLEX")
                   .otherwise("VERY_COMPLEX"))
        
        # SEASONAL AND TEMPORAL ANALYSIS
        .withColumn("claim_season",
                   when(col("claim_month").isin([12,1,2]), "WINTER")
                   .when(col("claim_month").isin([3,4,5]), "SPRING")
                   .when(col("claim_month").isin([6,7,8]), "SUMMER")
                   .otherwise("FALL"))
        .withColumn("days_since_claim", 
                   datediff(current_timestamp().cast("date"), col("claim_date_parsed")))
        .withColumn("claim_recency_category",
                   when(col("days_since_claim") <= 30, "RECENT")
                   .when(col("days_since_claim") <= 90, "MODERATE")
                   .when(col("days_since_claim") <= 365, "OLDER")
                   .otherwise("HISTORICAL"))
        
        # FINANCIAL ANALYTICS
        .withColumn("claim_amount_per_processing_day",
                   when(col("total_processing_days") > 0,
                        spark_round(col("claim_amount") / col("total_processing_days"), 2))
                   .otherwise(col("claim_amount")))
        
        # QUALITY AND COMPLIANCE
        .withColumn("claim_data_quality_score", col("data_quality_score"))
        .withColumn("referential_integrity_validated", col("referential_integrity_validated"))
        
        # AUDIT AND LINEAGE
        .withColumn("fact_created_timestamp", current_timestamp())
        .withColumn("source_system", lit("HEALTHCARE_DLT_PIPELINE"))
        .withColumn("data_lineage_pipeline_env", col("_pipeline_env"))
        .withColumn("data_lineage_silver_processing", col("_silver_processed_at"))
        
        # SELECT FINAL FACT TABLE COLUMNS
        .select(
            # Fact Keys
            "claim_fact_key",
            "claim_natural_key",
            
            # Dimension Foreign Keys
            "patient_surrogate_key",  # FK to dim_patients
            "patient_natural_key",
            
            # Temporal Dimensions
            "claim_date",
            "claim_year",
            "claim_month", 
            "claim_quarter",
            "claim_day_of_week",
            "claim_day_name",
            "claim_season",
            "is_weekend",
            
            # Financial Measures
            "claim_amount",
            "claim_amount_category",
            "claim_amount_per_processing_day",
            
            # Processing Measures
            "approval_date",
            "payment_date",
            "claim_processing_days",
            "payment_processing_days", 
            "total_processing_days",
            "processing_speed_category",
            
            # Claim Attributes
            "claim_status",
            "claim_type",
            "claim_approved",
            "claim_denied",
            "claim_paid",
            "denial_reason",
            
            # Clinical Attributes
            "primary_diagnosis_code",
            "primary_procedure_code",
            "diagnosis_description",
            "procedure_description",
            
            # Provider Attributes
            "provider_id",
            "provider_name",
            
            # Patient Context
            "patient_health_risk_category",
            "patient_age_category",
            "patient_region",
            
            # Patient Analytics
            "patient_claim_sequence",
            "is_first_claim",
            "patient_total_claims_to_date",
            
            # Complexity Analysis
            "claim_complexity_score",
            "claim_complexity_category",
            
            # Temporal Analysis
            "days_since_claim",
            "claim_recency_category",
            
            # Quality Measures
            "claim_data_quality_score",
            "referential_integrity_validated",
            
            # Audit Columns
            "fact_created_timestamp",
            "source_system",
            "data_lineage_pipeline_env",
            "data_lineage_silver_processing"
        )
    )

@dlt.table(
    name="fact_claims_monthly_summary",
    comment="Monthly claims analytics summary for executive dashboards and trending analysis"
)
def fact_claims_monthly_summary():
    """
    Generate monthly claims summary metrics for business intelligence dashboards.
    """
    
    return (
        dlt.read("fact_claims")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .groupBy(
            "claim_year", 
            "claim_month", 
            "claim_quarter",
            "patient_region",
            "claim_type",
            "patient_health_risk_category"
        )
        .agg(
            # Volume Metrics
            count("*").alias("total_claims"),
            countDistinct("patient_natural_key").alias("unique_patients"),
            
            # Financial Metrics
            spark_sum("claim_amount").alias("total_claim_amount"),
            avg("claim_amount").alias("avg_claim_amount"),
            spark_min("claim_amount").alias("min_claim_amount"),
            spark_max("claim_amount").alias("max_claim_amount"),
            
            # Outcome Metrics
            spark_sum(when(col("claim_approved") == True, 1).otherwise(0)).alias("approved_claims"),
            spark_sum(when(col("claim_denied") == True, 1).otherwise(0)).alias("denied_claims"),
            spark_sum(when(col("claim_paid") == True, 1).otherwise(0)).alias("paid_claims"),
            
            # Processing Metrics
            avg("claim_processing_days").alias("avg_processing_days"),
            avg("total_processing_days").alias("avg_total_processing_days"),
            
            # Quality Metrics
            avg("claim_data_quality_score").alias("avg_data_quality_score")
        )
        .withColumn("approval_rate", 
                   spark_round((col("approved_claims") / col("total_claims") * 100), 2))
        .withColumn("denial_rate",
                   spark_round((col("denied_claims") / col("total_claims") * 100), 2))
        .withColumn("payment_rate",
                   spark_round((col("paid_claims") / col("total_claims") * 100), 2))
        .withColumn("claims_per_patient",
                   spark_round((col("total_claims") / col("unique_patients")), 2))
        .withColumn("summary_created_at", current_timestamp())
        .withColumn("year_month", concat(col("claim_year"), lit("-"), 
                                       lpad(col("claim_month"), 2, "0")))
    )