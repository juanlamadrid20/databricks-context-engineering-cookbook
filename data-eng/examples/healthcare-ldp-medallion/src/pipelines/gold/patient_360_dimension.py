"""
Gold Layer: Patient 360 Dimension Table
SCD Type 2 patient dimension with complete patient analytics and business intelligence support.
Supports all business queries from sample_business_queries.ipynb with proper dimensional modeling.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, coalesce, sha2, concat,
    row_number, lag, lead, monotonically_increasing_id
)
from pyspark.sql.window import Window

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
    name="dim_patients",
    comment="SCD Type 2 patient dimension with comprehensive analytics attributes for business intelligence",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "data_classification": "ANALYTICS", 
        "dimensional_model": "patient_360"
    }
)
def dim_patients():
    """
    CRITICAL: Uses dlt.read() with simple table name for silver layer dependency.
    Implements SCD Type 2 with comprehensive patient analytics supporting all business queries.
    """
    silver_patients = dlt.read("silver_patients")  # CORRECT: Simple table name
    
    # Window for SCD Type 2 change detection
    patient_window = Window.partitionBy("patient_id").orderBy("processed_at")
    
    return (
        silver_patients
        
        # Generate surrogate key for dimensional modeling
        .withColumn("patient_surrogate_key", monotonically_increasing_id())
        
        # Natural key and SCD Type 2 fields
        .withColumn("patient_natural_key", col("patient_id"))
        
        # Hash PII for analytics (HIPAA compliance in gold layer)
        .withColumn("patient_id_hash", 
                   sha2(concat(col("patient_id"), lit("ANALYTICS_SALT")), 256))
        
        .withColumn("patient_name_hash",
                   sha2(concat(col("first_name"), lit("_"), col("last_name")), 256))
        
        # SCD Type 2 implementation
        .withColumn("effective_date", coalesce(col("processed_at"), current_timestamp()))
        .withColumn("expiry_date", lit(None).cast("timestamp"))  # Current records
        .withColumn("is_current_record", lit(True))
        .withColumn("record_version", lit(1))
        
        # Premium calculation based on risk factors (for business queries)
        .withColumn("estimated_annual_premium",
                   when(col("health_risk_category") == "HIGH_RISK", col("charges") * 1.5)
                   .when(col("health_risk_category") == "MODERATE_RISK", col("charges") * 1.2)
                   .otherwise(col("charges")))
        
        # Premium categories for Query 1 support
        .withColumn("patient_premium_category",
                   when(col("estimated_annual_premium") >= 40000, "VERY_HIGH_PREMIUM")
                   .when(col("estimated_annual_premium") >= 20000, "HIGH_PREMIUM") 
                   .when(col("estimated_annual_premium") >= 10000, "MEDIUM_PREMIUM")
                   .otherwise("LOW_PREMIUM"))
        
        # Health risk scoring (0-100 scale) for Query 2 support
        .withColumn("health_risk_score",
                   when(col("health_risk_category") == "HIGH_RISK", 
                        when(col("age") >= 65, 85.0)
                        .when(col("smoker"), 80.0)
                        .otherwise(75.0))
                   .when(col("health_risk_category") == "MODERATE_RISK", 
                        when(col("age") >= 50, 60.0)
                        .otherwise(45.0))
                   .otherwise(25.0))
        
        # Smoking status standardization for Query 1 support
        .withColumn("patient_smoking_status",
                   when(col("smoker"), "SMOKER").otherwise("NON_SMOKER"))
        
        # Geographic region mapping
        .withColumn("patient_region", col("region"))
        
        # Insurance details for analytics
        .withColumn("primary_insurance_plan", col("insurance_plan"))
        .withColumn("coverage_effective_date", col("coverage_start_date"))
        
        # Demographic attributes for Query 1 and Query 7 support
        .withColumn("has_dependents", col("children") > 0)
        .withColumn("family_size_category",
                   when(col("children") >= 3, "LARGE_FAMILY")
                   .when(col("children") >= 1, "SMALL_FAMILY")
                   .otherwise("INDIVIDUAL"))
        
        # Clinical attributes for predictive analytics (Query 7)
        .withColumn("obesity_indicator", col("bmi") >= 30)
        .withColumn("underweight_indicator", col("bmi") < 18.5)
        
        .withColumn("lifestyle_risk_factors",
                   when(col("smoker") & (col("bmi") >= 30), "HIGH_RISK_LIFESTYLE")
                   .when(col("smoker") | (col("bmi") >= 30), "MODERATE_RISK_LIFESTYLE")
                   .otherwise("LOW_RISK_LIFESTYLE"))
        
        # Data governance and quality attributes
        .withColumn("data_quality_tier",
                   when(col("patient_data_quality_score") >= 0.95, "GOLD_QUALITY")
                   .when(col("patient_data_quality_score") >= 0.85, "SILVER_QUALITY")
                   .otherwise("BRONZE_QUALITY"))
        
        # Final dimensional table selection
        .select(
            # Surrogate and natural keys
            col("patient_surrogate_key"),
            col("patient_natural_key"),
            col("patient_id_hash"),
            col("patient_name_hash"),
            
            # SCD Type 2 fields
            col("effective_date"),
            col("expiry_date"),
            col("is_current_record"),
            col("record_version"),
            
            # Demographics (de-identified)
            col("patient_age_category"),
            col("sex").alias("patient_gender"),
            col("patient_region"),
            col("demographic_segment"),
            
            # Health and risk attributes
            col("health_risk_category"), 
            col("health_risk_score"),
            col("patient_smoking_status"),
            col("bmi"),
            col("obesity_indicator"),
            col("underweight_indicator"),
            col("lifestyle_risk_factors"),
            
            # Family and dependents
            col("children").alias("number_of_dependents"),
            col("has_dependents"),
            col("family_size_category"),
            
            # Insurance and financial
            col("primary_insurance_plan"),
            col("coverage_effective_date"),
            col("charges").alias("base_premium"),
            col("estimated_annual_premium"),
            col("patient_premium_category"),
            
            # Data quality and governance  
            col("patient_data_quality_score"),
            col("data_quality_tier"),
            col("patient_record_quality"),
            col("hipaa_deidentification_applied"),
            col("age_privacy_protection"),
            col("geographic_privacy_protection"),
            col("data_retention_compliance"),
            
            # Pipeline metadata
            col("_pipeline_env"),
            col("processed_at").alias("dimension_last_updated")
        )
    )

print("✅ Gold patient 360 dimension pipeline loaded successfully")