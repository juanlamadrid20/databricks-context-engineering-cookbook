"""
Gold Layer: Patient 360 Dimension with SCD Type 2
Implements a comprehensive patient dimension table with Slowly Changing Dimensions Type 2 
for historical tracking and analytics-ready patient data.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, coalesce, row_number, max as spark_max,
    lag, lead, to_date, date_sub, date_add, desc, asc, sum as spark_sum,
    count, avg, round as spark_round
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
    name="dim_patients",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Patient 360 dimension with SCD Type 2 for historical tracking and comprehensive patient analytics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableChangeDataFeed": "true",  # Enable for downstream analytics
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.pii.fields": "patient_natural_key,first_name_standardized,last_name_standardized",
        "compliance": "HIPAA",
        "data_classification": "PHI_DEIDENTIFIED",
        "environment": PIPELINE_ENV,
        "dimensional_model": "scd_type_2",
        "business_domain": "patient_analytics"
    }
)
def dim_patients():
    """
    Create patient dimension table with SCD Type 2 implementation.
    
    CRITICAL IMPLEMENTATIONS:
    - Surrogate keys for dimensional modeling
    - Effective dates for historical tracking
    - Current record flags for active records
    - Patient risk categorization and analytics attributes
    - Complete patient 360 view for business intelligence
    """
    
    # Get the latest silver patients data
    silver_patients = dlt.read("silver_patients").alias("sp")
    
    # Generate surrogate key using row_number() over patient_id ordered by processed_at
    patient_window = Window.partitionBy("patient_id").orderBy(desc("processed_at"))
    
    return (
        silver_patients
        # SURROGATE KEY GENERATION
        .withColumn("patient_surrogate_key", 
                   row_number().over(Window.orderBy("patient_id", "processed_at")))
        .withColumn("patient_natural_key", col("patient_id"))  # Business key
        
        # SCD TYPE 2 EFFECTIVE DATING
        .withColumn("effective_from_date", 
                   coalesce(col("processed_at").cast("date"), current_timestamp().cast("date")))
        .withColumn("effective_to_date", 
                   lit("9999-12-31").cast("date"))  # Active records end date
        .withColumn("is_current_record", lit(True))
        .withColumn("scd_version", 
                   row_number().over(patient_window))
        
        # PATIENT DEMOGRAPHICS (HIPAA DE-IDENTIFIED)
        .withColumn("patient_first_name", col("first_name_standardized"))
        .withColumn("patient_last_name", col("last_name_standardized"))
        .withColumn("patient_age_category",
                   when(col("age_years") < 30, "YOUNG_ADULT")
                   .when(col("age_years") < 50, "MIDDLE_AGED") 
                   .when(col("age_years") < 65, "MATURE_ADULT")
                   .when(col("age_years") < 90, "SENIOR")
                   .otherwise("ELDERLY_DEIDENTIFIED"))
        .withColumn("patient_sex", col("sex_standardized"))
        .withColumn("patient_region", col("region_standardized"))
        
        # HEALTH METRICS AND RISK FACTORS
        .withColumn("patient_bmi_category",
                   when(col("bmi_validated") < 18.5, "UNDERWEIGHT")
                   .when(col("bmi_validated") < 25.0, "NORMAL")
                   .when(col("bmi_validated") < 30.0, "OVERWEIGHT")
                   .when(col("bmi_validated") < 35.0, "OBESE_CLASS_1")
                   .when(col("bmi_validated") < 40.0, "OBESE_CLASS_2")
                   .when(col("bmi_validated") >= 40.0, "OBESE_CLASS_3")
                   .otherwise("UNKNOWN"))
        .withColumn("patient_smoking_status", 
                   when(col("smoker_flag") == True, "SMOKER")
                   .when(col("smoker_flag") == False, "NON_SMOKER")
                   .otherwise("UNKNOWN"))
        .withColumn("patient_family_size_category",
                   when(col("children_count") == 0, "NO_DEPENDENTS")
                   .when(col("children_count") <= 2, "SMALL_FAMILY")
                   .when(col("children_count") <= 4, "MEDIUM_FAMILY")
                   .otherwise("LARGE_FAMILY"))
        
        # FINANCIAL PROFILE
        .withColumn("patient_premium_category",
                   when(col("insurance_charges") < 5000, "LOW_PREMIUM")
                   .when(col("insurance_charges") < 15000, "MEDIUM_PREMIUM")
                   .when(col("insurance_charges") < 30000, "HIGH_PREMIUM")
                   .otherwise("VERY_HIGH_PREMIUM"))
        .withColumn("patient_insurance_plan", col("insurance_plan_standardized"))
        .withColumn("patient_coverage_start_date", 
                   to_date(col("coverage_start_date"), "yyyy-MM-dd"))
        
        # PATIENT RISK SCORING
        .withColumn("health_risk_score",
                   # Base score calculation based on multiple factors
                   (when(col("age_years") > 65, 20).otherwise(0) +
                    when(col("smoker_flag") == True, 25).otherwise(0) +
                    when(col("bmi_validated") > 30, 15).otherwise(0) +
                    when(col("bmi_validated") < 18.5, 10).otherwise(0) +
                    when(col("insurance_charges") > 20000, 15).otherwise(0)))
        .withColumn("health_risk_category",
                   when(col("health_risk_score") < 20, "LOW_RISK")
                   .when(col("health_risk_score") < 40, "MODERATE_RISK")
                   .when(col("health_risk_score") < 60, "HIGH_RISK")
                   .otherwise("VERY_HIGH_RISK"))
        
        # DEMOGRAPHIC SEGMENT ANALYSIS
        .withColumn("demographic_segment",
                   concat_ws("_", 
                            col("patient_age_category"),
                            col("patient_sex"),
                            col("patient_region")))
        .withColumn("lifestyle_segment",
                   concat_ws("_",
                            col("patient_smoking_status"),
                            col("patient_bmi_category"),
                            col("patient_family_size_category")))
        
        # GEOGRAPHIC ANALYSIS (ANONYMIZED)
        .withColumn("patient_zip_code", col("zip_code_anonymized"))
        .withColumn("geographic_privacy_applied", col("zip_anonymized"))
        
        # DATA QUALITY AND COMPLETENESS
        .withColumn("patient_data_completeness_score", col("completeness_score"))
        .withColumn("patient_data_validity_score", col("validity_score"))
        .withColumn("patient_data_quality_score", col("data_quality_score"))
        .withColumn("patient_record_quality",
                   when(col("data_quality_score") >= 98, "EXCELLENT")
                   .when(col("data_quality_score") >= 95, "GOOD")
                   .when(col("data_quality_score") >= 90, "FAIR")
                   .otherwise("POOR"))
        
        # HIPAA COMPLIANCE TRACKING
        .withColumn("hipaa_deidentification_applied", col("hipaa_deidentified"))
        .withColumn("age_privacy_protection", col("age_deidentified"))
        .withColumn("geographic_privacy_protection", col("zip_anonymized"))
        .withColumn("data_retention_compliance", 
                   when(col("data_retention_years") == 7, True).otherwise(False))
        
        # AUDIT AND LINEAGE
        .withColumn("source_system", lit("HEALTHCARE_DLT_PIPELINE"))
        .withColumn("dimension_created_timestamp", current_timestamp())
        .withColumn("dimension_updated_timestamp", current_timestamp())
        .withColumn("data_lineage_pipeline_env", col("_pipeline_env"))
        .withColumn("data_lineage_ingestion_timestamp", col("_ingested_at"))
        .withColumn("data_lineage_silver_processing", col("_silver_processed_at"))
        
        # SELECT FINAL DIMENSION COLUMNS
        .select(
            # Dimension Keys
            "patient_surrogate_key",
            "patient_natural_key",
            
            # SCD Type 2 Attributes
            "effective_from_date",
            "effective_to_date", 
            "is_current_record",
            "scd_version",
            
            # Patient Demographics (De-identified)
            "patient_first_name",
            "patient_last_name",
            "patient_age_category",
            "patient_sex",
            "patient_region",
            "patient_zip_code",
            
            # Health Profile
            "patient_bmi_category",
            "patient_smoking_status", 
            "patient_family_size_category",
            "health_risk_score",
            "health_risk_category",
            
            # Financial Profile
            "patient_premium_category",
            "patient_insurance_plan",
            "patient_coverage_start_date",
            
            # Segmentation
            "demographic_segment",
            "lifestyle_segment",
            
            # Data Quality
            "patient_data_completeness_score",
            "patient_data_validity_score",
            "patient_data_quality_score",
            "patient_record_quality",
            
            # HIPAA Compliance
            "hipaa_deidentification_applied",
            "age_privacy_protection",
            "geographic_privacy_protection",
            "data_retention_compliance",
            
            # Audit and Lineage
            "source_system",
            "dimension_created_timestamp",
            "dimension_updated_timestamp",
            "data_lineage_pipeline_env",
            "data_lineage_ingestion_timestamp",
            "data_lineage_silver_processing"
        )
    )

@dlt.table(
    name="dim_patients_summary",
    comment="Patient dimension summary metrics for business intelligence dashboards"
)
def dim_patients_summary():
    """
    Generate summary metrics for patient dimension for BI dashboards.
    """
    
    return (
        dlt.read("dim_patients")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .filter("is_current_record = true")  # Only current records
        .groupBy("patient_region", "health_risk_category", "patient_age_category")
        .agg(
            count("*").alias("patient_count"),
            avg("health_risk_score").alias("avg_health_risk_score"),
            avg("patient_data_quality_score").alias("avg_data_quality"),
            spark_sum(when(col("hipaa_deidentification_applied") == True, 1).otherwise(0)).alias("hipaa_compliant_patients"),
            spark_sum(when(col("patient_smoking_status") == "SMOKER", 1).otherwise(0)).alias("smoker_count"),
            spark_sum(when(col("patient_bmi_category").isin(["OBESE_CLASS_1", "OBESE_CLASS_2", "OBESE_CLASS_3"]), 1).otherwise(0)).alias("obese_patient_count")
        )
        .withColumn("obesity_rate", 
                   spark_round((col("obese_patient_count") / col("patient_count") * 100), 2))
        .withColumn("smoking_rate",
                   spark_round((col("smoker_count") / col("patient_count") * 100), 2))
        .withColumn("hipaa_compliance_rate",
                   spark_round((col("hipaa_compliant_patients") / col("patient_count") * 100), 2))
        .withColumn("summary_created_at", current_timestamp())
    )