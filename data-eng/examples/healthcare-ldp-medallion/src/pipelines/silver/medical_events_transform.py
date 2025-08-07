"""
Silver Layer: Medical Events Transformation  
Data standardization, clinical validation, and care coordination metrics for medical events.
Implements comprehensive medical events processing with provider analytics.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, datediff, current_date,
    length, upper, coalesce, rand, round as spark_round
)
from pyspark.sql.types import IntegerType

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
    name="silver_medical_events",
    comment="Standardized medical events with clinical validation and care coordination metrics",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "data_classification": "Clinical",
        "compliance": "HIPAA"
    }
)
@dlt.expect_all_or_drop({
    "valid_event_id": "event_id IS NOT NULL AND LENGTH(event_id) >= 5",
    "valid_patient_reference": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "valid_event_date": "event_date IS NOT NULL",
    "valid_event_type": "event_type_standardized IS NOT NULL",
    "valid_medical_provider": "medical_provider IS NOT NULL"
})
@dlt.expect_all({
    "temporal_validity": "days_since_event >= 0",
    "provider_identification": "LENGTH(medical_provider) >= 3",
    "event_data_quality": "event_data_quality_score >= 0.8"
})
def silver_medical_events():
    """
    CRITICAL: Uses dlt.read() with simple table name for bronze layer dependency.
    Implements medical events standardization and clinical analytics.
    """
    bronze_events = dlt.read("bronze_medical_events")  # CORRECT: Simple table name
    
    return (
        bronze_events
        .filter(col("event_id").isNotNull() & col("patient_id").isNotNull())
        .withColumn("processed_at", current_timestamp())
        
        # Event type standardization
        .withColumn("event_type_standardized",
                   upper(col("event_type")))
        
        # Provider standardization
        .withColumn("provider_standardized",
                   upper(col("medical_provider")))
        
        # Temporal calculations
        .withColumn("days_since_event",
                   datediff(current_date(), col("event_date")))
        
        # Clinical event categorization
        .withColumn("emergency_visit",
                   col("event_type_standardized") == "EMERGENCY")
        
        .withColumn("preventive_care_indicator",
                   col("event_type_standardized").isin("CHECKUP", "CONSULTATION"))
        
        .withColumn("acute_care_indicator", 
                   col("event_type_standardized").isin("EMERGENCY", "SURGERY", "TREATMENT"))
        
        .withColumn("hospital_admission",
                   col("event_type_standardized").isin("SURGERY", "EMERGENCY"))
        
        # Provider type inference (simplified)
        .withColumn("provider_type",
                   when(col("provider_standardized").contains("HOSPITAL"), "HOSPITAL")
                   .when(col("provider_standardized").contains("CLINIC"), "CLINIC")
                   .when(col("provider_standardized").contains("EMERGENCY"), "EMERGENCY")
                   .when(col("provider_standardized").contains("URGENT"), "URGENT_CARE")
                   .otherwise("GENERAL_PRACTICE"))
        
        .withColumn("facility_type",
                   when(col("provider_type").isin("HOSPITAL", "EMERGENCY"), "INPATIENT")
                   .otherwise("OUTPATIENT"))
        
        # Clinical outcome simulation (for demo purposes)
        .withColumn("clinical_outcome_score",
                   spark_round((rand() * 40 + 10), 1))  # Random score 10-50
        
        .withColumn("care_efficiency_score",
                   spark_round((rand() * 30 + 70), 1))  # Random score 70-100
        
        .withColumn("care_appropriateness_score",
                   spark_round((rand() * 25 + 75), 1))  # Random score 75-100
        
        # Care coordination metrics
        .withColumn("visit_duration_minutes", 
                   when(col("emergency_visit"), lit(180))
                   .when(col("event_type_standardized") == "SURGERY", lit(240))
                   .when(col("event_type_standardized") == "CHECKUP", lit(30))
                   .otherwise(lit(45)))
        
        .withColumn("follow_up_required",
                   col("event_type_standardized").isin("SURGERY", "TREATMENT", "EMERGENCY"))
        
        .withColumn("chronic_management_indicator",
                   col("event_type_standardized").isin("TREATMENT", "CONSULTATION"))
        
        # Provider relationship metrics
        .withColumn("is_new_provider", 
                   rand() > 0.7)  # 30% chance new provider
        
        .withColumn("provider_patient_familiarity",
                   when(col("is_new_provider"), lit(1))
                   .otherwise(spark_round((rand() * 4 + 1), 0)))  # 1-5 scale
        
        # Data quality scoring
        .withColumn("event_data_quality_score",
                   (when(col("event_id").isNotNull() & (length(col("event_id")) >= 5), 0.25).otherwise(0) +
                    when(col("patient_id").isNotNull() & (length(col("patient_id")) >= 5), 0.25).otherwise(0) +
                    when(col("event_date").isNotNull(), 0.2).otherwise(0) +
                    when(col("event_type").isNotNull(), 0.15).otherwise(0) +
                    when(col("medical_provider").isNotNull() & (length(col("medical_provider")) >= 3), 0.15).otherwise(0)))
        
        # Days calculations for care gaps
        # .withColumn("days_to_next_event", lit(None))  # Placeholder for window function in gold layer
        # .withColumn("days_since_previous_event", lit(None))  # Placeholder for window function in gold layer
        .withColumn("days_to_next_event", lit(None).cast(IntegerType()))
.withColumn("days_since_previous_event", lit(None).cast(IntegerType()))
        
        .select(
            col("event_id"),
            col("patient_id"),
            col("event_date"),
            col("event_type_standardized"),
            col("provider_standardized").alias("medical_provider"),
            
            # Clinical attributes
            col("emergency_visit"),
            col("preventive_care_indicator"),
            col("acute_care_indicator"),
            col("hospital_admission"),
            col("provider_type"),
            col("facility_type"),
            
            # Clinical metrics
            col("clinical_outcome_score"),
            col("care_efficiency_score"),
            col("care_appropriateness_score"),
            col("visit_duration_minutes"),
            col("follow_up_required"),
            col("chronic_management_indicator"),
            
            # Provider metrics
            col("is_new_provider"),
            col("provider_patient_familiarity"),
            col("days_since_event"),
            col("days_to_next_event"),
            col("days_since_previous_event"),
            col("event_data_quality_score"),
            
            # Pipeline metadata
            col("_ingested_at"),
            col("_pipeline_env"), 
            col("_file_name"),
            col("processed_at")
        )
    )

print("✅ Silver medical events transformation pipeline loaded successfully")