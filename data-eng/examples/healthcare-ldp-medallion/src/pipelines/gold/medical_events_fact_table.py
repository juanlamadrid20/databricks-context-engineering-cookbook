"""
Gold Layer: Medical Events Fact Table
Comprehensive medical events analytics with provider performance and care coordination metrics.
Supports Healthcare Utilization, Provider Performance, and Predictive Analytics queries.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, year, month, quarter,
    sum as spark_sum, count, avg, countDistinct, round as spark_round
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
    name="fact_medical_events",
    comment="Medical events fact table with provider analytics and care coordination metrics",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true",
        "dimensional_model": "medical_events_clinical"
    }
)
def fact_medical_events():
    """
    CRITICAL: Uses dlt.read() with simple table names for silver layer dependencies.
    Implements medical events fact table supporting Queries 3, 5, 7 from business requirements.
    """
    silver_events = dlt.read("silver_medical_events")  # CORRECT: Simple table name
    dim_patients = dlt.read("dim_patients")           # CORRECT: Simple table name
    
    return (
        silver_events.alias("e")
        
        # Join with patient dimension for analytics
        .join(
            dim_patients.filter(col("is_current_record")).alias("p"),
            col("e.patient_id") == col("p.patient_natural_key"),
            "inner"
        )
        
        # Fact table surrogate key
        .withColumn("event_surrogate_key", col("e.event_id"))
        .withColumn("event_natural_key", col("e.event_id"))
        
        # Patient dimension foreign key
        .withColumn("patient_surrogate_key", col("p.patient_surrogate_key"))
        .withColumn("patient_natural_key", col("e.patient_id"))
        
        # Time dimension attributes
        .withColumn("event_year", year(col("e.event_date")))
        .withColumn("event_month", month(col("e.event_date")))
        .withColumn("event_quarter", quarter(col("e.event_date")))
        
        # Clinical measures for Query 3 support
        .withColumn("total_medical_events", lit(1))  # For aggregation
        .withColumn("emergency_visit", col("e.emergency_visit"))
        .withColumn("hospital_admission", col("e.hospital_admission"))
        .withColumn("preventive_care_indicator", col("e.preventive_care_indicator"))
        .withColumn("acute_care_indicator", col("e.acute_care_indicator"))
        
        # Provider performance measures for Query 5 support
        .withColumn("medical_provider", col("e.medical_provider"))
        .withColumn("provider_type", col("e.provider_type"))
        .withColumn("facility_type", col("e.facility_type"))
        .withColumn("clinical_outcome_score", col("e.clinical_outcome_score"))
        .withColumn("care_efficiency_score", col("e.care_efficiency_score"))
        .withColumn("care_appropriateness_score", col("e.care_appropriateness_score"))
        
        # Care coordination metrics for Query 3 support
        .withColumn("visit_duration_minutes", col("e.visit_duration_minutes"))
        .withColumn("follow_up_required", col("e.follow_up_required"))
        .withColumn("chronic_management_indicator", col("e.chronic_management_indicator"))
        .withColumn("is_new_provider", col("e.is_new_provider"))
        .withColumn("provider_patient_familiarity", col("e.provider_patient_familiarity"))
        
        # Care gap metrics (simplified for this implementation)
        .withColumn("days_since_previous_event", col("e.days_since_previous_event"))
        .withColumn("days_to_next_event", col("e.days_to_next_event"))
        
        # Patient context for Query 3 utilization analysis
        .withColumn("patient_health_risk_category", col("p.health_risk_category"))
        .withColumn("patient_age_category", col("p.patient_age_category"))
        .withColumn("patient_region", col("p.patient_region"))
        .withColumn("demographic_segment", col("p.demographic_segment"))
        
        # Data quality measures
        .withColumn("event_data_quality_score", col("e.event_data_quality_score"))
        
        # Fact table selection
        .select(
            # Surrogate keys
            col("event_surrogate_key"),
            col("event_natural_key"),
            col("patient_surrogate_key"),
            col("patient_natural_key"),
            
            # Time dimensions
            col("event_year"),
            col("event_month"),
            col("event_quarter"),
            col("e.event_date"),
            
            # Event measures
            col("total_medical_events"),
            col("emergency_visit"),
            col("hospital_admission"),
            col("preventive_care_indicator"),
            col("acute_care_indicator"),
            
            # Provider dimensions and measures
            col("medical_provider"),
            col("provider_type"),
            col("facility_type"),
            col("clinical_outcome_score"),
            col("care_efficiency_score"), 
            col("care_appropriateness_score"),
            
            # Care coordination measures
            col("visit_duration_minutes"),
            col("follow_up_required"),
            col("chronic_management_indicator"),
            col("is_new_provider"),
            col("provider_patient_familiarity"),
            col("days_since_previous_event"),
            col("days_to_next_event"),
            
            # Patient context dimensions
            col("patient_health_risk_category"),
            col("patient_age_category"),
            col("patient_region"),
            col("demographic_segment"),
            
            # Data quality measures
            col("event_data_quality_score"),
            
            # Pipeline metadata
            col("e._pipeline_env"),
            col("e.processed_at").alias("fact_last_updated")
        )
    )

print("✅ Gold medical events fact table pipeline loaded successfully")