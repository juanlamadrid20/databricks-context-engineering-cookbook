"""
Gold Layer: Medical Events Fact Table
Implements a comprehensive medical events fact table with clinical analytics,
care coordination metrics, and foreign key relationships to patient dimension.
"""

import dlt
import sys
import os
from pyspark.sql.functions import (
    current_timestamp, col, lit, when, coalesce, year, month, dayofweek,
    datediff, round as spark_round, sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, row_number, dense_rank, concat_ws, lag, lead, desc,
    countDistinct
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
    name="fact_medical_events",  # CORRECT: Simple table name - catalog/schema specified at pipeline level
    comment="Medical events fact table with clinical analytics, care coordination metrics, and comprehensive healthcare insights",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.feature.allowColumnDefaults": "supported",
        "delta.enableChangeDataFeed": "true",  # Enable for downstream analytics
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "pipelines.pii.fields": "patient_natural_key,medical_provider_standardized,facility_name_standardized",
        "compliance": "HIPAA",
        "data_classification": "PHI",
        "environment": PIPELINE_ENV,
        "dimensional_model": "fact_table",
        "business_domain": "clinical_events_analytics"
    }
)
def fact_medical_events():
    """
    Create medical events fact table with comprehensive clinical analytics and care coordination metrics.
    
    CRITICAL IMPLEMENTATIONS:
    - Foreign key relationships to patient dimension
    - Clinical pathway tracking and care coordination
    - Population health metrics and outcomes
    - Provider network analytics
    - Temporal analysis for care patterns
    """
    
    # Join silver medical events with patient dimension for surrogate keys
    silver_events = dlt.read("silver_medical_events").alias("se")
    dim_patients = dlt.read("dim_patients").alias("dp")
    
    # Create patient event sequencing windows
    patient_event_window = Window.partitionBy("patient_id").orderBy("event_date_parsed")
    patient_event_desc_window = Window.partitionBy("patient_id").orderBy(desc("event_date_parsed"))
    
    return (
        silver_events
        .join(
            dim_patients.filter("is_current_record = true").select(
                "patient_natural_key", 
                "patient_surrogate_key",
                "health_risk_category",
                "patient_age_category",
                "patient_region"
            ),
            silver_events.patient_id == dim_patients.patient_natural_key,
            "inner"  # Only events with valid patient dimension records
        )
        
        # FACT TABLE KEYS
        .withColumn("event_fact_key", 
                   row_number().over(Window.orderBy("event_id")))
        .withColumn("event_natural_key", col("event_id"))
        .withColumn("patient_surrogate_key", col("patient_surrogate_key"))  # FK to dim_patients
        .withColumn("patient_natural_key", col("patient_id"))
        
        # TEMPORAL DIMENSIONS
        .withColumn("event_date", col("event_date_parsed"))
        .withColumn("event_year", year(col("event_date_parsed")))
        .withColumn("event_month", month(col("event_date_parsed")))
        .withColumn("event_quarter", 
                   when(month(col("event_date_parsed")).isin([1,2,3]), 1)
                   .when(month(col("event_date_parsed")).isin([4,5,6]), 2)
                   .when(month(col("event_date_parsed")).isin([7,8,9]), 3)
                   .otherwise(4))
        .withColumn("event_day_of_week", dayofweek(col("event_date_parsed")))
        .withColumn("event_day_name",
                   when(col("event_day_of_week") == 1, "SUNDAY")
                   .when(col("event_day_of_week") == 2, "MONDAY")
                   .when(col("event_day_of_week") == 3, "TUESDAY")
                   .when(col("event_day_of_week") == 4, "WEDNESDAY")
                   .when(col("event_day_of_week") == 5, "THURSDAY")
                   .when(col("event_day_of_week") == 6, "FRIDAY")
                   .otherwise("SATURDAY"))
        .withColumn("is_weekend", 
                   col("event_day_of_week").isin([1, 7]))  # Sunday = 1, Saturday = 7
        
        # EVENT CLASSIFICATION
        .withColumn("event_type", col("event_type_standardized"))
        .withColumn("event_description", col("event_description_cleaned"))
        .withColumn("care_intensity", col("care_intensity"))
        .withColumn("care_urgency", col("care_urgency"))
        .withColumn("clinical_complexity", col("clinical_complexity"))
        
        # FACILITY AND PROVIDER DIMENSIONS
        .withColumn("facility_name", col("facility_name_standardized"))
        .withColumn("facility_type", col("facility_type_standardized"))
        .withColumn("medical_provider", col("medical_provider_standardized"))
        .withColumn("provider_type", col("provider_type_standardized"))
        
        # CLINICAL MEASURES
        .withColumn("primary_diagnosis_code", col("primary_diagnosis_standardized"))
        .withColumn("secondary_diagnosis_code", col("secondary_diagnosis_standardized"))
        .withColumn("visit_duration_minutes", col("visit_duration_validated"))
        .withColumn("medications_count", col("medications_count"))
        
        # VITAL SIGNS MEASURES
        .withColumn("bp_systolic", col("vital_signs_bp_systolic"))
        .withColumn("bp_diastolic", col("vital_signs_bp_diastolic"))
        .withColumn("pulse_rate", col("vital_signs_pulse"))
        .withColumn("body_temperature", col("vital_signs_temp"))
        .withColumn("vital_signs_normal", col("vital_signs_normal"))
        
        # CARE FLAGS AND INDICATORS
        .withColumn("follow_up_required", col("follow_up_required_flag"))
        .withColumn("emergency_visit", col("emergency_flag_validated"))
        .withColumn("hospital_admission", col("admission_flag_validated"))
        
        # PATIENT CONTEXT (from dimension)
        .withColumn("patient_health_risk_category", col("health_risk_category"))
        .withColumn("patient_age_category", col("patient_age_category"))
        .withColumn("patient_region", col("patient_region"))
        
        # PATIENT CARE JOURNEY ANALYTICS
        .withColumn("patient_event_sequence", 
                   row_number().over(patient_event_window))
        .withColumn("is_first_event", 
                   col("patient_event_sequence") == 1)
        .withColumn("patient_total_events_to_date",
                   count("*").over(patient_event_window.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn("previous_event_date",
                   lag("event_date_parsed", 1).over(patient_event_window))
        .withColumn("next_event_date",
                   lead("event_date_parsed", 1).over(patient_event_window))
        .withColumn("days_since_previous_event",
                   when(col("previous_event_date").isNotNull(),
                        datediff(col("event_date_parsed"), col("previous_event_date")))
                   .otherwise(None))
        .withColumn("days_to_next_event",
                   when(col("next_event_date").isNotNull(),
                        datediff(col("next_event_date"), col("event_date_parsed")))
                   .otherwise(None))
        
        # CARE COORDINATION METRICS
        .withColumn("care_continuity_gap",
                   when(col("days_since_previous_event") > 90, "LONG_GAP")
                   .when(col("days_since_previous_event") > 30, "MODERATE_GAP")
                   .when(col("days_since_previous_event") > 7, "SHORT_GAP")
                   .when(col("days_since_previous_event").isNotNull(), "CONTINUOUS")
                   .otherwise("FIRST_EVENT"))
        
        # SEASONAL AND TEMPORAL ANALYSIS
        .withColumn("event_season",
                   when(col("event_month").isin([12,1,2]), "WINTER")
                   .when(col("event_month").isin([3,4,5]), "SPRING")
                   .when(col("event_month").isin([6,7,8]), "SUMMER")
                   .otherwise("FALL"))
        .withColumn("days_since_event", 
                   datediff(current_timestamp().cast("date"), col("event_date_parsed")))
        .withColumn("event_recency_category", col("event_recency_category"))
        
        # CLINICAL OUTCOME SCORING
        .withColumn("clinical_outcome_score",
                   # Base outcome scoring
                   (when(col("vital_signs_normal") == True, 10).otherwise(0) +
                    when(col("follow_up_required") == False, 10).otherwise(0) +
                    when(col("emergency_visit") == False, 15).otherwise(-10) +
                    when(col("hospital_admission") == False, 15).otherwise(-15) +
                    when(col("visit_duration_minutes") <= 60, 5).otherwise(0) +
                    when(col("medications_count") <= 2, 5).otherwise(0)))
        .withColumn("clinical_outcome_category",
                   when(col("clinical_outcome_score") >= 30, "EXCELLENT")
                   .when(col("clinical_outcome_score") >= 15, "GOOD")
                   .when(col("clinical_outcome_score") >= 0, "FAIR")
                   .otherwise("POOR"))
        
        # POPULATION HEALTH INDICATORS
        .withColumn("preventive_care_indicator",
                   when(col("event_type").isin(["OFFICE_VISIT", "CONSULTATION"]) &
                        (col("emergency_visit") == False) &
                        (col("follow_up_required") == True), True)
                   .otherwise(False))
        .withColumn("acute_care_indicator",
                   when(col("event_type").isin(["EMERGENCY", "SURGERY"]) |
                        (col("emergency_visit") == True) |
                        (col("hospital_admission") == True), True)
                   .otherwise(False))
        .withColumn("chronic_management_indicator",
                   when((col("patient_event_sequence") > 3) &
                        (col("days_since_previous_event") <= 90) &
                        (col("medications_count") > 0), True)
                   .otherwise(False))
        
        # PROVIDER NETWORK ANALYTICS
        .withColumn("provider_patient_familiarity",
                   dense_rank().over(Window.partitionBy("patient_id", "medical_provider").orderBy("event_date_parsed")))
        .withColumn("is_new_provider",
                   col("provider_patient_familiarity") == 1)
        
        # CARE QUALITY INDICATORS
        .withColumn("care_efficiency_score",
                   when(col("visit_duration_minutes").isNotNull() & (col("visit_duration_minutes") > 0),
                        spark_round(60.0 / col("visit_duration_minutes") * 100, 2))  # Efficiency = 60min baseline / actual duration
                   .otherwise(None))
        .withColumn("care_appropriateness_score",
                   # Based on event type matching urgency
                   when((col("care_urgency") == "EMERGENCY") & (col("facility_type") == "HOSPITAL"), 100)
                   .when((col("care_urgency") == "ROUTINE") & (col("facility_type") == "CLINIC"), 100)
                   .when((col("care_urgency") == "URGENT") & (col("facility_type").isin(["HOSPITAL", "URGENT_CARE"])), 100)
                   .otherwise(75))  # Suboptimal but acceptable
        
        # QUALITY AND COMPLIANCE
        .withColumn("event_data_quality_score", col("data_quality_score"))
        .withColumn("clinical_quality_score", col("clinical_quality_score"))
        .withColumn("referential_integrity_validated", col("referential_integrity_validated"))
        
        # AUDIT AND LINEAGE
        .withColumn("fact_created_timestamp", current_timestamp())
        .withColumn("source_system", lit("HEALTHCARE_DLT_PIPELINE"))
        .withColumn("data_lineage_pipeline_env", col("_pipeline_env"))
        .withColumn("data_lineage_silver_processing", col("_silver_processed_at"))
        
        # SELECT FINAL FACT TABLE COLUMNS
        .select(
            # Fact Keys
            "event_fact_key",
            "event_natural_key",
            
            # Dimension Foreign Keys
            "patient_surrogate_key",  # FK to dim_patients
            "patient_natural_key",
            
            # Temporal Dimensions
            "event_date",
            "event_year",
            "event_month",
            "event_quarter",
            "event_day_of_week",
            "event_day_name",
            "event_season",
            "is_weekend",
            
            # Event Classification
            "event_type",
            "event_description",
            "care_intensity",
            "care_urgency",
            "clinical_complexity",
            
            # Facility and Provider
            "facility_name",
            "facility_type",
            "medical_provider",
            "provider_type",
            
            # Clinical Measures
            "primary_diagnosis_code",
            "secondary_diagnosis_code",
            "visit_duration_minutes",
            "medications_count",
            
            # Vital Signs
            "bp_systolic",
            "bp_diastolic",
            "pulse_rate",
            "body_temperature",
            "vital_signs_normal",
            
            # Care Flags
            "follow_up_required",
            "emergency_visit",
            "hospital_admission",
            
            # Patient Context
            "patient_health_risk_category",
            "patient_age_category",
            "patient_region",
            
            # Care Journey Analytics
            "patient_event_sequence",
            "is_first_event",
            "patient_total_events_to_date",
            "days_since_previous_event",
            "days_to_next_event",
            "care_continuity_gap",
            
            # Temporal Analysis
            "days_since_event",
            "event_recency_category",
            
            # Clinical Outcomes
            "clinical_outcome_score",
            "clinical_outcome_category",
            
            # Population Health
            "preventive_care_indicator",
            "acute_care_indicator",
            "chronic_management_indicator",
            
            # Provider Analytics
            "provider_patient_familiarity",
            "is_new_provider",
            
            # Care Quality
            "care_efficiency_score",
            "care_appropriateness_score",
            
            # Quality Measures
            "event_data_quality_score",
            "clinical_quality_score",
            "referential_integrity_validated",
            
            # Audit Columns
            "fact_created_timestamp",
            "source_system",
            "data_lineage_pipeline_env",
            "data_lineage_silver_processing"
        )
    )

@dlt.table(
    name="fact_medical_events_care_pathways",
    comment="Care pathway analytics and population health metrics derived from medical events"
)
def fact_medical_events_care_pathways():
    """
    Generate care pathway analytics for population health management and care coordination.
    """
    
    return (
        dlt.read("fact_medical_events")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
        .groupBy(
            "patient_natural_key",
            "patient_health_risk_category",
            "patient_age_category",
            "event_year",
            "event_quarter"
        )
        .agg(
            # Volume Metrics
            count("*").alias("total_events"),
            countDistinct("medical_provider").alias("unique_providers"),
            countDistinct("facility_name").alias("unique_facilities"),
            
            # Care Type Distribution
            spark_sum(when(col("preventive_care_indicator") == True, 1).otherwise(0)).alias("preventive_care_events"),
            spark_sum(when(col("acute_care_indicator") == True, 1).otherwise(0)).alias("acute_care_events"),
            spark_sum(when(col("chronic_management_indicator") == True, 1).otherwise(0)).alias("chronic_management_events"),
            
            # Emergency and Hospital Utilization
            spark_sum(when(col("emergency_visit") == True, 1).otherwise(0)).alias("emergency_visits"),
            spark_sum(when(col("hospital_admission") == True, 1).otherwise(0)).alias("hospital_admissions"),
            
            # Clinical Quality Measures
            avg("clinical_outcome_score").alias("avg_clinical_outcome_score"),
            avg("care_efficiency_score").alias("avg_care_efficiency_score"),
            avg("care_appropriateness_score").alias("avg_care_appropriateness_score"),
            
            # Care Coordination
            avg("days_since_previous_event").alias("avg_care_gap_days"),
            spark_sum(when(col("care_continuity_gap") == "LONG_GAP", 1).otherwise(0)).alias("long_care_gaps"),
            
            # Provider Continuity
            avg("provider_patient_familiarity").alias("avg_provider_familiarity"),
            spark_sum(when(col("is_new_provider") == True, 1).otherwise(0)).alias("new_provider_encounters"),
            
            # Follow-up Compliance
            spark_sum(when(col("follow_up_required") == True, 1).otherwise(0)).alias("follow_up_required_count"),
            
            # Clinical Metrics
            avg("visit_duration_minutes").alias("avg_visit_duration"),
            avg("medications_count").alias("avg_medications_per_event"),
            spark_sum(when(col("vital_signs_normal") == True, 1).otherwise(0)).alias("normal_vital_signs_count")
        )
        .withColumn("preventive_care_rate",
                   spark_round((col("preventive_care_events") / col("total_events") * 100), 2))
        .withColumn("acute_care_rate",
                   spark_round((col("acute_care_events") / col("total_events") * 100), 2))
        .withColumn("emergency_utilization_rate",
                   spark_round((col("emergency_visits") / col("total_events") * 100), 2))
        .withColumn("provider_continuity_score",
                   spark_round((1.0 / col("unique_providers") * 100), 2))  # Higher score = fewer providers (better continuity)
        .withColumn("care_coordination_score",
                   when(col("avg_care_gap_days") <= 30, 100)
                   .when(col("avg_care_gap_days") <= 60, 75)
                   .when(col("avg_care_gap_days") <= 90, 50)
                   .otherwise(25))
        .withColumn("pathway_analysis_date", current_timestamp())
        .withColumn("quarter_year", concat_ws("-", col("event_year"), col("event_quarter")))
    )