"""
Healthcare Data Schemas for HIPAA-Compliant Patient Data Processing
Centralized schema definitions for bronze, silver, and gold layer transformations.
Implements exact 3-entity healthcare domain model from INITIAL.md requirements.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType

# MANDATORY: EXACTLY 3 entities - NO additions or deviations allowed

# Entity 1: Patients (PRIMARY ENTITY)
PATIENT_SCHEMA = StructType([
    # Primary Key
    StructField("patient_id", StringType(), False),           # PK - MANDATORY
    
    # Demographics (from INITIAL.md lines 220-240)
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True), 
    StructField("age", IntegerType(), True),                  # 18-85 range (μ=45, σ=15)
    StructField("sex", StringType(), True),                   # MALE/FEMALE only
    StructField("region", StringType(), True),                # NORTHEAST/NORTHWEST/SOUTHEAST/SOUTHWEST
    
    # Health Metrics
    StructField("bmi", DoubleType(), True),                   # 16-50 range (μ=28, σ=6)
    StructField("smoker", BooleanType(), True),               # Age-correlated probability
    StructField("children", IntegerType(), True),             # Poisson λ=1.2
    
    # Financial Data
    StructField("charges", DoubleType(), True),               # Calculated premium based on risk
    
    # Insurance Details  
    StructField("insurance_plan", StringType(), True),
    StructField("coverage_start_date", StringType(), True),
    
    # Temporal Data
    StructField("timestamp", StringType(), True),             # Record creation timestamp
])

# Entity 2: Claims (TRANSACTIONAL ENTITY)
CLAIMS_SCHEMA = StructType([
    StructField("claim_id", StringType(), False),             # PK - MANDATORY
    StructField("patient_id", StringType(), False),          # FK to patients - MANDATORY
    StructField("claim_amount", DoubleType(), True),
    StructField("claim_date", StringType(), True),  
    StructField("diagnosis_code", StringType(), True),        # ICD-10
    StructField("procedure_code", StringType(), True),        # CPT
    StructField("claim_status", StringType(), True),          # submitted/approved/denied/paid
    StructField("timestamp", StringType(), True),             # Record creation timestamp
])

# Entity 3: Medical_Events (EVENT ENTITY)  
MEDICAL_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),             # PK - MANDATORY
    StructField("patient_id", StringType(), False),          # FK to patients - MANDATORY
    StructField("event_date", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("medical_provider", StringType(), True),
    StructField("timestamp", StringType(), True),             # Record creation timestamp
])

# Validation Constants
VALID_REGIONS = ["NORTHEAST", "NORTHWEST", "SOUTHEAST", "SOUTHWEST"]
VALID_SEX_VALUES = ["MALE", "FEMALE"]
VALID_CLAIM_STATUSES = ["submitted", "approved", "denied", "paid"]
VALID_EVENT_TYPES = ["checkup", "emergency", "surgery", "consultation", "diagnostic", "treatment"]

# Bronze Layer Schema Enhancements (with metadata)
BRONZE_PATIENT_SCHEMA = StructType([
    # Core patient fields
    StructField("patient_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True), 
    StructField("age", IntegerType(), True),
    StructField("sex", StringType(), True),
    StructField("region", StringType(), True),
    StructField("bmi", DoubleType(), True),
    StructField("smoker", BooleanType(), True),
    StructField("children", IntegerType(), True),
    StructField("charges", DoubleType(), True),
    StructField("insurance_plan", StringType(), True),
    StructField("coverage_start_date", StringType(), True),
    StructField("timestamp", StringType(), True),
    
    # Pipeline metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True),
    StructField("_file_name", StringType(), True),
    StructField("_file_path", StringType(), True),
    StructField("_file_size", StringType(), True),
    StructField("_file_modification_time", StringType(), True),
])

BRONZE_CLAIMS_SCHEMA = StructType([
    # Core claims fields
    StructField("claim_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("claim_amount", DoubleType(), True),
    StructField("claim_date", StringType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("procedure_code", StringType(), True),
    StructField("claim_status", StringType(), True),
    StructField("timestamp", StringType(), True),
    
    # Pipeline metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True),
    StructField("_file_name", StringType(), True),
    StructField("_file_path", StringType(), True),
    StructField("_file_size", StringType(), True),
    StructField("_file_modification_time", StringType(), True),
])

BRONZE_MEDICAL_EVENTS_SCHEMA = StructType([
    # Core medical events fields
    StructField("event_id", StringType(), False),
    StructField("patient_id", StringType(), False),
    StructField("event_date", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("medical_provider", StringType(), True),
    StructField("timestamp", StringType(), True),
    
    # Pipeline metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True),
    StructField("_file_name", StringType(), True),
    StructField("_file_path", StringType(), True),
    StructField("_file_size", StringType(), True),
    StructField("_file_modification_time", StringType(), True),
])

# Data Quality Expectations Dictionary
PATIENT_DATA_QUALITY_EXPECTATIONS = {
    # Primary Key Validation
    "valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    
    # Demographics Validation
    "valid_age": "age IS NOT NULL AND age BETWEEN 18 AND 85",
    "valid_sex": "sex IS NOT NULL AND sex IN ('MALE', 'FEMALE')",
    "valid_region": "region IS NOT NULL AND region IN ('NORTHEAST', 'NORTHWEST', 'SOUTHEAST', 'SOUTHWEST')",
    
    # Health Metrics Validation
    "valid_bmi": "bmi IS NOT NULL AND bmi BETWEEN 16 AND 50",
    "valid_smoker": "smoker IS NOT NULL",
    "valid_children": "children IS NOT NULL AND children >= 0",
    
    # Financial Data Validation
    "valid_charges": "charges IS NOT NULL AND charges > 0"
}

PATIENT_MONITORING_EXPECTATIONS = {
    # Name Completeness
    "complete_name": "first_name IS NOT NULL AND last_name IS NOT NULL",
    
    # Insurance Details
    "valid_coverage_date": "coverage_start_date IS NOT NULL",
    "valid_insurance_plan": "insurance_plan IS NOT NULL",
    
    # Temporal Data
    "valid_timestamp": "timestamp IS NOT NULL",
    
    # Reasonable Range Checks
    "reasonable_age": "age BETWEEN 18 AND 80",  # Most common range
    "reasonable_bmi": "bmi BETWEEN 18 AND 40",  # Most common range
    "reasonable_children": "children <= 10"     # Reasonable upper bound
}

CLAIMS_DATA_QUALITY_EXPECTATIONS = {
    "valid_claim_id": "claim_id IS NOT NULL AND LENGTH(claim_id) >= 5",
    "valid_patient_reference": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "valid_claim_amount": "claim_amount IS NOT NULL AND claim_amount > 0",
    "valid_claim_date": "claim_date IS NOT NULL",
    "valid_claim_status": "claim_status IS NOT NULL AND claim_status IN ('submitted', 'approved', 'denied', 'paid')"
}

MEDICAL_EVENTS_DATA_QUALITY_EXPECTATIONS = {
    "valid_event_id": "event_id IS NOT NULL AND LENGTH(event_id) >= 5",
    "valid_patient_reference": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "valid_event_date": "event_date IS NOT NULL",
    "valid_event_type": "event_type IS NOT NULL",
    "valid_medical_provider": "medical_provider IS NOT NULL"
}

# HIPAA Compliance Constants
HIPAA_AGE_THRESHOLD = 89  # Ages 89+ become 90 for de-identification
PATIENT_SALT = "HEALTHCARE_PATIENT_PII_SALT_2024"  # Salt for PII hashing

print("✅ Healthcare schemas loaded successfully - 3 entities with HIPAA compliance patterns")