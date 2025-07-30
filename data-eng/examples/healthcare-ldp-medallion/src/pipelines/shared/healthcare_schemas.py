"""
Healthcare Domain Schema Definitions
Complete schema definitions for healthcare insurance patient data with HIPAA compliance.
Implements exactly 3 entities: Patients, Claims, Medical_Events as required by domain model.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, 
    BooleanType, TimestampType, DateType
)

# COMPLETE PATIENT SCHEMA - MANDATORY FIELDS FROM INITIAL.MD
PATIENT_SCHEMA = StructType([
    # Primary Key
    StructField("patient_id", StringType(), False),      # PK - MANDATORY
    
    # Demographics
    StructField("first_name", StringType(), True),       # Demographics
    StructField("last_name", StringType(), True),        # Demographics  
    StructField("age", IntegerType(), True),             # Demographics (18-85)
    StructField("sex", StringType(), True),              # Demographics (MALE/FEMALE)
    StructField("region", StringType(), True),           # Location (NORTHEAST/NORTHWEST/SOUTHEAST/SOUTHWEST)
    
    # Health Metrics
    StructField("bmi", DoubleType(), True),              # Health metric (16-50)
    StructField("smoker", BooleanType(), True),          # Lifestyle factor
    StructField("children", IntegerType(), True),        # Number of dependents
    
    # Financial Data
    StructField("charges", DoubleType(), True),          # Calculated insurance premium
    
    # Insurance Details
    StructField("insurance_plan", StringType(), True),   # Insurance details
    StructField("coverage_start_date", StringType(), True), # Insurance details
    
    # HIPAA Sensitive Fields (for bronze layer only - will be hashed in silver)
    StructField("ssn", StringType(), True),              # SSN - MUST be hashed in silver layer
    StructField("date_of_birth", StringType(), True),    # DOB - for age calculation validation
    StructField("zip_code", StringType(), True),         # Geographic data - last 2 digits only for elderly
    
    # Temporal Data
    StructField("timestamp", StringType(), True),        # Record creation timestamp
    
    # Pipeline Metadata fields - Added during ingestion
    StructField("_ingested_at", TimestampType(), True),  # Ingestion timestamp
    StructField("_pipeline_env", StringType(), True),    # Environment (dev/prod)
    StructField("_file_name", StringType(), True),       # Source file name
    StructField("_file_path", StringType(), True),       # Source file path
    StructField("_file_size", StringType(), True),       # Source file size
    StructField("_file_modification_time", TimestampType(), True)  # File modification time
])

# CLAIMS SCHEMA - TRANSACTIONAL ENTITY
CLAIMS_SCHEMA = StructType([
    # Primary Key and Foreign Key
    StructField("claim_id", StringType(), False),        # PK - MANDATORY
    StructField("patient_id", StringType(), False),     # FK to patients - MANDATORY
    
    # Financial Data
    StructField("claim_amount", DoubleType(), True),    # Financial data
    StructField("claim_date", StringType(), True),      # Temporal data
    StructField("approval_date", StringType(), True),   # Processing date
    StructField("payment_date", StringType(), True),    # Payment processing
    
    # Clinical Data
    StructField("diagnosis_code", StringType(), True),  # ICD-10 code
    StructField("procedure_code", StringType(), True),  # CPT code
    StructField("diagnosis_description", StringType(), True),  # Clinical description
    StructField("procedure_description", StringType(), True),  # Procedure description
    
    # Claim Processing
    StructField("claim_status", StringType(), True),    # Status (submitted/approved/denied/paid)
    StructField("claim_type", StringType(), True),      # Type (inpatient/outpatient/pharmacy/dental)
    StructField("provider_id", StringType(), True),     # Healthcare provider identifier
    StructField("provider_name", StringType(), True),   # Provider name
    StructField("denial_reason", StringType(), True),   # Reason for denial if applicable
    
    # Pipeline Metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True),
    StructField("_file_name", StringType(), True),
    StructField("_file_path", StringType(), True),
    StructField("_file_size", StringType(), True),
    StructField("_file_modification_time", TimestampType(), True)
])

# MEDICAL_EVENTS SCHEMA - EVENT ENTITY
MEDICAL_EVENTS_SCHEMA = StructType([
    # Primary Key and Foreign Key
    StructField("event_id", StringType(), False),       # PK - MANDATORY
    StructField("patient_id", StringType(), False),    # FK to patients - MANDATORY
    
    # Event Data
    StructField("event_date", StringType(), True),     # Temporal data
    StructField("event_type", StringType(), True),     # Event category (office_visit/procedure/lab_test/imaging/emergency)
    StructField("event_description", StringType(), True), # Clinical description
    
    # Provider Information
    StructField("medical_provider", StringType(), True), # Provider info
    StructField("provider_type", StringType(), True),   # Provider specialty
    StructField("facility_name", StringType(), True),   # Healthcare facility
    StructField("facility_type", StringType(), True),   # Facility type (hospital/clinic/lab/imaging)
    
    # Clinical Details
    StructField("primary_diagnosis", StringType(), True), # Primary diagnosis code
    StructField("secondary_diagnosis", StringType(), True), # Secondary diagnosis
    StructField("vital_signs", StringType(), True),     # Vital signs (JSON string)
    StructField("medications_prescribed", StringType(), True), # Medications (JSON array)
    
    # Outcome Data
    StructField("visit_duration_minutes", IntegerType(), True), # Visit length
    StructField("follow_up_required", BooleanType(), True),     # Follow-up flag
    StructField("emergency_flag", BooleanType(), True),         # Emergency indicator
    StructField("admission_flag", BooleanType(), True),         # Hospital admission
    
    # Pipeline Metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True),
    StructField("_file_name", StringType(), True),
    StructField("_file_path", StringType(), True),
    StructField("_file_size", StringType(), True),
    StructField("_file_modification_time", TimestampType(), True)
])

# HIPAA COMPLIANCE METADATA SCHEMA - For audit trails
HIPAA_AUDIT_SCHEMA = StructType([
    StructField("audit_id", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("operation_type", StringType(), False),  # INSERT/UPDATE/DELETE
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("timestamp", TimestampType(), False),
    StructField("data_classification", StringType(), True),  # PHI/PII/PUBLIC
    StructField("encryption_key_id", StringType(), True),
    StructField("retention_policy", StringType(), True)
])

# CLINICAL VALIDATION REFERENCE DATA
VALID_REGIONS = ["NORTHEAST", "NORTHWEST", "SOUTHEAST", "SOUTHWEST"]
VALID_SEX_VALUES = ["MALE", "FEMALE", "OTHER", "UNKNOWN"]
VALID_CLAIM_STATUSES = ["SUBMITTED", "APPROVED", "DENIED", "PAID", "PENDING", "CANCELLED"]
VALID_CLAIM_TYPES = ["INPATIENT", "OUTPATIENT", "PHARMACY", "DENTAL", "VISION", "MENTAL_HEALTH"]
VALID_EVENT_TYPES = ["OFFICE_VISIT", "PROCEDURE", "LAB_TEST", "IMAGING", "EMERGENCY", "SURGERY", "CONSULTATION"]
VALID_FACILITY_TYPES = ["HOSPITAL", "CLINIC", "LABORATORY", "IMAGING_CENTER", "PHARMACY", "URGENT_CARE"]

# AGE RANGES AND BMI LIMITS FOR VALIDATION
MIN_PATIENT_AGE = 18
MAX_PATIENT_AGE = 85
MIN_BMI = 16.0
MAX_BMI = 50.0
MAX_CHILDREN = 10

# HIPAA COMPLIANCE SETTINGS
PATIENT_SALT = "PATIENT_DATA_ENCRYPTION_SALT_2024"  # For SSN hashing
ELDERLY_AGE_THRESHOLD = 89  # Ages 89+ become 90 for de-identification
ZIP_CODE_ANONYMIZATION_THRESHOLD = 89  # Last 2 digits only for elderly patients

# DATA QUALITY THRESHOLDS
TARGET_DATA_QUALITY_SCORE = 99.5  # 99.5% target across all layers
HIPAA_AUDIT_RETENTION_DAYS = 2555  # 7 years in days