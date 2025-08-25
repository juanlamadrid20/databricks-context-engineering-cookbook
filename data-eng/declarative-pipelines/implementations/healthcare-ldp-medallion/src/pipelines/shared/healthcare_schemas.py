"""
Healthcare Schemas for Patient Data Medallion Pipeline

This module provides centralized schema definitions for all layers of the healthcare
data medallion pipeline, ensuring consistency and compliance with HIPAA requirements.

CRITICAL: Implementation MUST create EXACTLY these 3 entities with EXACTLY these schemas
- No additions, no omissions per domain model enforcement
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, DoubleType, BooleanType, DateType
)


class HealthcareSchemas:
    """
    Centralized healthcare schema definitions for Bronze, Silver, and Gold layers.
    
    Business Entities (EXACTLY 3 - STRICT ENFORCEMENT):
    1. Patients (Primary Entity): Demographics, insurance, health metrics
    2. Claims (Transactional Entity): Insurance claims with ICD-10/CPT coding  
    3. Medical_Events (Event Entity): Clinical encounters, lab results, vital signs
    """
    
    # ==============================================================================
    # PATIENT ENTITY SCHEMAS
    # ==============================================================================
    
    BRONZE_PATIENT_SCHEMA = StructType([
        StructField("patient_id", StringType(), False),        # Primary patient identifier
        StructField("mrn", StringType(), True),                # Medical Record Number
        StructField("first_name", StringType(), True),         # Raw first name from EHR
        StructField("last_name", StringType(), True),          # Raw last name from EHR
        StructField("date_of_birth", StringType(), True),      # Raw DOB (various formats)
        StructField("gender", StringType(), True),             # Raw gender (various formats)
        StructField("address_line1", StringType(), True),      # Raw address
        StructField("city", StringType(), True),               # Raw city
        StructField("state", StringType(), True),              # Raw state
        StructField("zip_code", StringType(), True),           # Raw ZIP
        StructField("phone", StringType(), True),              # Raw phone
        StructField("email", StringType(), True),              # Raw email
        StructField("ssn", StringType(), True),                # Raw SSN (will be hashed)
        StructField("insurance_id", StringType(), True),       # Raw insurance identifier
        StructField("primary_care_provider", StringType(), True), # PCP identifier
        StructField("emergency_contact_name", StringType(), True), # Emergency contact
        StructField("emergency_contact_phone", StringType(), True), # Emergency phone
        StructField("source_system", StringType(), False),     # EHR system identifier
        StructField("effective_date", TimestampType(), False), # When record becomes effective
        StructField("created_at", TimestampType(), True),      # Original creation timestamp
        StructField("_ingested_at", TimestampType(), True)     # Pipeline metadata
    ])

    SILVER_PATIENT_SCHEMA = StructType([
        StructField("patient_id", StringType(), False),        # Primary patient identifier
        StructField("mrn", StringType(), True),                # Medical Record Number
        StructField("first_name", StringType(), True),         # Cleaned first name
        StructField("last_name", StringType(), True),          # Cleaned last name
        StructField("age_years", IntegerType(), True),         # Calculated age from DOB
        StructField("sex", StringType(), True),                # Standardized sex (M/F/U)
        StructField("region", StringType(), True),             # Derived region from state
        StructField("bmi", DoubleType(), True),                # Body Mass Index
        StructField("smoker", BooleanType(), True),            # Smoking status
        StructField("children", IntegerType(), True),          # Number of dependents
        StructField("charges", DoubleType(), True),            # Insurance premium
        StructField("insurance_plan", StringType(), True),     # Insurance plan type
        StructField("coverage_start_date", DateType(), True),  # Coverage start date
        StructField("zip_code", StringType(), True),           # De-identified ZIP
        StructField("phone", StringType(), True),              # Phone number
        StructField("email", StringType(), True),              # Email address
        StructField("ssn_hash", StringType(), True),           # Hashed SSN for privacy
        StructField("primary_care_provider", StringType(), True), # PCP identifier
        StructField("emergency_contact_name", StringType(), True), # Emergency contact
        StructField("emergency_contact_phone", StringType(), True), # Emergency phone
        StructField("patient_risk_category", StringType(), True), # Risk classification
        StructField("data_quality_score", DoubleType(), True), # Data quality metric
        StructField("effective_date", TimestampType(), False), # When record becomes effective
        StructField("processed_at", TimestampType(), True)     # Pipeline metadata
    ])

    GOLD_PATIENT_SCHEMA = StructType([
        StructField("patient_id", StringType(), False),        # Primary patient identifier
        StructField("mrn", StringType(), True),                # Medical Record Number
        StructField("age_years", IntegerType(), True),         # Age in years (de-identified)
        StructField("sex", StringType(), True),                # Sex (M/F/U)
        StructField("region", StringType(), True),             # Geographic region
        StructField("bmi", DoubleType(), True),                # Body Mass Index
        StructField("smoker", BooleanType(), True),            # Smoking status
        StructField("children", IntegerType(), True),          # Number of dependents
        StructField("charges", DoubleType(), True),            # Insurance premium
        StructField("insurance_plan", StringType(), True),     # Insurance plan type
        StructField("coverage_start_date", DateType(), True),  # Coverage start date
        StructField("patient_risk_category", StringType(), True), # Risk classification
        StructField("effective_from", TimestampType(), False), # SCD Type 2 effective start
        StructField("effective_to", TimestampType(), True),    # SCD Type 2 effective end
        StructField("is_current", BooleanType(), False)        # Current record flag
    ])

    # ==============================================================================
    # CLAIMS ENTITY SCHEMAS
    # ==============================================================================
    
    BRONZE_CLAIMS_SCHEMA = StructType([
        StructField("claim_id", StringType(), False),          # Primary claim identifier
        StructField("patient_id", StringType(), False),        # Foreign key to patients
        StructField("claim_amount", StringType(), True),       # Raw claim amount (string)
        StructField("claim_date", StringType(), True),         # Raw claim date (various formats)
        StructField("diagnosis_code", StringType(), True),     # Raw ICD-10 code
        StructField("procedure_code", StringType(), True),     # Raw CPT code
        StructField("claim_status", StringType(), True),       # Raw claim status
        StructField("provider_id", StringType(), True),        # Healthcare provider ID
        StructField("service_location", StringType(), True),   # Location of service
        StructField("source_system", StringType(), False),     # Claims system identifier
        StructField("_ingested_at", TimestampType(), True)     # Pipeline metadata
    ])

    SILVER_CLAIMS_SCHEMA = StructType([
        StructField("claim_id", StringType(), False),          # Primary claim identifier
        StructField("patient_id", StringType(), False),        # Foreign key to patients
        StructField("claim_amount", DoubleType(), True),       # Standardized claim amount
        StructField("claim_date", DateType(), True),           # Standardized claim date
        StructField("diagnosis_code", StringType(), True),     # Validated ICD-10 code
        StructField("procedure_code", StringType(), True),     # Validated CPT code
        StructField("claim_status", StringType(), True),       # Standardized claim status
        StructField("claim_category", StringType(), True),     # Derived claim category
        StructField("provider_id", StringType(), True),        # Healthcare provider ID
        StructField("service_location", StringType(), True),   # Location of service
        StructField("processed_at", TimestampType(), True)     # Pipeline metadata
    ])

    GOLD_CLAIMS_SCHEMA = StructType([
        StructField("claim_id", StringType(), False),          # Primary claim identifier
        StructField("patient_id", StringType(), False),        # Foreign key to gold_patients
        StructField("claim_amount", DoubleType(), True),       # Claim amount
        StructField("claim_date", DateType(), True),           # Claim date
        StructField("diagnosis_code", StringType(), True),     # ICD-10 diagnosis code
        StructField("procedure_code", StringType(), True),     # CPT procedure code
        StructField("claim_status", StringType(), True),       # Claim status
        StructField("claim_category", StringType(), True),     # Claim category
        StructField("provider_id", StringType(), True),        # Healthcare provider ID
        StructField("service_location", StringType(), True),   # Location of service
        StructField("claim_month", StringType(), True),        # Derived month for partitioning
        StructField("claim_year", IntegerType(), True)         # Derived year for partitioning
    ])

    # ==============================================================================
    # MEDICAL EVENTS ENTITY SCHEMAS  
    # ==============================================================================
    
    BRONZE_MEDICAL_EVENTS_SCHEMA = StructType([
        StructField("event_id", StringType(), False),          # Primary event identifier
        StructField("patient_id", StringType(), False),        # Foreign key to patients
        StructField("event_date", StringType(), True),         # Raw event date (various formats)
        StructField("event_type", StringType(), True),         # Raw event type
        StructField("medical_provider", StringType(), True),   # Raw provider information
        StructField("event_details", StringType(), True),      # Additional event context
        StructField("clinical_results", StringType(), True),   # Lab results, vital signs
        StructField("source_system", StringType(), False),     # ADT/Lab system identifier
        StructField("_ingested_at", TimestampType(), True)     # Pipeline metadata
    ])

    SILVER_MEDICAL_EVENTS_SCHEMA = StructType([
        StructField("event_id", StringType(), False),          # Primary event identifier
        StructField("patient_id", StringType(), False),        # Foreign key to patients
        StructField("event_date", DateType(), True),           # Standardized event date
        StructField("event_type", StringType(), True),         # Standardized event type
        StructField("medical_provider", StringType(), True),   # Cleaned provider information
        StructField("event_category", StringType(), True),     # Derived event category
        StructField("event_details", StringType(), True),      # Additional event context
        StructField("clinical_results", StringType(), True),   # Lab results, vital signs
        StructField("processed_at", TimestampType(), True)     # Pipeline metadata
    ])

    GOLD_MEDICAL_EVENTS_SCHEMA = StructType([
        StructField("event_id", StringType(), False),          # Primary event identifier
        StructField("patient_id", StringType(), False),        # Foreign key to gold_patients
        StructField("event_date", DateType(), True),           # Event date
        StructField("event_type", StringType(), True),         # Event type
        StructField("medical_provider", StringType(), True),   # Medical provider
        StructField("event_category", StringType(), True),     # Event category
        StructField("event_details", StringType(), True),      # Additional event context
        StructField("clinical_results", StringType(), True),   # Lab results, vital signs
        StructField("event_month", StringType(), True),        # Derived month for partitioning
        StructField("event_year", IntegerType(), True)         # Derived year for partitioning
    ])


class HealthcareValidationRules:
    """
    Healthcare-specific validation rules and constants for data quality expectations.
    """
    
    # Valid healthcare codes and ranges
    VALID_GENDER_VALUES = ["M", "F", "U", "MALE", "FEMALE", "UNKNOWN"]
    VALID_REGIONS = ["NORTHEAST", "NORTHWEST", "SOUTHEAST", "SOUTHWEST"]
    VALID_CLAIM_STATUS = ["submitted", "approved", "denied", "paid", "pending"]
    VALID_EVENT_TYPES = ["admission", "discharge", "lab_result", "vital_signs", "clinical_note"]
    
    # Clinical ranges for validation
    MIN_AGE = 0
    MAX_AGE = 150
    MIN_BMI = 16.0
    MAX_BMI = 50.0
    MIN_CLAIM_AMOUNT = 0.0
    MAX_CLAIM_AMOUNT = 1000000.0
    
    # HIPAA compliance constants
    HIPAA_AGE_THRESHOLD = 89  # Ages 89+ become 90 for de-identification
    HIPAA_SAFE_AGE = 90       # Safe harbor age for elderly patients
    
    # Data quality thresholds
    MIN_DATA_QUALITY_SCORE = 0.6   # 60% minimum for silver layer
    TARGET_DATA_QUALITY_SCORE = 0.95  # 95% target for gold layer


class HealthcarePipelineUtilities:
    """
    Utility functions for healthcare data processing and HIPAA compliance.
    """
    
    @staticmethod
    def get_environment_config():
        """Get pipeline environment configuration from Spark conf."""
        return {
            'catalog': spark.conf.get("CATALOG", "juan_dev"),
            'schema': spark.conf.get("SCHEMA", "ctx_eng"),
            'pipeline_env': spark.conf.get("PIPELINE_ENV", "dev"),
            'volumes_path': spark.conf.get("VOLUMES_PATH", "/Volumes/juan_dev/ctx_eng/raw_data"),
            'max_files_per_trigger': spark.conf.get("MAX_FILES_PER_TRIGGER", "100")
        }
    
    @staticmethod
    def get_hipaa_salt():
        """Get HIPAA-compliant salt for hashing PII."""
        return "PATIENT_SALT_2024"  # In production, use secure key management
    
    @staticmethod
    def validate_icd10_format(code):
        """Validate ICD-10 diagnosis code format."""
        import re
        if not code:
            return True  # Allow null codes
        pattern = r'^[A-Z][0-9]{2}\.?[0-9A-Z]{0,4}$'
        return bool(re.match(pattern, code))
    
    @staticmethod
    def validate_cpt_format(code):
        """Validate CPT procedure code format."""
        import re
        if not code:
            return True  # Allow null codes
        pattern = r'^[0-9]{5}$'
        return bool(re.match(pattern, code))