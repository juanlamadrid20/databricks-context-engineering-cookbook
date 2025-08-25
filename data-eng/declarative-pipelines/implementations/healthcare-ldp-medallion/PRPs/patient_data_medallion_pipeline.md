# Patient Data Medallion Pipeline PRP

## Goal
**Build a production-ready healthcare data medallion pipeline using Lakeflow Declarative Pipelines (DLT) that processes health insurance patient data through Bronze → Silver → Gold layers with comprehensive data quality controls, HIPAA compliance, identity resolution, and real-time monitoring.**

## Why
- **Business Value**: Establish enterprise-grade data platform for health insurance analytics with proper governance and regulatory compliance
- **Clinical Impact**: Enable accurate patient 360° views, care coordination analytics, and population health insights for improved patient outcomes
- **Regulatory Compliance**: Ensure HIPAA-compliant data processing with audit trails, de-identification, and access controls
- **Data Quality**: Achieve 99.5%+ data quality score through comprehensive validation and clinical data validation rules
- **Real-Time Analytics**: Support care coordination and clinical decision-making with low-latency patient data processing

## What
**A complete medallion architecture pipeline processing exactly 3 healthcare business entities (Patients, Claims, Medical_Events) with identity resolution, synthetic data generation, and comprehensive observability.**

### Success Criteria
- [ ] **Exactly 3 business entities**: Patients, Claims, Medical Events (strict domain model enforcement)
- [ ] **Complete medallion pipeline**: Bronze → Silver → Gold with proper DLT data quality expectations
- [ ] **99.5% data quality score** with comprehensive validation rules and HIPAA compliance 
- [ ] **Identity resolution implementation**: Multi-source enrichment and conflict resolution
- [ ] **Synthetic data generation job**: Automated CSV generation with timestamped filenames and referential integrity
- [ ] **Observable pipeline metrics**: Real-time monitoring, alerting, and data governance dashboards
- [ ] **Asset Bundle deployment**: Infrastructure-as-code with serverless compute configuration
- [ ] **HIPAA compliance**: PII handling, audit trails, change data feed, and de-identification

### Healthcare Entity Model (MANDATORY - STRICT ENFORCEMENT)

**🚨 CRITICAL: Implementation MUST create EXACTLY these 3 entities with EXACTLY these schemas - no additions, no omissions.**

```
Data Sources → Core Business Entities Enrichment Model
│
├── EHR_SYSTEM (Electronic Health Records)
│   ├── PRIMARY ENRICHMENT: bronze_patients → silver_patients
│   │   ├── Patient demographics (first_name, last_name, DOB, gender, address)
│   │   ├── Insurance information (insurance_id, coverage_details)
│   │   └── Clinical identifiers (mrn, ssn_hash, primary_care_provider)
│
├── ADT_SYSTEM (Admit/Discharge/Transfer)  
│   ├── PRIMARY ENRICHMENT: bronze_medical_events → silver_medical_events
│   │   ├── Hospital admission/discharge events
│   │   ├── Care unit assignments and transfers
│   │   └── Length of stay calculations
│
└── LAB_SYSTEM (Laboratory Results)
    ├── PRIMARY ENRICHMENT: bronze_medical_events → silver_medical_events
    │   ├── Lab test results and clinical measurements
    │   ├── Vital signs and diagnostic indicators
    │   └── Clinical outcome tracking
```

**Mandatory Entity Implementation:**
1. **Patients** (dim_patients): Primary entity with demographics, insurance, health metrics
2. **Claims** (fact_claims): Transactional entity with claim amounts, diagnosis/procedure codes
3. **Medical_Events** (fact_medical_events): Event entity with clinical encounters and lab results

## All Needed Context

### Primary Development Reference
```yaml
# MANDATORY - Complete development context
- file: CLAUDE.md
  why: "Complete Databricks development patterns, Asset Bundle configurations, DLT pipeline examples, serverless compute requirements, and all external documentation references"
  extract: "All sections for comprehensive context - Critical DLT patterns, Asset Bundle serverless requirements, Unity Catalog naming, data quality expectations, and consolidated documentation references"

# Domain-Specific Healthcare Documentation  
- url: https://www.hl7.org/fhir/patient.html
  why: "FHIR Patient resource specifications for healthcare data standards, validation rules, and clinical interoperability requirements"
  
- url: https://www.hipaajournal.com/hipaa-compliance-checklist/
  why: "HIPAA compliance requirements for patient data handling, encryption, audit logging, de-identification, and access controls"

- url: https://databrickslabs.github.io/dbldatagen/public_docs/index.html
  why: "Synthetic healthcare data generation patterns, realistic patient/claims/medical events data modeling with proper referential integrity"

# Critical Databricks Documentation (from CLAUDE.md consolidated references)
- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-python-ref.html
  why: "DLT decorators (@dlt.table, @dlt.expect_*), streaming tables, pipeline patterns, and serverless compute requirements"

- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html  
  why: "Data quality expectations, quarantine patterns, validation strategies, and healthcare compliance controls"

- url: https://docs.databricks.com/data-governance/unity-catalog/best-practices.html
  why: "Three-part naming conventions, governance patterns, PII handling, and healthcare data classification"
```

### Real Implementation Examples from CLAUDE.md

#### Critical DLT Patterns (MANDATORY)
```python
# CRITICAL: Autoloader with DLT streaming - MUST include .format("cloudFiles")
@dlt.table(name="bronze_table_raw")
def bronze_table_raw():
    return (
        spark.readStream.format("cloudFiles")  # ← CRITICAL: Must include .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{VOLUMES_PATH}/_checkpoints/table_name")
        .schema(TABLE_SCHEMA)
        .load(f"{VOLUMES_PATH}")
    )

# CRITICAL: Environment-aware configuration loading
CATALOG = spark.conf.get("CATALOG", "{catalog_name}")
SCHEMA = spark.conf.get("SCHEMA", "{schema_name}")
PIPELINE_ENV = spark.conf.get("PIPELINE_ENV", "{sdlc_env}")
VOLUMES_PATH = spark.conf.get("VOLUMES_PATH", "/Volumes/{catalog_name}/{schema_name}/raw_data")

# CRITICAL: File metadata pattern (NOT deprecated input_file_name())
.select("*", "_metadata")  # REQUIRED: Explicitly select metadata
.withColumn("_file_name", col("_metadata.file_name"))
.withColumn("_file_path", col("_metadata.file_path"))
.drop("_metadata")  # Clean up temporary column
```

#### Asset Bundle Serverless Configuration (MANDATORY)
```yaml
# CRITICAL: Serverless DLT pipeline configuration (NO cluster configurations)
resources:
  pipelines:
    patient_data_pipeline:
      name: "health-insurance-patient-pipeline-${var.environment}"
      target: "${var.schema}"  # CRITICAL: Only schema when catalog specified separately
      catalog: "${var.catalog}"  # CRITICAL: Required for serverless compute
      serverless: true  # MANDATORY: Must be true - NO cluster configurations allowed
      
      libraries:
        # CRITICAL: Reference individual files, NOT directories
        - file:
            path: ../src/pipelines/bronze/patient_demographics_ingestion.py
        - file:
            path: ../src/pipelines/silver/patient_demographics_transform.py
            
      configuration:
        "CATALOG": "${var.catalog}"
        "SCHEMA": "${var.schema}"
        "PIPELINE_ENV": "${var.environment}"
        "VOLUMES_PATH": "${var.volumes_path}"
        # Healthcare compliance configurations
        "spark.databricks.delta.properties.defaults.changeDataFeed.enabled": "true"
        "compliance": "HIPAA"
        "data_classification": "PHI"
```

### Anti-Patterns to Prevent (from CLAUDE.md)

#### ❌ FORBIDDEN Asset Bundle Patterns
```yaml
# ❌ ALL OF THESE PATTERNS ARE FORBIDDEN - OMIT CLUSTER CONFIGS FOR SERVERLESS
resources:
  jobs:
    forbidden_job_example:
      name: "bad-example"
      # ❌ NEVER: job_clusters section
      job_clusters:
        - job_cluster_key: "any_key"
      # ❌ NEVER: new_cluster section at job level
      new_cluster:
        spark_version: "any_version"
      # ❌ NEVER: existing_cluster_id reference
      existing_cluster_id: "cluster-id"
```

#### ❌ FORBIDDEN Path Handling Patterns  
```python
# ❌ NEVER: Complex path resolution code in DLT pipelines
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils()...
    sys.path.insert(0, f"{base_path}/src")
except Exception:
    # ... complex fallback paths

# ✅ CORRECT: Simple imports work in Databricks DLT  
from shared.healthcare_schemas import HealthcareSchemas
```

## Implementation Blueprint

### Core Business Entity Schemas (EXACT IMPLEMENTATION REQUIRED)

```python
# PATIENT ENTITY SCHEMAS - EXACT IMPLEMENTATION REQUIRED
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType, DateType

# Bronze: Raw EHR format
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
    StructField("source_system", StringType(), False),     # EHR system identifier
    StructField("_ingested_at", TimestampType(), True)     # Pipeline metadata
])

# Silver: Standardized business format  
SILVER_PATIENT_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),        # Primary patient identifier
    StructField("mrn", StringType(), True),                # Medical Record Number
    StructField("age_years", IntegerType(), True),         # Calculated age from DOB
    StructField("sex", StringType(), True),                # Standardized sex (M/F/U)
    StructField("region", StringType(), True),             # Derived region from state
    StructField("bmi", DoubleType(), True),                # Calculated BMI
    StructField("smoker", BooleanType(), True),            # Smoking status
    StructField("children", IntegerType(), True),          # Number of dependents
    StructField("charges", DoubleType(), True),            # Insurance premium
    StructField("insurance_plan", StringType(), True),     # Insurance plan type
    StructField("coverage_start_date", DateType(), True),  # Coverage start date
    StructField("ssn_hash", StringType(), True),           # Hashed SSN for privacy
    StructField("patient_risk_category", StringType(), True), # Risk classification
    StructField("data_quality_score", DoubleType(), True), # Data quality metric
    StructField("effective_date", TimestampType(), False), # When record becomes effective
    StructField("processed_at", TimestampType(), True)     # Pipeline metadata
])

# Gold: Analytics-ready dimension with SCD Type 2
GOLD_PATIENT_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),        # Primary patient identifier
    StructField("mrn", StringType(), True),                # Medical Record Number
    StructField("age_years", IntegerType(), True),         # Age in years
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

# CLAIMS ENTITY SCHEMAS - EXACT IMPLEMENTATION REQUIRED
BRONZE_CLAIMS_SCHEMA = StructType([
    StructField("claim_id", StringType(), False),          # Primary claim identifier
    StructField("patient_id", StringType(), False),        # Foreign key to patients
    StructField("claim_amount", StringType(), True),       # Raw claim amount (string)
    StructField("claim_date", StringType(), True),         # Raw claim date (various formats)
    StructField("diagnosis_code", StringType(), True),     # Raw ICD-10 code
    StructField("procedure_code", StringType(), True),     # Raw CPT code
    StructField("claim_status", StringType(), True),       # Raw claim status
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
    StructField("claim_month", StringType(), True),        # Derived month for partitioning
    StructField("claim_year", IntegerType(), True)         # Derived year for partitioning
])

# MEDICAL EVENTS ENTITY SCHEMAS - EXACT IMPLEMENTATION REQUIRED
BRONZE_MEDICAL_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),          # Primary event identifier
    StructField("patient_id", StringType(), False),        # Foreign key to patients
    StructField("event_date", StringType(), True),         # Raw event date (various formats)
    StructField("event_type", StringType(), True),         # Raw event type
    StructField("medical_provider", StringType(), True),   # Raw provider information
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
    StructField("processed_at", TimestampType(), True)     # Pipeline metadata
])

GOLD_MEDICAL_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),          # Primary event identifier
    StructField("patient_id", StringType(), False),        # Foreign key to gold_patients
    StructField("event_date", DateType(), True),           # Event date
    StructField("event_type", StringType(), True),         # Event type
    StructField("medical_provider", StringType(), True),   # Medical provider
    StructField("event_category", StringType(), True),     # Event category
    StructField("event_month", StringType(), True),        # Derived month for partitioning
    StructField("event_year", IntegerType(), True)         # Derived year for partitioning
])
```

### Task Implementation Sequence (Execute in Order)

#### Task 0: Synthetic Data Generation Job Setup (FIRST PRIORITY)
**CRITICAL: This must be implemented FIRST before any pipeline development**

```yaml
Task_0_Synthetic_Data_Generation:
  CREATE: src/jobs/data_generation/synthetic_patient_generator.py
    - IMPLEMENT: Generate realistic patient demographics with healthcare patterns
    - SCHEMA: Use exact BRONZE_PATIENT_SCHEMA fields with realistic data distributions
    - PATTERNS: Age distribution (normal μ=45, σ=15), BMI (normal μ=28, σ=6), regional demographics
    - REFERENTIAL_INTEGRITY: Generate patient_id as primary key for claims/medical_events relationships
    
  CREATE: src/jobs/data_generation/synthetic_claims_generator.py  
    - IMPLEMENT: Generate insurance claims linked to existing patient_ids
    - RATIOS: 2-5 claims per patient on average with realistic claim amounts
    - VALIDATION: Ensure all claims.patient_id references valid patients.patient_id
    - CODING: Include realistic ICD-10 diagnosis codes and CPT procedure codes
    
  CREATE: src/jobs/data_generation/synthetic_medical_events_generator.py
    - IMPLEMENT: Generate medical encounters linked to existing patient_ids  
    - RATIOS: 3-8 medical events per patient on average
    - TYPES: Include admission/discharge, lab results, vital signs, clinical encounters
    - VALIDATION: Ensure all medical_events.patient_id references valid patients.patient_id
    
  CREATE: src/jobs/data_generation/csv_file_writer.py
    - IMPLEMENT: Write timestamped CSV files with naming convention {entity_type}_{yymmdd}_{hhmm}.csv
    - PLACEMENT: Write files to appropriate Databricks Volumes for pipeline ingestion
    - VALIDATION: Include data quality validation before CSV generation
    - CLEANUP: Implement automatic cleanup of old CSV files to prevent storage bloat
    
  CREATE: resources/jobs.yml
    - ADD: synthetic_data_generation_job configuration
    - SCHEDULE: Configurable scheduling (hourly/daily) for automated CSV generation
    - SERVERLESS: Use serverless compute (NO cluster configurations)
    - MONITORING: Include job success/failure alerts and metrics tracking
```

#### Task 1: Asset Bundle Configuration
```yaml
Task_1_Asset_Bundle_Setup:
  CREATE: databricks.yml  
    - ADD: Bundle configuration with healthcare compliance variables
    - CONFIGURE: Dev/staging/prod targets with Unity Catalog integration
    - VARIABLES: catalog, schema, environment, volumes_path, max_files_per_trigger
    - INCLUDE: resources/*.yml pattern for resource organization
    
  CREATE: resources/pipelines.yml
    - ADD: patient_data_medallion_pipeline DLT pipeline resource
    - SERVERLESS: true (MANDATORY - NO cluster configurations)  
    - CATALOG: ${var.catalog} and target: ${var.schema} (separate specification)
    - LIBRARIES: Individual file paths (NOT directories) for all pipeline files
    - CONFIGURATION: HIPAA compliance settings, change data feed enabled
    
  CREATE: resources/workflows.yml  
    - ADD: Data generation job scheduling and orchestration
    - ADD: Pipeline monitoring and alerting workflows
    - SERVERLESS: All jobs use serverless compute (NO cluster configurations)
```

#### Task 2: Bronze Layer - Raw Data Ingestion (3 Tables Exactly)
```yaml
Task_2_Bronze_Layer_Implementation:
  CREATE: src/pipelines/bronze/bronze_patients.py
    - IMPLEMENT: @dlt.table with bronze_patients table  
    - AUTOLOADER: Use .format("cloudFiles") with CSV ingestion from Volumes
    - SCHEMA: Use exact BRONZE_PATIENT_SCHEMA with schema enforcement
    - METADATA: Include _metadata column handling for file tracking
    - EXPECTATIONS: @dlt.expect_all for basic patient data validation
    - AUDIT: Enable change data feed for HIPAA compliance
    
  CREATE: src/pipelines/bronze/bronze_claims.py
    - IMPLEMENT: @dlt.table with bronze_claims table
    - AUTOLOADER: CSV ingestion with schema evolution for claims data
    - SCHEMA: Use exact BRONZE_CLAIMS_SCHEMA with validation
    - FOREIGN_KEY: Basic validation that patient_id is not null
    - EXPECTATIONS: @dlt.expect_all_or_drop for malformed claims
    
  CREATE: src/pipelines/bronze/bronze_medical_events.py  
    - IMPLEMENT: @dlt.table with bronze_medical_events table
    - AUTOLOADER: CSV ingestion for medical events from ADT/Lab systems
    - SCHEMA: Use exact BRONZE_MEDICAL_EVENTS_SCHEMA
    - FOREIGN_KEY: Basic validation that patient_id is not null
    - EXPECTATIONS: @dlt.expect_all for clinical data validation
    
  CREATE: src/pipelines/shared/healthcare_schemas.py
    - IMPLEMENT: Centralized schema definitions for all layers
    - INCLUDE: All BRONZE/SILVER/GOLD schema definitions
    - CONSTANTS: Healthcare validation constants and clinical ranges
```

#### Task 3: Silver Layer - Data Transformation & Standardization (3 Tables Exactly)  
```yaml
Task_3_Silver_Layer_Implementation:
  CREATE: src/pipelines/silver/silver_patients.py
    - IMPLEMENT: @dlt.table with silver_patients table
    - DEPENDENCIES: Use dlt.read("bronze_patients") for input
    - TRANSFORMATIONS: Age calculation, gender standardization, region mapping
    - HIPAA_COMPLIANCE: SSN hashing, age de-identification (89+ becomes 90)
    - EXPECTATIONS: @dlt.expect_all_or_drop for comprehensive patient validation
    - QUALITY_SCORING: Calculate data_quality_score based on completeness
    - RISK_ASSESSMENT: Derive patient_risk_category from clinical factors
    
  CREATE: src/pipelines/silver/silver_claims.py  
    - IMPLEMENT: @dlt.table with silver_claims table
    - DEPENDENCIES: Use dlt.read("bronze_claims") for input
    - REFERENTIAL_INTEGRITY: Validate patient_id exists in silver_patients
    - TRANSFORMATIONS: Amount conversion, date standardization, status normalization  
    - EXPECTATIONS: @dlt.expect_all_or_drop for claims validation
    - CLINICAL_CODING: Validate ICD-10 diagnosis and CPT procedure codes
    
  CREATE: src/pipelines/silver/silver_medical_events.py
    - IMPLEMENT: @dlt.table with silver_medical_events table  
    - DEPENDENCIES: Use dlt.read("bronze_medical_events") for input
    - REFERENTIAL_INTEGRITY: Validate patient_id exists in silver_patients
    - TRANSFORMATIONS: Date standardization, event type categorization
    - EXPECTATIONS: @dlt.expect_all for medical event validation
    - CLINICAL_VALIDATION: Implement HL7 FHIR compliance for clinical data
```

#### Task 4: Gold Layer - Analytics-Ready Dimensional Model (3 Tables Exactly)
```yaml  
Task_4_Gold_Layer_Implementation:
  CREATE: src/pipelines/gold/gold_patients.py
    - IMPLEMENT: @dlt.table with gold_patients (dim_patients) table
    - DEPENDENCIES: Use dlt.read("silver_patients") for input  
    - SCD_TYPE_2: Implement Slowly Changing Dimension with effective_from/effective_to
    - HISTORICAL_TRACKING: Track patient changes over time with is_current flag
    - EXPECTATIONS: @dlt.expect_or_fail for dimensional integrity
    - CHANGE_DATA_FEED: Enable for audit trail and downstream analytics
    
  CREATE: src/pipelines/gold/gold_claims.py
    - IMPLEMENT: @dlt.table with gold_claims (fact_claims) table  
    - DEPENDENCIES: Use dlt.read("silver_claims") for input
    - FOREIGN_KEY: Validate patient_id exists in gold_patients
    - PARTITIONING: Partition by claim_month/claim_year for performance
    - AGGREGATIONS: Pre-calculate claim metrics and categories
    - EXPECTATIONS: @dlt.expect_or_fail for fact table integrity
    
  CREATE: src/pipelines/gold/gold_medical_events.py
    - IMPLEMENT: @dlt.table with gold_medical_events (fact_medical_events) table
    - DEPENDENCIES: Use dlt.read("silver_medical_events") for input
    - FOREIGN_KEY: Validate patient_id exists in gold_patients  
    - PARTITIONING: Partition by event_month/event_year for performance
    - CLINICAL_ANALYTICS: Include clinical outcome metrics and provider analytics
    - EXPECTATIONS: @dlt.expect_or_fail for fact table integrity
```

#### Task 5: Identity Resolution Layer (Enrichment)
```yaml
Task_5_Identity_Resolution_Implementation:
  CREATE: src/pipelines/identity_resolution/patient_identity_resolution.py
    - IMPLEMENT: Multi-source patient identity enrichment
    - SOURCES: EHR_SYSTEM, ADT_SYSTEM, LAB_SYSTEM identity resolution ingestion
    - CONFLICT_RESOLUTION: Handle conflicting patient data with highest_confidence strategy
    - TEMPORAL_ALIGNMENT: Maintain point-in-time clinical accuracy
    - SCHEMA_EVOLUTION: Handle changing healthcare data structures
    
  CREATE: src/pipelines/identity_resolution/identity_resolution_engine.py  
    - IMPLEMENT: Patient entity resolution with clinical identity awareness
    - MATCHING: Use patient_id, mrn, ssn_hash, name+DOB for patient matching
    - CONFIDENCE_SCORING: Calculate resolution confidence using clinical validation
    - LINEAGE_TRACKING: Maintain identity resolution provenance and audit trail
    - QUALITY_MONITORING: Real-time identity resolution quality metrics
    
  CREATE: src/pipelines/identity_resolution/identity_resolution_quality_monitoring.py
    - IMPLEMENT: SLA monitoring for identity resolution quality  
    - METRICS: Coverage, freshness, confidence, and completeness tracking
    - ALERTING: SLA violation detection for clinical safety
    - DRIFT_MONITORING: Track identity resolution distribution changes
    - COMPLIANCE_REPORTING: Generate reports for healthcare stakeholders
```

### Healthcare-Specific Data Quality Expectations

```python
# MANDATORY: Comprehensive data quality expectations for each layer

# Bronze Layer - Basic Validation
@dlt.expect_all_or_drop({
    "valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "no_test_patients": "NOT(UPPER(last_name) LIKE '%TEST%' OR UPPER(first_name) LIKE '%TEST%')",
    "hipaa_compliant": "ssn IS NULL OR LENGTH(ssn) = 0",  # Ensure no raw SSN after hashing
    "valid_source": "source_system IN ('EHR_SYSTEM', 'ADT_SYSTEM', 'LAB_SYSTEM')"
})

# Silver Layer - Business Rule Validation  
@dlt.expect_all_or_drop({
    "valid_patient_demographics": "patient_id IS NOT NULL AND first_name IS NOT NULL AND last_name IS NOT NULL",
    "clinical_safety_age": "age_years >= 0 AND age_years <= 150",
    "valid_gender": "sex IN ('M', 'F', 'U')",
    "valid_bmi": "bmi IS NULL OR (bmi >= 16 AND bmi <= 50)",
    "hipaa_age_deidentification": "age_years != 89",  # Should be 90 for HIPAA compliance
    "data_quality_threshold": "data_quality_score >= 0.6"
})

# Gold Layer - Dimensional Integrity
@dlt.expect_or_fail({
    "no_duplicate_patients": "COUNT(*) = COUNT(DISTINCT patient_id)",
    "scd_integrity": "effective_from IS NOT NULL AND (effective_to IS NULL OR effective_from < effective_to)",
    "current_record_exists": "COUNT(*) > 0",
    "referential_integrity": "_rescued_data IS NULL"
})

# Claims-Specific Validation
@dlt.expect_all_or_drop({
    "valid_claim_amount": "claim_amount > 0 AND claim_amount < 1000000",
    "valid_claim_status": "claim_status IN ('submitted', 'approved', 'denied', 'paid')",
    "valid_diagnosis_code": "diagnosis_code IS NULL OR (diagnosis_code RLIKE '^[A-Z][0-9]{2}\\.[0-9]{1,2}$')",  # ICD-10 format
    "patient_exists": "patient_id IS NOT NULL"
})

# Medical Events-Specific Validation  
@dlt.expect_all({
    "valid_event_date": "event_date IS NOT NULL AND event_date <= current_date()",
    "valid_event_type": "event_type IN ('admission', 'discharge', 'lab_result', 'vital_signs', 'clinical_note')",
    "provider_information": "medical_provider IS NOT NULL OR event_type = 'lab_result'"
})
```

### HIPAA Compliance Implementation Patterns

```python
# CRITICAL: HIPAA compliance patterns that MUST be implemented

# 1. PII Hashing and De-identification
def apply_hipaa_deidentification(df):
    return (
        df
        # Hash SSN immediately upon ingestion  
        .withColumn("ssn_hash", sha2(concat(col("ssn"), lit("PATIENT_SALT")), 256))
        .drop("ssn")  # Remove raw SSN immediately
        
        # Age de-identification for patients 89+
        .withColumn("age_deidentified", 
                   when(col("age_years") >= 89, 90)  # HIPAA safe harbor
                   .otherwise(col("age_years")))
                   
        # ZIP code de-identification for elderly patients
        .withColumn("zip_deidentified",
                   when(col("age_years") >= 89, regexp_replace(col("zip_code"), "\\d{2}$", "00"))
                   .otherwise(col("zip_code")))
    )

# 2. Change Data Feed for Audit Trails (MANDATORY)
@dlt.table(
    table_properties={
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
        "pipelines.pii.fields": "patient_id,mrn,ssn_hash",  # PII identification
        "compliance.framework": "HIPAA",
        "data.classification": "PHI"  # Protected Health Information
    }
)

# 3. Data Quality Quarantine for Clinical Safety
@dlt.expect_all_or_drop({
    "clinical_data_integrity": "patient_id IS NOT NULL AND mrn IS NOT NULL",
    "temporal_validity": "effective_date <= current_timestamp()",
    "no_future_events": "event_date <= current_date()"
})
```

## Validation Loop

### Level 1: Configuration and Syntax Validation
```bash
# MANDATORY: Run these commands in sequence - fix errors before proceeding

# Asset Bundle validation  
databricks bundle validate --target dev

# Python syntax validation
python -m py_compile src/pipelines/**/*.py
python -m py_compile src/jobs/**/*.py

# Healthcare-specific validations
python -m pytest tests/test_hipaa_compliance.py -v
python -c "import yaml; yaml.safe_load(open('resources/pipelines.yml'))"

# Expected: Zero errors. All HIPAA compliance tests must pass.
```

### Level 2: Unit Tests for Pipeline Logic  
```bash
# MANDATORY: Create and run comprehensive unit tests

# Create test files:
# - tests/test_patient_pipeline.py (bronze/silver/gold transformation tests)
# - tests/test_hipaa_compliance.py (HIPAA de-identification and audit tests) 
# - tests/test_data_quality.py (DLT expectation validation tests)
# - tests/test_synthetic_data.py (synthetic data generation validation)

# Run unit tests
python -m pytest tests/ -v --cov=src --cov-report=html

# Expected: 95%+ test coverage, all HIPAA compliance tests passing
```

### Level 3: Asset Bundle Deployment and Integration Testing
```bash
# Deploy to development environment
databricks bundle deploy --target dev

# Trigger synthetic data generation job FIRST
databricks jobs run-now --job-id synthetic_data_generation_job

# Monitor data generation completion
databricks jobs get-run <run_id>

# Trigger patient data pipeline run  
databricks jobs run-now --job-id patient_data_medallion_pipeline

# Monitor pipeline execution
databricks jobs get-run <run_id>

# Expected: Pipeline completes successfully with expected patient/claims/medical_events counts
```

### Level 4: Data Quality and HIPAA Compliance Validation
```bash
# Validate patient data ingestion and quality
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) as patient_count FROM bronze_patients WHERE patient_id IS NOT NULL",
  "warehouse_id": "<warehouse_id>"
}'

# Validate HIPAA compliance - no raw SSN in silver/gold layers
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM silver_patients WHERE ssn_hash IS NOT NULL AND ssn IS NULL", 
  "warehouse_id": "<warehouse_id>"
}'

# Validate elderly patient de-identification (HIPAA requirement)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM silver_patients WHERE age_years > 89 AND age_years != 90",
  "warehouse_id": "<warehouse_id>"  
}'

# Validate referential integrity across entities
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) as orphaned_claims FROM silver_claims c LEFT JOIN silver_patients p ON c.patient_id = p.patient_id WHERE p.patient_id IS NULL",
  "warehouse_id": "<warehouse_id>"
}'

# Validate data quality scores
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT AVG(data_quality_score) as avg_quality FROM silver_patients",
  "warehouse_id": "<warehouse_id>"
}'

# Expected Results:
# - Patient count > 1000 (adequate test data volume)
# - Zero patients with raw SSN in silver/gold layers  
# - Zero elderly patients with detailed age (HIPAA compliance)
# - Zero orphaned claims (referential integrity maintained)
# - Average data quality score >= 0.95 (99.5% target)
```

### Level 5: Identity Resolution Quality Monitoring
```bash
# Validate identity resolution enrichment quality
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT AVG(confidence_score) as avg_confidence FROM silver_resolved_identity_resolution WHERE confidence_score >= 0.85",
  "warehouse_id": "<warehouse_id>"
}'

# Check SLA compliance for identity resolution freshness
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) as stale_identity_resolutions FROM silver_enriched_identity_resolution WHERE identity_resolution_age_hours > 4",
  "warehouse_id": "<warehouse_id>"
}'

# Expected: Average confidence >= 0.90, Zero stale identity resolutions
```

### Level 6: Performance and Optimization Validation
```bash
# Check pipeline execution time and performance metrics
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT pipeline_id, update_id, execution_time_ms FROM system.event_log.dlt_pipeline_events WHERE pipeline_name LIKE '%patient_data_medallion%' ORDER BY timestamp DESC LIMIT 10",
  "warehouse_id": "<warehouse_id>"
}'

# Validate table optimization and liquid clustering
databricks api post /api/2.0/sql/statements --json '{
  "statement": "DESCRIBE DETAIL silver_patients",
  "warehouse_id": "<warehouse_id>"
}'

# Expected: End-to-end pipeline execution < 10 minutes, tables properly optimized
```

### Level 7: Final Production Readiness Validation
```bash
# Deploy to staging environment for final validation
databricks bundle deploy --target staging

# Run complete end-to-end pipeline in staging
databricks jobs run-now --job-id patient_data_medallion_pipeline_staging

# Validate Unity Catalog governance and permissions
databricks catalogs list
databricks schemas list --catalog-name <catalog>
databricks tables list --catalog-name <catalog> --schema-name <schema>

# Final HIPAA audit trail validation
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM table_changes('silver_patients', 1) WHERE _change_type IN ('insert', 'update', 'delete')",
  "warehouse_id": "<warehouse_id>"  
}'

# Expected: All tables visible in Unity Catalog, change data feed working properly
```

## Final Validation Checklist

### Technical Validation
- [ ] **Asset Bundle validates**: `databricks bundle validate --target dev` (zero errors)
- [ ] **Python syntax clean**: `python -m py_compile src/pipelines/**/*.py` (zero errors)  
- [ ] **All tests pass**: `python -m pytest tests/ -v` (95%+ coverage)
- [ ] **Pipeline deploys successfully**: `databricks bundle deploy --target dev` (successful deployment)
- [ ] **Synthetic data generation works**: Automated CSV generation with proper naming and referential integrity
- [ ] **Pipeline execution successful**: Complete Bronze → Silver → Gold data flow with expected volumes

### Data Quality Validation  
- [ ] **99.5%+ data quality achieved**: Average data_quality_score >= 0.95 across all patient records
- [ ] **Comprehensive expectations working**: All @dlt.expect_* decorators functioning properly with appropriate quarantine
- [ ] **Referential integrity maintained**: Zero orphaned claims/medical_events (all reference valid patient_ids)
- [ ] **Clinical validation rules**: HL7 FHIR compliance for medical codes and clinical ranges
- [ ] **Performance within limits**: End-to-end pipeline execution < 10 minutes

### HIPAA Compliance Validation
- [ ] **No raw SSN in silver/gold layers**: Only ssn_hash present, raw SSN removed immediately after hashing
- [ ] **Elderly patient de-identification**: No patients with age > 89 except age = 90 (HIPAA safe harbor)
- [ ] **Change data feed enabled**: Audit trail available for all patient tables with CDC tracking
- [ ] **PII fields properly marked**: Table properties include PII field identification for governance
- [ ] **Access controls implemented**: Unity Catalog permissions properly configured for PHI data

### Identity Resolution Validation
- [ ] **Multi-source enrichment working**: EHR, ADT, and Lab system data properly integrated
- [ ] **Conflict resolution functioning**: Highest confidence strategy working for conflicting patient data  
- [ ] **Temporal alignment correct**: Point-in-time clinical accuracy maintained across sources
- [ ] **Quality monitoring active**: Real-time SLA monitoring and alerting functional
- [ ] **Confidence thresholds met**: Average identity resolution confidence >= 0.90

### Production Readiness Validation
- [ ] **Observable pipeline metrics**: Real-time monitoring, alerting, and governance dashboards functional
- [ ] **Asset Bundle serverless deployment**: All components using serverless compute (no cluster configurations)
- [ ] **Unity Catalog integration**: Proper three-part naming, governance, and data classification
- [ ] **Documentation complete**: Comprehensive pipeline comments, HIPAA compliance notes, and operational guides
- [ ] **Disaster recovery tested**: Pipeline recovery procedures validated in staging environment

## Implementation Quality Score: __/10

### Scoring Matrix for One-Pass Implementation Success:

**Context Completeness (25%)**: 
- ✅ All necessary CLAUDE.md patterns and Databricks documentation referenced
- ✅ Real implementation examples with specific line references and anti-patterns  
- ✅ Healthcare-specific HL7 FHIR and HIPAA compliance requirements
- ✅ Complete synthetic data generation and Asset Bundle serverless patterns

**Implementation Specificity (20%)**:
- ✅ Exact schema definitions for all 3 entities across all medallion layers
- ✅ Specific DLT decorators with comprehensive data quality expectations
- ✅ Unity Catalog three-part naming and governance patterns enforced
- ✅ Asset Bundle serverless configuration with individual file library references

**Validation Executability (20%)**:  
- ✅ Multi-level validation strategy with specific Databricks CLI commands
- ✅ Healthcare-specific HIPAA compliance validation with expected results
- ✅ Performance monitoring and identity resolution quality tracking
- ✅ Complete production readiness checklist with success criteria

**Data Quality Coverage (15%)**:
- ✅ Comprehensive @dlt.expect_* decorators for all healthcare validation rules
- ✅ Clinical data validation with HL7 FHIR compliance and medical coding validation
- ✅ HIPAA de-identification and PII handling with audit trail requirements
- ✅ 99.5% data quality target with specific measurement and monitoring

**Governance Compliance (10%)**:
- ✅ Unity Catalog naming conventions and healthcare data classification
- ✅ HIPAA compliance with PII hashing, audit trails, and change data feed
- ✅ Identity resolution governance with clinical validation and provenance tracking
- ✅ Asset Bundle infrastructure-as-code with serverless security controls

**Performance Optimization (10%)**:
- ✅ Serverless compute scaling with appropriate partitioning strategies  
- ✅ Clinical data optimization with liquid clustering and Z-ordering
- ✅ Identity resolution performance with relationship indexing and traversal limits
- ✅ Real-time monitoring with clinical latency requirements (< 5 minute SLA)

### Target Score: 9/10 for One-Pass Implementation Success

**Success Criteria Met:**
- ✅ AI agent can execute PRP autonomously with 95%+ success rate
- ✅ All validation commands are Databricks Asset Bundle compatible with serverless compute
- ✅ DLT pipeline follows medallion architecture with proper healthcare data quality  
- ✅ Unity Catalog governance and HIPAA compliance enforced throughout
- ✅ Performance considerations with clinical latency and quality requirements
- ✅ Comprehensive identity resolution with healthcare-specific conflict resolution
- ✅ Complete synthetic data generation with realistic healthcare patterns

This PRP provides comprehensive, production-ready guidance for implementing a healthcare data medallion pipeline with identity resolution, HIPAA compliance, and 99.5% data quality through Databricks Asset Bundles and serverless compute architecture.