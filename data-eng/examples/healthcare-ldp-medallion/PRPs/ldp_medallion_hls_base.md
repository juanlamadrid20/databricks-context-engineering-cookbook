# Healthcare LDP Medallion Architecture PRP

## Purpose
Implement a comprehensive healthcare data pipeline using Delta Live Tables with identity resolution and medallion architecture for health insurance patient data.

## Goal
**Build a mature health insurance patient data medallion pipeline using Delta Live Tables that implements comprehensive data quality controls, governance, and observability for patient demographics, claims, and medical events processing.**

## Why
- **Business value**: Establish a production-ready data platform for health insurance analytics with proper governance
- **Greenfield implementation**: Build from scratch focusing on health insurance patient workflows  
- **Data quality & compliance**: Ensure accurate, complete, and HIPAA-compliant patient data processing

## What
**A complete medallion architecture (Bronze → Silver → Gold) pipeline that processes health insurance patient data with identity resolution, real-time monitoring, data quality validation, and healthcare compliance controls.**

### Success Criteria
- [ ] **Exactly 3 business entities**: Patients, Claims, Medical Events (strictly enforced domain model)
- [ ] **Complete medallion pipeline**: Bronze → Silver → Gold layers with proper data quality expectations
- [ ] **99.5% data quality score** with comprehensive validation rules and HIPAA compliance
- [ ] **Identity resolution implementation**: Multi-source identity resolution ingestion, resolution, and enrichment
- [ ] **Observable pipeline metrics**: Real-time monitoring, alerting, and data governance dashboards
- [ ] **Asset Bundle deployment**: Infrastructure-as-code with serverless compute configuration
- [ ] **Synthetic data generation job**: Automated CSV generation for EHR/ADT/Lab systems with timestamped filenames

### Entity Model Clarification
**Business Entities (What We're Building):**
- **Patients**: Patient demographic, health, and insurance data
- **Claims**: Insurance claims with diagnosis and procedure codes
- **Medical Events**: Medical history, encounters, and clinical events

**Data Sources (Where Data Comes From):**
- **EHR_SYSTEM**: Electronic Health Records (raw patient data)
- **ADT_SYSTEM**: Admit/Discharge/Transfer (encounter data)
- **LAB_SYSTEM**: Laboratory results (clinical data)

### Data Source Enrichment Domain Model

**🚨 CRITICAL: This domain model shows EXACTLY how data sources enrich the 3 core business entities**

```
Data Sources → Core Business Entities Enrichment Model
│
├── EHR_SYSTEM (Electronic Health Records)
│   ├── PRIMARY ENRICHMENT: bronze_patients → silver_patients
│   │   ├── Raw patient demographics (first_name, last_name, DOB, gender, address)
│   │   ├── Insurance information (insurance_id, coverage_details)
│   │   ├── Contact information (phone, email, emergency_contact)
│   │   └── Clinical identifiers (mrn, ssn_hash, primary_care_provider)
│   │
│   ├── SECONDARY ENRICHMENT: bronze_medical_events → silver_medical_events
│   │   ├── Patient encounter history
│   │   ├── Clinical documentation
│   │   └── Care provider assignments
│   │
│   └── ENRICHMENT PATTERN: Direct entity population with demographic and clinical context
│
├── ADT_SYSTEM (Admit/Discharge/Transfer)
│   ├── PRIMARY ENRICHMENT: bronze_medical_events → silver_medical_events
│   │   ├── Hospital admission events
│   │   ├── Discharge summaries
│   │   ├── Transfer events between units
│   │   ├── Length of stay calculations
│   │   └── Care unit assignments
│   │
│   ├── SECONDARY ENRICHMENT: bronze_patients → silver_patients
│   │   ├── Recent encounter history
│   │   ├── Care episode context
│   │   └── Hospital utilization patterns
│   │
│   └── ENRICHMENT PATTERN: Temporal event enrichment with care coordination context
│
├── LAB_SYSTEM (Laboratory Results)
│   ├── PRIMARY ENRICHMENT: bronze_medical_events → silver_medical_events
│   │   ├── Lab test results (blood work, imaging, pathology)
│   │   ├── Vital signs (blood pressure, heart rate, temperature)
│   │   ├── Clinical measurements (height, weight, BMI)
│   │   ├── Medication levels and drug monitoring
│   │   └── Disease markers and diagnostic indicators
│   │
│   ├── SECONDARY ENRICHMENT: bronze_patients → silver_patients
│   │   ├── Health status indicators
│   │   ├── Risk factor assessments
│   │   ├── Clinical trend analysis
│   │   └── Preventive care recommendations
│   │
│   └── ENRICHMENT PATTERN: Clinical measurement enrichment with health analytics context
│
└── IDENTITY RESOLUTION LAYER
    ├── PATTERN: Multi-source identity resolution and conflict handling
    ├── PURPOSE: Resolve patient identity across systems and enrich with comprehensive clinical identity resolution
    ├── OUTPUT: Enhanced silver and gold layer entities with resolved, high-confidence identity resolution
    └── QUALITY: 99.5% data quality with clinical validation and HIPAA compliance
```

**Key Enrichment Relationships:**
1. **EHR_SYSTEM** → **Patients**: Primary demographic and insurance data source
2. **ADT_SYSTEM** → **Medical Events**: Primary encounter and care coordination data source  
3. **LAB_SYSTEM** → **Medical Events**: Primary clinical measurement and diagnostic data source
4. **Cross-System Enrichment**: All systems contribute to patient context resolution and clinical 360° view

### Data Source Field Mapping to Business Entities

**🚨 CRITICAL: This mapping shows EXACTLY which fields from each source system populate which business entities**

| Data Source | Source Fields | Target Entity | Target Fields | Enrichment Type |
|-------------|---------------|---------------|---------------|-----------------|
| **EHR_SYSTEM** | `patient_id`, `mrn`, `first_name`, `last_name`, `date_of_birth`, `gender`, `address_line1`, `city`, `state`, `zip_code`, `phone`, `email`, `ssn`, `insurance_id`, `primary_care_provider` | **bronze_patients** | `patient_id`, `mrn`, `first_name`, `last_name`, `date_of_birth`, `gender`, `address_line1`, `city`, `state`, `zip_code`, `phone`, `email`, `ssn_hash`, `insurance_id`, `primary_care_provider` | **Primary Population** |
| **EHR_SYSTEM** | `patient_id`, `encounter_date`, `encounter_type`, `provider_id`, `clinical_notes` | **bronze_medical_events** | `event_id`, `patient_id`, `event_date`, `event_type`, `medical_provider` | **Secondary Enrichment** |
| **ADT_SYSTEM** | `patient_id`, `admission_date`, `discharge_date`, `unit_from`, `unit_to`, `length_of_stay`, `discharge_disposition` | **bronze_medical_events** | `event_id`, `patient_id`, `event_date`, `event_type`, `medical_provider`, `event_details` | **Primary Population** |
| **ADT_SYSTEM** | `patient_id`, `last_encounter_date`, `total_admissions`, `avg_length_of_stay` | **bronze_patients** | `patient_id`, `encounter_history`, `utilization_metrics` | **Secondary Enrichment** |
| **LAB_SYSTEM** | `patient_id`, `test_date`, `test_type`, `test_result`, `reference_range`, `abnormal_flag`, `vital_signs` | **bronze_medical_events** | `event_id`, `patient_id`, `event_date`, `event_type`, `medical_provider`, `clinical_results` | **Primary Population** |
| **LAB_SYSTEM** | `patient_id`, `bmi`, `blood_pressure`, `heart_rate`, `temperature`, `risk_factors` | **bronze_patients** | `patient_id`, `bmi`, `health_metrics`, `risk_assessment` | **Secondary Enrichment** |

**Enrichment Flow Summary:**
- **EHR_SYSTEM**: Primary source for patient demographics and insurance data
- **ADT_SYSTEM**: Primary source for hospital encounter and care coordination events  
- **LAB_SYSTEM**: Primary source for clinical measurements and diagnostic results
- **Cross-System Context**: All systems contribute to patient identity resolution and comprehensive clinical context

## Context & Requirements

### Identity Resolution Specifications
```yaml
# Identity Resolution Definition & Scope
identity_resolution_domain: patient_healthcare # Patient clinical, demographic, and care coordination identity resolution

# Data Sources for Identity Resolution Enrichment
identity_resolution_sources:
  - source: EHR_SYSTEM # Electronic Health Records system
    type: streaming # Real-time patient updates
    refresh_pattern: continuous # Critical patient data updates
    latency_requirements: 30_seconds # Maximum acceptable identity resolution staleness for patient safety
    purpose: "Primary source for patient demographic and clinical data"
  - source: ADT_SYSTEM # Admit/Discharge/Transfer system
    type: streaming # Real-time admission events
    refresh_pattern: continuous 
    latency_requirements: 15_seconds
    purpose: "Enrich patient data with encounter and care episode identity resolution"
  - source: LAB_SYSTEM # Laboratory results system
    type: batch # Lab results updates
    refresh_pattern: hourly # Lab results processed hourly
    latency_requirements: 1_hour
    purpose: "Enrich patient data with clinical lab results and vitals identity resolution"
    
# Identity Resolution for Business Entities
identity_resolution:
  - entity_type: patients # Primary entities requiring identity resolution
    resolution_keys: patient_id, mrn, ssn_hash, first_name_last_name_dob # Keys used for patient matching
    conflict_strategy: highest_confidence # Medical data requires highest confidence resolution
    temporal_strategy: point_in_time # Clinical identity resolution must be temporally accurate
    
identity_resolution_quality_requirements:
  - completeness_threshold: 98% # Minimum patient identity resolution coverage for clinical safety
  - freshness_sla: 5_minutes # Maximum identity resolution age for active patients
  - consistency_rules: clinical_validation_rules # Cross-system clinical data validation
  - confidence_thresholds: 0.95 # Minimum confidence scores for patient identification

identity_resolution_graph_structure:
  - relationship_types: hierarchical_care_team, patient_episodes, clinical_pathways # Healthcare relationships
  - traversal_depth: 3 # Maximum relationship hops for care coordination
  - graph_algorithms: care_pathway_analysis, clinical_decision_support # Healthcare-specific algorithms

# IMPORTANT: Identity resolution ENRICHES the 3 business entities (Patients, Claims, Medical Events)
# It does NOT replace or duplicate the core entity model
```

### Documentation & References (list all context needed to implement the feature)
```yaml
# MUST READ - Include these in your context window

# Primary Development Reference
- file: CLAUDE.md
  why: Complete Databricks development patterns, Asset Bundle configurations, DLT pipeline examples, and all external documentation references
  extract: "All sections for comprehensive context - see Consolidated Documentation References section for external URLs"

# Domain-Specific Healthcare Documentation  
- url: https://www.hl7.org/fhir/patient.html
  why: FHIR Patient resource specifications for healthcare data standards and validation rules
  
- url: https://www.hipaajournal.com/hipaa-compliance-checklist/
  why: HIPAA compliance requirements for patient data handling, encryption, and audit logging

```

### Current Codebase tree (run `tree` in the root of the project) to get an overview of the codebase
```bash
# Greenfield implementation - no existing patient data pipeline codebase
# Starting with basic project structure:
dbrx-ctxeng-de/
  - CLAUDE.md
  - PRPs/
    - prp_base.md
    - templates/
  - README.md
```

### This is a reference example of codebase tree with files to be added and responsibility of file. 
```bash
# Patient Data Medallion Pipeline Structure (Entity-Based Approach):
src/
  pipelines/
    bronze/                             # Raw data ingestion
      bronze_{entity}.py                # Raw entity data ingestion
      .
      .
    silver/
      silver_{entity}.py                # Cleaned and standardized entity data with HIPAA compliance
      .
      .
    gold/
      gold_{entity}.py                  # entity dimension table with SCD Type 2
    identity_resolution/                # Identity resolution layer (enriches business entities)
              patient_identity_resolution.py     # Enrich patients with EHR/ADT/Lab identity resolution
        identity_resolution.py             # Resolve patient identity resolution across systems
        identity_resolution_quality_monitoring.py     # Monitor identity resolution quality and SLA compliance
  data_generation/
    synthetic_patient_generator.py      # Generate realistic patient/claims/medical_events/adt/lab data
databricks.yml                         # Asset Bundle configuration for patient_data_medallion_pipeline
resources/
  pipelines.yml                        # Pipeline configuration with healthcare compliance settings
  workflows.yml                        # Monitoring, Synthetic Data Generation, and data quality jobs
tests/
  test_patient_pipeline.py             # Unit tests for patient data transformations and quality rules
  test_hipaa_compliance.py             # HIPAA compliance validation tests
```

### Known Gotchas of our codebase & Databricks Quirks
```python
# CRITICAL: Delta Live Tables requires specific decorators and patterns
# Example: @dlt.table() functions must return DataFrames, not display()
# Example: Asset Bundles use specific variable interpolation syntax ${var.environment}
# Example: Pipeline dependencies must be explicit using dlt.read() or dlt.read_stream()

# HEALTHCARE DATA SPECIFIC GOTCHAS:
# CRITICAL: Healthcare audit logs are required for compliance - enable Delta change data feed on all patient tables
# CRITICAL: Patient data requires strict PII handling - never log actual patient identifiers
# CRITICAL: Healthcare data has strict temporal requirements - always include effective dates and version tracking
# CRITICAL: Patient matching requires deterministic hashing for SSN/DOB - use consistent salt across pipeline
# CRITICAL: Clinical data validation must follow HL7 FHIR standards for interoperability
# CRITICAL: Medical Record Numbers (MRN) may not be unique across healthcare systems - implement proper entity resolution
# CRITICAL: Lab results and vitals have specific value ranges - implement clinical validation rules
# CRITICAL: Patient consent and data retention policies must be enforced at the data layer
# CRITICAL: Delta Live Tables streaming with patient data requires careful ordering for clinical accuracy
# CRITICAL: Healthcare audit logs are required for compliance - enable Delta change data feed on all patient tables
```
## ARCHITECTURE

## Implementation Blueprint

### Core Business Entity Schemas

Create clear, focused schemas for the 3 core business entities across all medallion layers.

```python
# Core Business Entity Schemas
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType, DateType

# PATIENT ENTITY SCHEMAS
# Bronze: Raw EHR format
BRONZE_PATIENT_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),        # Primary patient identifier
    StructField("mrn", StringType(), True),                # Medical Record Number
    StructField("first_name", StringType(), True),         # Raw first name from EHR
    StructField("last_name", StringType(), True),          # Raw last name from EHR
    StructField("date_of_birth", StringType(), True),      # Raw DOB from EHR (various formats)
    StructField("gender", StringType(), True),             # Raw gender from EHR (various formats)
    StructField("address_line1", StringType(), True),      # Raw address from EHR
    StructField("city", StringType(), True),               # Raw city from EHR
    StructField("state", StringType(), True),              # Raw state from EHR
    StructField("zip_code", StringType(), True),           # Raw ZIP from EHR
    StructField("phone", StringType(), True),              # Raw phone from EHR
    StructField("email", StringType(), True),              # Raw email from EHR
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
    StructField("bmi", DoubleType(), True),                # Calculated BMI if available
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

# Gold: Analytics-ready dimension
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

# CLAIMS ENTITY SCHEMAS
# Bronze: Raw claims format
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

# Silver: Standardized claims format
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

# Gold: Analytics-ready fact table
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

# MEDICAL EVENTS ENTITY SCHEMAS
# Bronze: Raw medical events format
BRONZE_MEDICAL_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),          # Primary event identifier
    StructField("patient_id", StringType(), False),        # Foreign key to patients
    StructField("event_date", StringType(), True),         # Raw event date (various formats)
    StructField("event_type", StringType(), True),         # Raw event type
    StructField("medical_provider", StringType(), True),   # Raw provider information
    StructField("source_system", StringType(), False),     # ADT/Lab system identifier
    StructField("_ingested_at", TimestampType(), True)     # Pipeline metadata
])

# Silver: Standardized medical events format
SILVER_MEDICAL_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),          # Primary event identifier
    StructField("patient_id", StringType(), False),        # Foreign key to patients
    StructField("event_date", DateType(), True),           # Standardized event date
    StructField("event_type", StringType(), True),         # Standardized event type
    StructField("medical_provider", StringType(), True),   # Cleaned provider information
    StructField("event_category", StringType(), True),     # Derived event category
    StructField("processed_at", TimestampType(), True)     # Pipeline metadata
])

# Gold: Analytics-ready fact table
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

### list of tasks to be completed to fullfill the PRP in the order they should be completed

```yaml
Task 0: Synthetic Data Generation Job Setup
CREATE OR MODIFY resources/jobs.yml:
  - ADD synthetic_data_generation_job for automated CSV file creation
  - CONFIGURE job to run on schedule (hourly/daily) generating timestamped CSV files
  - IMPLEMENT file naming convention: {entity_type}_{yymmdd}_{hhmm}.csv
  - PLACE generated files in appropriate Databricks Volumes for pipeline ingestion
  - ENSURE referential integrity between patients, claims, and medical_events
  - SIMULATE realistic healthcare data patterns matching production EHR/ADT/Lab systems

Task 1: Asset Bundle Configuration
CREATE OR MODIFY databricks.yml:
  - ADD new pipeline resource for patient_data_medallion_pipeline
  - CONFIGURE target environments (dev, staging, prod) with HIPAA compliance settings
  - SET appropriate compute settings for healthcare_workload with encryption at rest

Task 2: Bronze Layer - Raw Data Ingestion
BRONZE PATIENTS
  - IMPLEMENT patient data ingestion from EHR system (raw format: first_name, last_name, DOB, etc.)
  - IMPLEMENT data quality expectations for patient_data_validation_rules with quarantine
  - ENABLE Delta change data feed for audit compliance

BRONZE CLAIMS
  - IMPLEMENT claims data ingestion from claims system
  - IMPLEMENT basic validation for claim structure and required fields

GOLD MEDICAL EVENTS
  - IMPLEMENT medical events ingestion from ADT/Lab systems
  - IMPLEMENT clinical validation rules for encounter data

Task 3: Silver Layer - Data Transformation & Standardization
SILVER PATIENTS
  - IMPLEMENT transformation from raw EHR format to standardized business format
  - TRANSFORM: first_name+last_name+DOB → age, sex, region
  - ADD HIPAA de-identification and data quality expectations
  - USE proper dlt.read() dependencies for bronze layer integration

SILVER CLAIMS
  - IMPLEMENT claims validation with referential integrity to silver_patients
  - ADD business rule validation for claim amounts and status

SILVER MEDICAL EVENTS
  - IMPLEMENT medical events validation with referential integrity to silver_patients
  - ADD clinical validation for event types and provider information

Task 4: Gold Layer - Analytics-Ready Dimensional Model

GOLD PATIENTS
  - IMPLEMENT SCD Type 2 patient dimension with complete patient 360 view
  - ADD historical tracking for patient changes over time

GOLD CLAIMS
  - IMPLEMENT claims fact table with pre-aggregated metrics
  - ADD foreign key to gold_patients with proper referential integrity

GOLD MEDICAL EVENTS
  - IMPLEMENT medical events fact table with clinical analytics
  - ADD foreign key to gold_patients with proper referential integrity

Identity Resolution Tasks (Enrichment Layer):

Task Identity-1: Identity Resolution Ingestion & Enrichment
CREATE src/pipelines/identity_resolution/patient_identity_resolution.py:
  - IMPLEMENT enrichment of silver_patients with additional EHR/ADT/Lab identity resolution
  - ADD patient identity resolution deduplication and clinical conflict detection logic
  - HANDLE patient identity resolution versioning and clinical temporal windows
  - IMPLEMENT schema evolution for changing healthcare data structures

Task Identity-2: Identity Resolution & Quality
CREATE src/pipelines/identity_resolution/identity_resolution.py:
  - IMPLEMENT patient entity resolution with clinical identity resolution awareness
  - ADD clinical conflict resolution logic using highest_confidence strategy
  - CALCULATE patient identity resolution confidence scores using clinical validation algorithms
  - MAINTAIN patient identity resolution lineage and clinical provenance tracking
  - HANDLE temporal clinical identity resolution alignment and care episode synchronization

Task Identity-3: Identity Resolution Analytics & Monitoring
CREATE src/pipelines/identity_resolution/identity_resolution_quality_monitoring.py:
  - IMPLEMENT real-time patient identity resolution quality monitoring with clinical SLA tracking
  - ADD alerting for patient identity resolution SLA violations and clinical data quality issues
  - CALCULATE patient identity resolution coverage and clinical completeness metrics
  - TRACK patient identity resolution drift and clinical distribution changes for data governance
  - GENERATE patient identity resolution quality reports for healthcare compliance stakeholders

```

### Per task pseudocode as needed added to each task

**🚨 CRITICAL: PSEUDOCODE USAGE GUIDANCE**

**This pseudocode section provides GENERIC PATTERNS and BEST PRACTICES for healthcare data pipeline development. It is NOT production-ready code and should NOT be copied directly.**

**How to Use This Pseudocode:**
1. **Study the patterns** - Understand the DLT decorators, data quality expectations, and healthcare compliance approaches
2. **Reference the structure** - Use the table organization and naming conventions as architectural guidance
3. **Implement your own logic** - Fill in all TODO placeholders, add proper imports, and implement complete business logic
4. **Customize for your needs** - Adapt the patterns to your specific healthcare data sources and business requirements
5. **Add production features** - Include proper error handling, logging, monitoring, and security controls

**What This Pseudocode Provides:**
- ✅ DLT table structure patterns with proper decorators
- ✅ Healthcare-specific data quality expectation examples
- ✅ HIPAA compliance pattern demonstrations
- ✅ Identity resolution architecture examples
- ✅ Bronze→Silver→Gold transformation patterns

**What This Pseudocode Does NOT Provide:**
- ❌ Complete, production-ready code
- ❌ All required imports and dependencies
- ❌ Complete business logic implementation
- ❌ Real data source configurations
- ❌ Error handling and monitoring code
- ❌ Security and access control implementations

**Example of Proper Usage:**
```python
# DON'T copy this pseudocode directly:
.withColumn("enriched_attributes",
           create_map(
               lit("relationship_count"), col("relationship_count").cast("string"),
               **<TODO: your_additional_derived_attributes>**  # ← Placeholder
           ))

# DO implement your own complete logic:
.withColumn("enriched_attributes",
           create_map(
               lit("relationship_count"), col("relationship_count").cast("string"),
               lit("centrality_score"), col("centrality_score").cast("string"),
               lit("age_hours"), col("age_hours").cast("string"),
               lit("risk_score"), col("calculated_risk_score").cast("string"),
               lit("care_coordination"), col("care_coordination_status").cast("string")
           ))
```
```python

# ============================================================================
# PSEUDOCODE EXAMPLES - REFERENCE PATTERNS ONLY
# ============================================================================
# 
# ⚠️  WARNING: This is pseudocode demonstrating best practices and patterns.
# ⚠️  DO NOT copy this code directly - implement your own complete logic.
# ⚠️  Use these examples to understand the structure and approach.
# ============================================================================

# Task 1 - Bronze Layer Patient EHR Ingestion
# Pseudocode with CRITICAL details for patient data ingestion
# NOTE: This shows the pattern - you need to implement the complete logic
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import current_timestamp, hash, sha2

@dlt.table(
    name="bronze_patient_ehr",
    comment="Raw patient EHR data ingestion with HIPAA compliance and audit logging",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableChangeDataFeed": "true",  # HIPAA audit requirement
    }
)
@dlt.expect_all({"hipaa_compliant": "ssn IS NULL OR LENGTH(ssn) = 0"})  # Ensure no raw SSN
def bronze_patient_ehr() -> DataFrame:
    # PATTERN: Always define schema for patient tables with HIPAA considerations
    schema = StructType([
        StructField("patient_id", StringType(), False),        # Primary patient identifier
        StructField("mrn", StringType(), True),                # Medical Record Number
        StructField("first_name", StringType(), True),         # Patient first name
        StructField("last_name", StringType(), True),          # Patient last name
        StructField("date_of_birth", StringType(), True),      # DOB as string for privacy
        StructField("gender", StringType(), True),             # Patient gender
        StructField("address_line1", StringType(), True),      # Patient address
        StructField("city", StringType(), True),               # Patient city
        StructField("state", StringType(), True),              # Patient state
        StructField("zip_code", StringType(), True),           # Patient ZIP
        StructField("phone", StringType(), True),              # Patient phone
        StructField("email", StringType(), True),              # Patient email
        StructField("emergency_contact_name", StringType(), True), # Emergency contact
        StructField("emergency_contact_phone", StringType(), True), # Emergency phone
        StructField("primary_care_provider", StringType(), True),   # PCP ID
        StructField("insurance_id", StringType(), True),       # Insurance identifier
        StructField("effective_date", TimestampType(), False), # When record becomes effective
        StructField("source_system", StringType(), False),     # EHR system identifier
        StructField("created_at", TimestampType(), True)       # Original creation timestamp
    ])
    
    # GOTCHA: Use streaming for real-time patient updates for clinical safety
    return (
        spark.readStream
        .option("cloudFiles.format", "json")  # Common EHR export format
        .option("cloudFiles.schemaLocation", "/mnt/healthcare/schemas/patient_ehr")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle EHR schema changes
        .schema(schema)
        .load("/mnt/healthcare/raw/ehr/patients")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_pipeline_version", lit("v1.0"))
        # CRITICAL: Hash PII for privacy compliance
        .withColumn("ssn_hash", 
                   when(col("ssn").isNotNull(), sha2(concat(col("ssn"), lit("PATIENT_SALT")), 256))
                   .otherwise(lit(None)))
        .drop("ssn")  # Remove raw SSN immediately after hashing
    )

# Task 2 - Silver Layer Patient Data Transformation
@dlt.table(
    name="silver_patient_master",
    comment="Cleaned and standardized patient data with clinical validation and HIPAA compliance",
    table_properties={
        "quality": "silver", 
        "delta.enableChangeDataFeed": "true",
        "pipelines.patient.compliance": "HIPAA"
    }
)
@dlt.expect_all({
    "valid_patient_timestamp": "effective_date IS NOT NULL",
    "valid_demographics": "patient_id IS NOT NULL AND first_name IS NOT NULL AND last_name IS NOT NULL",
    "clinical_safety": "age_years >= 0 AND age_years <= 150"
})
def silver_patient_master() -> DataFrame:
    # PATTERN: Reference bronze tables using dlt.read()
    bronze_ehr = dlt.read("bronze_patient_ehr")
    
    # CRITICAL: Apply clinical data transformations and HIPAA de-identification
    return (
        bronze_ehr
        .filter(col("patient_id").isNotNull())
        .withColumn("processed_at", current_timestamp())
        
        # Clinical data standardization
        .withColumn("age_years", 
                   floor(datediff(current_date(), to_date(col("date_of_birth"), "yyyy-MM-dd")) / 365.25))
        .withColumn("gender_standardized", 
                   when(upper(col("gender")).isin("M", "MALE"), "M")
                   .when(upper(col("gender")).isin("F", "FEMALE"), "F")
                   .otherwise("U"))  # Unknown
        
        # HIPAA de-identification - remove direct identifiers for age 89+
        .withColumn("age_deidentified",
                   when(col("age_years") >= 89, 90)  # HIPAA safe harbor
                   .otherwise(col("age_years")))
        .withColumn("zip_deidentified",
                   when(col("age_years") >= 89, regexp_replace(col("zip_code"), "\\d{2}$", "00"))
                   .otherwise(col("zip_code")))
        
        # Clinical data quality validations
        .withColumn("data_quality_score",
                   (when(col("first_name").isNotNull(), 0.2).otherwise(0) +
                    when(col("last_name").isNotNull(), 0.2).otherwise(0) +
                    when(col("date_of_birth").isNotNull(), 0.2).otherwise(0) +
                    when(col("mrn").isNotNull(), 0.2).otherwise(0) +
                    when(col("primary_care_provider").isNotNull(), 0.2).otherwise(0)))
        
        # Clinical context enrichment
        .withColumn("patient_risk_category",
                   when(col("age_deidentified") >= 65, "HIGH_RISK_AGE")
                   .when(col("emergency_contact_name").isNull(), "HIGH_RISK_SOCIAL")
                   .otherwise("STANDARD_RISK"))
        
        .select(
            col("patient_id"),
            col("mrn"),
            col("first_name"),
            col("last_name"), 
            col("age_deidentified").alias("age_years"),
            col("gender_standardized").alias("gender"),
            col("zip_deidentified").alias("zip_code"),
            col("phone"),
            col("email"),
            col("emergency_contact_name"),
            col("emergency_contact_phone"),
            col("primary_care_provider"),
            col("insurance_id"),
            col("ssn_hash"),
            col("patient_risk_category"),
            col("data_quality_score"),
            col("effective_date"),
            col("source_system"),
            col("processed_at")
        )
    )

# Identity Resolution Implementation Patterns

# Task Identity-1 - Multi-Source Identity Resolution Ingestion
import dlt
from pyspark.sql.functions import current_timestamp, col, when, lit, hash, row_number, lag
from pyspark.sql.window import Window

@dlt.table(
    name="bronze_identity_resolution_patient_clinical",  # Healthcare patient clinical identity resolution
    comment="Raw identity resolution data with temporal tracking and conflict detection",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
        "identity_resolution.domain": "patient_healthcare"  # Healthcare domain for patient clinical identity resolution
    }
)
@dlt.expect_all_or_drop({
    "valid_identity_resolution_id": "identity_resolution_id IS NOT NULL AND LENGTH(identity_resolution_id) > 0",
    "valid_entity_link": "entity_id IS NOT NULL AND entity_type IS NOT NULL",
    "temporal_validity": "effective_from IS NOT NULL",
    "future_timestamp": "effective_from <= current_timestamp()"
})
@dlt.expect_all({
    "identity_resolution_completeness": "identity_resolution_data IS NOT NULL",
    "reasonable_confidence": "confidence_score IS NULL OR (confidence_score >= 0.0 AND confidence_score <= 1.0)"
})
def bronze_identity_resolution_ingestion():
    """Pattern: Handle multiple identity resolution sources with schema evolution and deduplication"""
    schema = PATIENT_IDENTITY_RESOLUTION_SCHEMA  # Use healthcare-specific patient identity resolution schema from implementation blueprint
    
    return (
        spark.readStream
        .option("cloudFiles.format", "csv")  # Healthcare data commonly ingested as CSV from EHR systems
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .schema(schema)
        .load(f"{VOLUMES_PATH}/patient_identity_resolution")  # Healthcare data from Databricks Volumes
        .withColumn("ingested_at", current_timestamp())
        .withColumn("version", monotonically_increasing_id())
        .withColumn("identity_resolution_hash", hash(col("identity_resolution_data")))  # For deduplication
        .withColumn("_pipeline_env", lit(PIPELINE_ENV))
    )

# Task Identity-2 - Identity Resolution Engine with Conflict Handling
@dlt.table(
    name="silver_resolved_identity_resolution",
    comment="Entity-resolved identity resolution with conflict resolution and confidence scoring",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true"
    }
)
@dlt.expect_all({
    "identity_resolution_completeness": "COUNT(*) >= 100",  # Minimum patient identity resolutions for statistical significance in healthcare
    "temporal_consistency": "effective_from <= effective_to OR effective_to IS NULL",
    "confidence_threshold": "confidence_score >= 0.85"  # High confidence threshold required for healthcare/clinical decisions
})
@dlt.expect_or_fail({
    "no_duplicate_identity_resolutions": "COUNT(*) = COUNT(DISTINCT identity_resolution_id)",
    "resolution_integrity": "canonical_entity_id IS NOT NULL"
})
def identity_resolution():
    """Pattern: Resolve entity identity resolution with confidence scoring and conflict resolution"""
    bronze_identity_resolutions = dlt.read("bronze_identity_resolution_patient_clinical")  # Healthcare patient clinical identity resolution
    
    # Window for temporal conflict resolution
    temporal_window = Window.partitionBy("entity_id", "identity_resolution_type").orderBy("effective_from", "confidence_score")
    
    return (
        bronze_identity_resolutions
        # Calculate confidence scores based on source reliability and data completeness
        .withColumn("source_confidence_score", 
                   when(col("source_system").isin(["EHR_SYSTEM", "ADT_SYSTEM"]), 0.9)  # Trusted healthcare source systems
                   .when(col("source_system").isin(["LAB_SYSTEM", "PHARMACY_SYSTEM"]), 0.7)  # Known healthcare source systems
                   .otherwise(0.5))
        .withColumn("completeness_score",
                   (when(col("identity_resolution_data.patient_id").isNotNull(), 0.25).otherwise(0) + 
                   when(col("identity_resolution_data.medical_record_number").isNotNull(), 0.25).otherwise(0) + 
                   when(col("identity_resolution_data.diagnosis_code").isNotNull(), 0.25).otherwise(0) + 
                   when(col("identity_resolution_data.care_provider").isNotNull(), 0.25).otherwise(0)))  # Healthcare identity resolution completeness
        .withColumn("calculated_confidence",
                   (col("source_confidence_score") * 0.6 + col("completeness_score") * 0.4))
        
        # Handle temporal conflicts - keep most recent with highest confidence
        .withColumn("row_num", row_number().over(temporal_window.orderByDesc("effective_from", "calculated_confidence")))
        .filter(col("row_num") == 1)
        
        # Implement entity resolution
        .withColumn("canonical_entity_id", 
                   when(col("context_data.medical_record_number").isNotNull(), 
                   concat(lit("MRN_"), col("context_data.medical_record_number")))
                   .otherwise(col("entity_id")))  # Healthcare entity resolution using MRN
        
        # Update final confidence score
        .withColumn("final_confidence_score", 
                   when(col("canonical_entity_id") == col("entity_id"), col("calculated_confidence"))
                   .otherwise(col("calculated_confidence") * 0.8))  # Penalize resolved entities
        
        .select(
            col("context_id"),
            col("canonical_entity_id").alias("entity_id"),
            col("entity_type"),
            col("context_type"),
            col("context_data"),
            col("final_confidence_score").alias("confidence_score"),
            col("effective_from"),
            col("effective_to"),
            col("source_system"),
            current_timestamp().alias("resolved_at")
        )
    )

# Task Identity-3 - Identity Resolution Graph Enrichment with Relationship Traversal
@dlt.table(
    name="silver_enriched_identity_resolution",
    comment="Graph-enriched identity resolution with derived attributes and relationship-based features"
)
@dlt.expect_all({
    "enrichment_completeness": "enriched_attributes IS NOT NULL",
    "relationship_integrity": "related_identity_resolutions IS NOT NULL OR relationship_count = 0"
})
def identity_resolution_enrichment():
    """Pattern: Graph traversal for identity resolution enrichment with configurable depth"""
    resolved_identity_resolution = dlt.read("silver_resolved_identity_resolution")
    identity_resolution_relationships = dlt.read("bronze_identity_resolution_relationships")
    
    # First-degree relationship enrichment
    enriched_l1 = (
        resolved_identity_resolution.alias("ctx")
        .join(
            identity_resolution_relationships.alias("rel"),
            col("ctx.identity_resolution_id") == col("rel.source_identity_resolution_id"),
            "left"
        )
        .join(
            resolved_identity_resolution.alias("related"),
            col("rel.target_identity_resolution_id") == col("related.identity_resolution_id"),
            "left"
        )
        .groupBy("ctx.identity_resolution_id", "ctx.entity_id", "ctx.entity_type", "ctx.identity_resolution_type", 
                "ctx.identity_resolution_data", "ctx.confidence_score", "ctx.effective_from", "ctx.effective_to")
        .agg(
            collect_list("related.identity_resolution_id").alias("related_identity_resolutions"),
            count("related.identity_resolution_id").alias("relationship_count"),
            avg("related.confidence_score").alias("avg_related_confidence"),
            collect_set("related.identity_resolution_type").alias("related_identity_resolution_types")
        )
    )
    
    return (
        enriched_l1
        # Calculate derived identity resolution attributes
        .withColumn("identity_resolution_centrality_score",
                   when(col("relationship_count") > 0, col("relationship_count") * col("avg_related_confidence"))
                   .otherwise(0.0))
        
        # Temporal identity resolution features
        .withColumn("identity_resolution_age_hours",
                   (unix_timestamp(current_timestamp()) - unix_timestamp(col("effective_from"))) / 3600)
        
        # Create enriched attributes map
        .withColumn("enriched_attributes",
                   create_map(
                       lit("relationship_count"), col("relationship_count").cast("string"),
                       lit("centrality_score"), col("identity_resolution_centrality_score").cast("string"),
                       lit("age_hours"), col("identity_resolution_age_hours").cast("string"),
                       **<TODO: your_additional_derived_attributes>**
                   ))
        
        .withColumn("enrichment_timestamp", current_timestamp())
        .select(
            col("identity_resolution_id"),
            col("entity_id"),
            col("entity_type"),
            col("identity_resolution_type"),
            col("identity_resolution_data"),
            col("enriched_attributes"),
            col("confidence_score"),
            col("relationship_count"),
            col("related_identity_resolutions"),
            col("effective_from"),
            col("effective_to"),
            col("enrichment_timestamp")
        )
    )

# Task Identity-4 - Identity Resolution Analytics and Business Metrics
@dlt.table(
    name="gold_identity_resolution_metrics",
    comment="Identity resolution quality and attribution analytics for business intelligence",
    table_properties={
        "quality": "gold",
        "delta.enableChangeDataFeed": "true"
    }
)
def identity_resolution_analytics():
    """Pattern: Identity resolution-driven business metrics with quality tracking"""
    enriched_identity_resolution = dlt.read("silver_enriched_identity_resolution")
    
    return (
        enriched_context
        .withColumn("date_partition", to_date(col("effective_from")))
        .withColumn("hour_partition", hour(col("effective_from")))
        
        # Identity resolution quality dimensions
        .withColumn("is_high_quality", 
                   col("confidence_score") >= 0.90)  # Healthcare quality threshold
        .withColumn("is_well_connected",
                   col("relationship_count") >= 2)  # Healthcare connectivity threshold (care coordination)
        .withColumn("is_fresh",
                   col("identity_resolution_age_hours") <= 4)  # Healthcare freshness threshold (4 hours for clinical identity resolution)
        
        .groupBy("entity_type", "identity_resolution_type", "date_partition", "hour_partition")
        .agg(
            count("*").alias("total_identity_resolutions"),
            countDistinct("entity_id").alias("unique_entities"),
            avg("confidence_score").alias("avg_confidence"),
            sum(when(col("is_high_quality"), 1).otherwise(0)).alias("high_quality_identity_resolutions"),
            sum(when(col("is_well_connected"), 1).otherwise(0)).alias("connected_identity_resolutions"),
            sum(when(col("is_fresh"), 1).otherwise(0)).alias("fresh_identity_resolutions"),
            avg("relationship_count").alias("avg_relationships"),
            max("confidence_score").alias("max_confidence"),
            min("confidence_score").alias("min_confidence"),
            avg(when(col("identity_resolution_data.clinical_outcome_score").isNotNull(), 
                col("identity_resolution_data.clinical_outcome_score").cast("double")).otherwise(0)).alias("avg_clinical_outcomes"),
            sum(when(col("identity_resolution_data.emergency_visit") == "true", 1).otherwise(0)).alias("emergency_identity_resolutions"),
            countDistinct(when(col("identity_resolution_data.care_provider").isNotNull(), 
                col("identity_resolution_data.care_provider"))).alias("unique_providers"),
            sum(when(col("identity_resolution_data.readmission_risk").cast("double") > 0.5, 1).otherwise(0)).alias("high_risk_patients")  # Healthcare business metrics
        )
        
        # Calculate quality scores
        .withColumn("quality_score",
                   (col("high_quality_identity_resolutions").cast("double") / col("total_identity_resolutions")) * 100)
        .withColumn("connectivity_score",
                   (col("connected_identity_resolutions").cast("double") / col("total_identity_resolutions")) * 100)
        .withColumn("freshness_score",
                   (col("fresh_identity_resolutions").cast("double") / col("total_identity_resolutions")) * 100)
    )

# Task Identity-5 - Identity Resolution Quality Monitoring
@dlt.table(
    name="gold_identity_resolution_quality_monitoring",
    comment="Real-time identity resolution quality monitoring and SLA tracking"
)
def identity_resolution_quality_monitoring():
    """Pattern: Real-time quality monitoring with alerting thresholds"""
    enriched_identity_resolution = dlt.read("silver_enriched_identity_resolution")
    
    # **<TODO: Implement your specific quality monitoring logic>**
    # **<TODO: Add SLA violation detection and alerting>**
    # **<TODO: Calculate identity resolution drift metrics>**
    
    return (
        enriched_identity_resolution
        .withColumn("quality_check_timestamp", current_timestamp())
        .withColumn("sla_violations",
                   array(
                       when(col("confidence_score") < 0.85, lit("low_confidence")).otherwise(lit(None)),  # Healthcare confidence SLA
                       when(col("identity_resolution_age_hours") > 4, lit("stale_identity_resolution")).otherwise(lit(None)),  # Healthcare freshness SLA (4 hours)
                       when(col("identity_resolution_data.care_gap_days").cast("int") > 30, lit("care_gap_violation")).otherwise(lit(None)),
                       when(col("identity_resolution_data.medication_adherence").cast("double") < 0.8, lit("medication_adherence_violation")).otherwise(lit(None)),
                       when(col("relationship_count") == 0 and col("identity_resolution_type") == "care_coordination", lit("isolated_patient")).otherwise(lit(None))  # Additional healthcare SLA checks
                   ).filter(lambda x: x.isNotNull()))
        .filter(size(col("sla_violations")) > 0)  # Only keep violations for alerting
    )

# **<TODO: Add additional identity resolution functions for your specific domain>**
```

### Integration Points
```yaml
DATABRICKS_ASSET_BUNDLE:
  - file: databricks.yml
  - add_resource: 
      name: "patient_data_medallion_pipeline"
      type: "pipelines"
      configuration: "resources/pipelines.yml"
  
PIPELINE_CONFIGURATION:
  - file: resources/pipelines.yml
  - pattern: |
      patient_data_medallion_pipeline:
        name: "${var.pipeline_name}"
        target: "${var.environment}"
        libraries:
          - file:
              path: "./src/pipelines"
        clusters:
          - label: "default"
            node_type_id: "${var.node_type}"
            num_workers: "${var.num_workers}"
            spark_conf:
              # "spark.databricks.delta.properties.defaults.encryption.enabled": "true" # TODO: Claud.md review
              "spark.databricks.delta.properties.defaults.changeDataFeed.enabled": "true"
            custom_tags:
              "compliance": "HIPAA"
              "data_classification": "PHI"
```

## Validation Loop

### Level 1: Syntax & Configuration Validation
```bash
# Run these FIRST - fix any errors before proceeding
databricks bundle validate --environment dev    # Validate Asset Bundle config
python -m py_compile src/pipelines/**/*.py      # Check Python syntax

# Healthcare-specific validations
python -m pytest tests/test_hipaa_compliance.py -v  # HIPAA compliance validation
python -c "import yaml; yaml.safe_load(open('resources/pipelines.yml'))"  # YAML syntax check

# Expected: No errors. If errors, READ the error and fix.
# CRITICAL: All HIPAA compliance tests must pass before deployment
```

### Level 2: Unit Tests for Pipeline Logic
```python
# CREATE test_patient_pipeline.py with these test cases:
import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch
from pyspark.sql.functions import col

def test_bronze_patient_ehr_schema():
    """Verify bronze patient EHR table schema is correct for healthcare data"""
    # Mock dlt.read() and test schema validation
    with patch('dlt.read') as mock_read:
        mock_read.return_value = create_mock_patient_dataframe()
        result = bronze_patient_ehr()
        assert "patient_id" in result.columns
        assert "mrn" in result.columns
        assert "ssn_hash" in result.columns
        assert "ssn" not in result.columns  # Critical: raw SSN must be removed
        assert "_ingested_at" in result.columns

def test_silver_patient_transformation():
    """Verify silver layer patient transformations for clinical data standardization"""
    # Test HIPAA de-identification and clinical validation
    with patch('dlt.read') as mock_read:
        mock_read.return_value = create_test_patient_bronze_data()
        result = silver_patient_master()
        
        # Verify no null patient IDs
        assert result.filter(col("patient_id").isNull()).count() == 0
        
        # Verify HIPAA age de-identification (89+ becomes 90)
        elderly_patients = result.filter(col("age_years") == 90)
        assert elderly_patients.count() > 0  # Should have de-identified elderly patients
        
        # Verify gender standardization
        valid_genders = result.filter(col("gender").isin(["M", "F", "U"]))
        assert valid_genders.count() == result.count()

def test_patient_data_quality_expectations():
    """Verify DLT expectations work correctly for patient data quality rules"""
    # Test that malformed patient data is properly quarantined
    test_data = create_malformed_patient_data()
    
    # Test age validation (0-150 years)
    invalid_age_data = test_data.filter(col("age_years") < 0 | col("age_years") > 150)
    assert invalid_age_data.count() > 0  # Should detect invalid ages
    
    # Test test patient detection
    test_patients = test_data.filter(upper(col("last_name")).contains("TEST"))
    assert test_patients.count() > 0  # Should detect test patients for quarantine

def test_hipaa_compliance():
    """Verify HIPAA compliance across patient data transformations"""
    test_data = create_patient_test_data()
    result = silver_patient_master()
    
    # Verify no raw SSN in result
    assert "ssn" not in result.columns
    assert "ssn_hash" in result.columns
    
    # Verify elderly patient de-identification
    elderly = result.filter(col("age_years") >= 89)
    if elderly.count() > 0:
        assert elderly.select("age_years").distinct().collect()[0][0] == 90
    
    # Verify change data feed is enabled for audit
    table_properties = result.schema.metadata
    # Note: In actual implementation, check Delta table properties

# Identity Resolution Test Cases

def test_identity_resolution_logic():
    """Verify identity resolution handles conflicts correctly"""
    # Test multiple identity resolutions for same entity with different confidence scores
    test_identity_resolutions = create_conflicting_identity_resolution_data()
    result = identity_resolution_function(test_identity_resolutions)
    
    # Verify highest confidence identity resolution wins
    assert result.filter(col("confidence_score") < 0.8).count() == 0
    # Verify no duplicate canonical entity IDs
    assert result.groupBy("canonical_entity_id").count().filter(col("count") > 1).count() == 0
    # **<TODO: Add your conflict resolution validation logic>**

def test_temporal_identity_resolution_validity():
    """Verify temporal identity resolution windows are handled correctly"""
    # Test overlapping effective periods
    test_data = create_temporal_identity_resolution_data()
    result = temporal_identity_resolution_function(test_data)
    
    # Verify temporal consistency
    invalid_temporal = result.filter(
        (col("effective_to").isNotNull()) & 
        (col("effective_from") > col("effective_to"))
    )
    assert invalid_temporal.count() == 0
    
    # Test point-in-time identity resolution retrieval
    point_in_time = datetime(2024, 1, 15, 10, 0, 0)
    active_identity_resolutions = result.filter(
        (col("effective_from") <= lit(point_in_time)) &
        ((col("effective_to").isNull()) | (col("effective_to") > lit(point_in_time)))
    )
    # **<TODO: Add your temporal logic validation>**

def test_identity_resolution_graph_traversal():
    """Verify identity resolution relationship traversal works correctly"""
    # Test multi-hop identity resolution enrichment
    test_identity_resolutions = create_graph_identity_resolution_data()
    test_relationships = create_identity_resolution_relationships_data()
    
    result = identity_resolution_graph_enrichment(test_identity_resolutions, test_relationships)
    
    # Verify relationship counts are correct
    assert result.filter(col("relationship_count") < 0).count() == 0
    # Verify centrality scores are calculated
    assert result.filter(col("identity_resolution_centrality_score").isNull()).count() == 0
    # **<TODO: Add graph traversal validation for your specific algorithms>**

def test_identity_resolution_quality_metrics():
    """Verify identity resolution quality calculations are accurate"""
    test_identity_resolutions = create_quality_test_data()
    result = identity_resolution_quality_function(test_identity_resolutions)
    
    # Test completeness score calculation
    complete_identity_resolutions = result.filter(col("completeness_score") == 1.0)
    incomplete_identity_resolutions = result.filter(col("completeness_score") < 1.0)
    # Verify completeness logic
    
    # Test freshness score calculation
    fresh_identity_resolutions = result.filter(col("freshness_score") >= 0.8)
    stale_identity_resolutions = result.filter(col("freshness_score") < 0.5)
    # **<TODO: Add your quality metric validations>**

def test_identity_resolution_sla_monitoring():
    """Verify SLA violation detection works correctly"""
    test_identity_resolutions = create_sla_violation_data()
    result = identity_resolution_sla_monitoring(test_identity_resolutions)
    
    # Verify SLA violations are detected
    confidence_violations = result.filter(array_contains(col("sla_violations"), "low_confidence"))
    freshness_violations = result.filter(array_contains(col("sla_violations"), "stale_identity_resolution"))
    
    assert confidence_violations.count() > 0  # Should detect low confidence
    assert freshness_violations.count() > 0   # Should detect stale identity resolution
    # **<TODO: Add your SLA monitoring validations>**

def test_identity_resolution_entity_resolution():
    """Verify entity resolution algorithms work correctly"""
    test_entities = create_entity_resolution_test_data()
    result = entity_resolution_function(test_entities)
    
    # Test fuzzy matching accuracy
    correctly_resolved = result.filter(col("resolution_confidence") >= 0.9)
    # Test that duplicate entities are merged
    unique_entities = result.select("canonical_entity_id").distinct().count()
    # **<TODO: Add your entity resolution accuracy tests>**

# **<TODO: Add additional test cases for your business logic>**
```

```bash
# Run and iterate until passing:
python -m pytest tests/test_patient_pipeline.py -v
python -m pytest tests/test_hipaa_compliance.py -v
# If failing: Read error, understand root cause, fix code, re-run
# CRITICAL: All HIPAA compliance tests must pass before deployment
```

### Level 3: Integration Test with Asset Bundle
```bash
# Deploy to dev environment with HIPAA compliance
databricks bundle deploy --environment dev

# Trigger patient data pipeline run
databricks jobs run-now --job-id patient_data_medallion_pipeline

# Monitor pipeline execution
databricks jobs get-run <run_id>

# Verify patient data quality and compliance
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM bronze_patient_ehr WHERE patient_id IS NOT NULL",
  "warehouse_id": "<your_warehouse_id>"
}'

databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM silver_patient_master WHERE ssn_hash IS NOT NULL AND ssn IS NULL",
  "warehouse_id": "<your_warehouse_id>"
}'

databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT AVG(data_quality_score) FROM silver_patient_master",
  "warehouse_id": "<your_warehouse_id>"
}'

# Verify HIPAA compliance - no elderly patients with detailed age
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM silver_patient_master WHERE age_years > 89 AND age_years != 90",
  "warehouse_id": "<your_warehouse_id>"
}'

# Expected: Pipeline completes successfully with expected patient counts and 99.5%+ data quality
# Expected: Zero elderly patients with detailed age (HIPAA compliance)
# If error: Check Databricks job logs and DLT event logs for clinical data validation issues
```

## Final validation Checklist
- [ ] Asset Bundle validates: `databricks bundle validate --target dev`
- [ ] All tests pass: `python -m pytest tests/ -v`
- [ ] HIPAA compliance tests pass: `python -m pytest tests/test_hipaa_compliance.py -v`
- [ ] Pipeline deploys successfully: `databricks bundle deploy --environment dev`
- [ ] Patient data pipeline run successful: Check Databricks UI for patient_data_medallion_pipeline
- [ ] Patient data quality expectations met: 99.5%+ data quality score achieved
- [ ] HIPAA de-identification working: No elderly patients with detailed age (>89 becomes 90)
- [ ] No raw SSN in silver/gold layers: Only ssn_hash present
- [ ] Change data feed enabled: Audit trail available for all patient tables
- [ ] Performance within acceptable limits: <5 minute end-to-end latency
- [ ] Observable pipeline metrics: Real-time monitoring and alerting functional
- [ ] Clinical data validation: HL7 FHIR compliance and clinical range checks working
- [ ] Documentation updated: README.md with healthcare compliance notes and pipeline comments

---

## Anti-Patterns to Avoid
- ❌ Don't use spark.read() directly in DLT tables - use dlt.read() for dependencies
- ❌ Don't skip data quality expectations - they're critical for pipeline reliability
- ❌ Don't hardcode paths or cluster configs - use Asset Bundle variables
- ❌ Don't use display() in DLT functions - return DataFrames
- ❌ Don't ignore DLT event logs when debugging - they contain crucial info
- ❌ Don't mix streaming and batch patterns without understanding implications
- ❌ Don't deploy directly to prod - always test in dev environment first

### Identity Resolution Anti-Patterns
- ❌ Don't ignore temporal context - always track effective periods and version changes
- ❌ Don't assume context is static - implement versioning and change tracking for evolving context  
- ❌ Don't skip conflict resolution - multiple sources will have conflicting context that must be resolved
- ❌ Don't ignore context quality - implement confidence scoring, completeness metrics, and validation
- ❌ Don't flatten context relationships - preserve graph structures and relationship metadata in silver layer
- ❌ Don't batch-process real-time context - use streaming for time-sensitive context with low latency requirements
- ❌ Don't ignore context lineage - track context source, transformation history, and data provenance
- ❌ Don't over-engineer entity resolution - start with simple matching before complex ML approaches
- ❌ Don't skip context freshness validation - stale context can be worse than no context
- ❌ Don't ignore context graph performance - index relationship tables and limit traversal depth
- ❌ Don't mix context types in single tables - separate behavioral, demographic, and transactional context
- ❌ Don't assume context completeness - handle missing context gracefully with default values
- ❌ Don't ignore context privacy - implement proper PII handling and access controls for sensitive context
- ❌ Don't skip context validation - validate context against business rules and referential integrity
- ❌ Don't ignore context drift - monitor context distribution changes that may indicate data quality issues
- ❌ Don't store raw SSN or other direct identifiers - always hash PII immediately upon ingestion
- ❌ Don't assume MRNs are unique across healthcare systems - implement proper patient entity resolution
- ❌ Don't ignore clinical data ranges - implement HL7 FHIR validation for lab values and vitals
- ❌ Don't skip change data feed enablement - HIPAA requires audit trails for all patient data modifications
- ❌ Don't hardcode clinical thresholds - use configurable parameters for age limits and clinical ranges
- ❌ Don't process patient data without encryption - ensure Delta tables have encryption enabled
- ❌ Don't ignore temporal clinical context - patient data must maintain point-in-time clinical accuracy
- ❌ Don't deploy patient pipelines without HIPAA compliance testing and validation

## Domain Model: Health Insurance Patient Analytics

### Architecture Overview
- **Medallion Architecture**: Bronze → Silver → Gold data transformation pipeline
- **Delta Live Tables (DLT)**: Declarative pipelines with data quality expectations  
- **Asset Bundle Deployment**: Infrastructure-as-code with serverless compute
- **Identity Resolution**: Multi-source identity resolution ingestion, resolution, and enrichment
- **HIPAA Compliance**: Healthcare data governance and audit trails

### Unity Catalog Structure
```
Catalog: {user}_{environment}  # e.g., juan_dev, juan_prod
└── Schema: ctx__eng
    ├── bronze_*     # Raw ingestion (patients, claims, medical_events)
    ├── silver_*     # Cleaned & validated data
    └── gold_*       # Analytics-ready dimensional model
```

### Data Sources
- **Health Insurance CSV Files**: Patient demographics, claims, medical events (Databricks Volumes)
- **Synthetic Data Generation Job**: **CRITICAL** - Automated Databricks job that generates realistic test data
  - **File Naming Convention**: `{entity_type}_{yymmdd}_{hhmm}.csv` (e.g., `patient_2508015_0543.csv`)
  - **Data Placement**: Standard CSV files in respective Databricks Volumes for pipeline ingestion
  - **Source System Simulation**: EHR_SYSTEM (patients), ADT_SYSTEM (medical_events), LAB_SYSTEM (claims)
  - **Referential Integrity**: Maintains patient_id relationships across all generated entities
- **Auto Loader Ingestion**: Schema evolution with hourly batch processing

#### Entity Relationship Model (MANDATORY - STRICTLY ENFORCE)

**🚨 CRITICAL: ALL synthetic data generation and medallion architecture implementation MUST strictly adhere to this exact three-entity model. No additional entities or deviations allowed.**

```
Health Insurance Domain: Patient Analytics (EXACTLY 3 ENTITIES)
├── Patients (dim_patients) - PRIMARY ENTITY
│   ├── patient_id (PK) - String, unique identifier
│   ├── demographics (age, sex, region) - Core patient attributes
│   │   ├── age - Integer, 18-85 range (normal distribution μ=45, σ=15)
│   │   ├── sex - String, MALE/FEMALE
│   │   └── region - String, NORTHEAST/NORTHWEST/SOUTHEAST/SOUTHWEST
│   ├── health_metrics (bmi, smoker, children) - Health and lifestyle data
│   │   ├── bmi - Double, 16-50 range (normal distribution μ=28, σ=6)
│   │   ├── smoker - Boolean, age-correlated smoking probability
│   │   └── children - Integer, number of dependents (Poisson λ=1.2)
│   ├── financial_data (charges) - Insurance cost information
│   │   └── charges - Double, calculated premium based on risk factors
│   ├── insurance_details (plan_type, coverage_start_date) - Insurance information
│   ├── temporal_data (timestamp) - Record creation timestamp
│   └── SCD Type 2 for historical tracking - Track changes over time
│
├── Claims (fact_claims) - TRANSACTIONAL ENTITY
│   ├── claim_id (PK) - String, unique claim identifier
│   ├── patient_id (FK) - String, references Patients.patient_id
│   ├── claim_amount, claim_date - Financial and temporal data
│   ├── diagnosis_code, procedure_code - Medical coding (ICD-10, CPT)
│   └── claim_status - Processing status (submitted, approved, denied, paid)
│
└── Medical_Events (fact_medical_events) - EVENT ENTITY
    ├── event_id (PK) - String, unique event identifier
    ├── patient_id (FK) - String, references Patients.patient_id
    ├── event_date, event_type - Temporal and categorical data
    ├── medical_provider - Healthcare provider information
    ├── clinical_results - Lab results, vital signs, diagnostic data
    └── event_details - Additional clinical context and documentation
```

#### Mandatory Entity Implementation Requirements

**🚨 SYNTHETIC DATA GENERATION REQUIREMENTS:**
1. **Generate CSV files**: patients_yymmdd_hhmm.csv, claims_yymmdd_hhmm.csv, medical_events_yymmdd_hhmm.csv
2. **Maintain referential integrity**: All claims.patient_id and medical_events.patient_id MUST reference valid patients.patient_id
3. **Realistic ratios**: Each patient should have 2-5 claims and 3-8 medical events on average
4. **No additional entities**: Do not create provider tables, diagnosis tables, or other entities
5. **Data Quality**: Ensure realistic healthcare data patterns with proper validation rules

**🚨 MEDALLION ARCHITECTURE REQUIREMENTS:**
1. **Bronze Layer**: Exactly 3 tables (bronze_patients, bronze_claims, bronze_medical_events)
2. **Silver Layer**: Exactly 3 tables (silver_patients, silver_claims, silver_medical_events) with data quality and standardization
3. **Gold Layer**: Dimensional model with dim_patients and 2 fact tables (fact_claims, fact_medical_events)
4. **Foreign Key Validation**: Silver and Gold layers MUST validate and maintain referential integrity
5. **No Schema Drift**: Additional columns or entities require explicit approval and domain model updates
6. **Entity Consistency**: All layers MUST use consistent entity names (Patients, Claims, Medical_Events)
7. **Data Source Alignment**: Bronze layer ingestion must align with EHR_SYSTEM, ADT_SYSTEM, and LAB_SYSTEM data sources

#### Synthetic Data Generation Job Requirements

**🚨 CRITICAL: Synthetic data generation is REQUIRED before pipeline implementation**

**Job Configuration Requirements:**
- **Databricks Job**: Automated synthetic data generation with configurable scheduling
- **File Naming Convention**: `{entity_type}_{yymmdd}_{hhmm}.csv` (e.g., `patient_25080115_0543.csv`)
- **Data Placement**: Standard CSV files in respective Databricks Volumes for pipeline ingestion
- **Source System Simulation**: 
  - **EHR_SYSTEM** → `patient_{yymmdd}_{hhmm}.csv` (patient demographics and insurance)
  - **ADT_SYSTEM** → `medical_events_{yymmdd}_{hhmm}.csv` (encounter and care coordination)
  - **LAB_SYSTEM** → `claims_{yymmdd}_{hhmm}.csv` (lab results and diagnostic claims)

**Data Generation Requirements:**
- **Realistic Healthcare Patterns**: Age distributions, regional demographics, clinical coding
- **Referential Integrity**: All claims.patient_id and medical_events.patient_id reference valid patients.patient_id
- **Data Volume**: Configurable patient counts with realistic claim and medical event ratios
- **Temporal Consistency**: Coordinated timestamps across all generated entities
- **Quality Validation**: Built-in data quality checks before CSV file generation

**Integration Requirements:**
- **Pipeline Ingestion**: Generated CSV files must be ingestible by bronze layer Auto Loader
- **Schema Compatibility**: Generated data must match bronze layer schema requirements
- **Volume Management**: Automatic cleanup of old CSV files to prevent storage bloat
- **Monitoring**: Job success/failure alerts and data generation metrics tracking

### Medallion Architecture Implementation

#### Bronze Layer (Raw Data Landing)
- **Purpose**: Immutable raw data ingestion with minimal transformation
- **Data Quality**: Schema enforcement, basic validation, rescued data handling
- **Storage**: Delta tables with partition by ingestion date
- **Governance**: PII detection, data classification, audit logging

#### Silver Layer (Cleaned & Enriched)
- **Purpose**: Cleaned, deduplicated, and business-rule validated data
- **Data Quality**: Comprehensive validation, outlier detection, referential integrity
- **Transformations**: Data type standardization, business logic application
- **Governance**: Data lineage tracking, quality metrics collection

#### Gold Layer (Analytics-Ready)
- **Purpose**: Aggregated, dimensional model for analytics and ML
- **Design Pattern**: Star schema with fact and dimension tables
- **Performance**: Pre-aggregated metrics, materialized views
- **Governance**: Change data capture enabled, access controls

### Project Structure
```
healthcare-ldp-medallion/
├── databricks.yml              # Asset Bundle configuration
├── resources/
│   ├── pipelines.yml          # DLT pipeline definitions  
│   ├── jobs.yml               # **CRITICAL**: Synthetic data generation and monitoring jobs
│   └── workflows.yml          # Data generation scheduling and orchestration
├── src/
│   ├── pipelines/
│   │   ├── bronze/            # Raw data ingestion (3 tables)
│   │   ├── silver/            # Data cleaning & validation (3 tables)
│   │   ├── gold/              # Dimensional model (3 tables)
│   │   └── shared/            # Common schemas and utilities
│   ├── jobs/
│   │   ├── data_generation/   # **CRITICAL**: Synthetic patient data generation job
│   │   │   ├── synthetic_patient_generator.py      # Generate realistic patient/claims/medical_events data
│   │   │   ├── csv_file_writer.py                  # Write timestamped CSV files to Volumes
│   │   │   └── data_quality_validator.py           # Validate referential integrity and data quality
│   │   └── monitoring/        # Context quality monitoring
│   └── tests/                 # Unit and integration tests
└── PRPs/                      # Planning and documentation
```

## Implementation Guidelines

### Schema Standards
- **Naming Convention**: `{layer}_{entity}` (e.g., bronze_patients, silver_claims, gold_patient_metrics)
- **Data Types**: String for IDs, Integer for counts, Double for amounts, Boolean for flags
- **Primary Keys**: patient_id, claim_id, event_id for each respective entity
- **Partitioning**: By ingestion date (bronze), by business date (silver/gold)

### Mandatory Schema Implementation (DOMAIN MODEL ENFORCEMENT)

**🚨 CRITICAL: Implementation MUST create EXACTLY these tables with EXACTLY these schemas - no additions, no omissions.**

#### Bronze Layer Tables (RAW DATA - EXACTLY 3 TABLES)

**1. bronze_patients** (Raw patient demographic and insurance data)
- **Purpose**: Raw patient data ingestion from EHR system
- **Key Fields**: patient_id, mrn, first_name, last_name, date_of_birth, gender, address, ssn, insurance_id
- **Schema**: See BRONZE_PATIENT_SCHEMA in Core Business Entity Schemas section above

**2. bronze_claims** (Raw insurance claims data)
- **Purpose**: Raw claims data ingestion from claims system
- **Key Fields**: claim_id, patient_id, claim_amount, claim_date, diagnosis_code, procedure_code, claim_status
- **Schema**: See BRONZE_CLAIMS_SCHEMA in Core Business Entity Schemas section above

**3. bronze_medical_events** (Raw medical history/events data)
- **Purpose**: Raw medical events ingestion from ADT/Lab systems  
- **Key Fields**: event_id, patient_id, event_date, event_type, medical_provider
- **Schema**: See BRONZE_MEDICAL_EVENTS_SCHEMA in Core Business Entity Schemas section above

#### Silver Layer Requirements (DATA QUALITY - EXACTLY 3 TABLES)
- **silver_patients**: Cleaned patient data with HIPAA compliance and data quality validation
- **silver_claims**: Validated claims with referential integrity checks to silver_patients
- **silver_medical_events**: Cleaned medical events with referential integrity checks to silver_patients

#### Gold Layer Requirements (DIMENSIONAL MODEL - EXACTLY 3 TABLES)
- **dim_patients**: SCD Type 2 patient dimension with complete patient 360 view
- **fact_claims**: Claims fact table with pre-aggregated metrics and foreign key to dim_patients
- **fact_medical_events**: Medical events fact table with foreign key to dim_patients

### Key Implementation Requirements
1. **Exactly 3 entities**: Patients, Claims, Medical_Events (no additional entities)
2. **Referential integrity**: All claims/medical_events must reference valid patient_id
3. **HIPAA compliance**: PII hashing, audit trails, data quality quarantine 
4. **Asset Bundle deployment**: Infrastructure-as-code with serverless compute
5. **Identity resolution**: Multi-source ingestion, resolution, enrichment patterns
6. **Data source alignment**: EHR_SYSTEM, ADT_SYSTEM, and LAB_SYSTEM integration

### Data Quality Patterns
```python
# Health insurance data validation example
@dlt.expect_all_or_drop({
    "valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "valid_age": "age IS NOT NULL AND age BETWEEN 18 AND 85", 
    "valid_sex": "sex IS NOT NULL AND sex IN ('MALE', 'FEMALE')",
    "valid_bmi": "bmi IS NOT NULL AND bmi BETWEEN 16 AND 50",
    "valid_charges": "charges IS NOT NULL AND charges > 0"
})
def silver_patients():
    return dlt.read("bronze_patients")
```

## Development Phases

1. **Synthetic Data Generation Job**: **FIRST PRIORITY** - Implement automated CSV generation
   - Create Databricks job for generating realistic patient/claims/medical_events data
   - Implement file naming convention: `{entity_type}_{yymmdd}_{hhmm}.csv`
   - Place files in appropriate Databricks Volumes for pipeline ingestion
   - Ensure referential integrity and realistic healthcare data patterns
2. **Asset Bundle Setup**: Configure `databricks.yml` with dev/prod environments
3. **Bronze Layer**: Auto Loader ingestion with schema enforcement
4. **Silver Layer**: Data cleaning, validation, and context resolution
5. **Gold Layer**: Dimensional modeling with fact/dimension tables
6. **Identity Resolution**: Multi-source identity resolution processing and quality monitoring

## References

- **Primary documentation**: See `CLAUDE.md` for complete Databricks patterns and Asset Bundle configurations
- **Healthcare compliance**: HL7 FHIR standards, HIPAA requirements
- **Identity resolution**: MCP servers for additional examples and patterns

---

*Modern health insurance data platform using Databricks medallion architecture with identity resolution and HIPAA compliance.*
