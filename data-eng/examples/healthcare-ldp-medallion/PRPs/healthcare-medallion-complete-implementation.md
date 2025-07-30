# Healthcare Data Medallion Architecture - Complete Implementation PRP

## Goal
**Implement a complete healthcare insurance patient data medallion architecture using Databricks Asset Bundles, Delta Live Tables, and serverless compute with HIPAA compliance, featuring synthetic data generation, bronze-silver-gold layers, and comprehensive data quality controls.**

## Why
- **Business Value**: Establish enterprise-grade healthcare data engineering foundation with complete data governance, quality controls, and HIPAA compliance for patient analytics
- **Greenfield Implementation**: No existing infrastructure dependencies - starting fresh with modern Databricks platform capabilities
- **Data Quality & Compliance**: Critical healthcare data requires 99.5%+ data quality scores with full audit trails and HIPAA compliance
- **Performance Requirements**: Sub-5 minute end-to-end pipeline latency for clinical decision support

## What
**A production-ready healthcare data medallion pipeline with synthetic data generation, three-layer architecture (Bronze→Silver→Gold), Asset Bundle deployment, comprehensive data quality validation, HIPAA compliance controls, and real-time monitoring capabilities.**

### Success Criteria
- [ ] **Complete Asset Bundle deployment structure** with dev/staging/prod environments and serverless compute
- [ ] **Synthetic healthcare data generation** producing realistic patient demographics, claims, and medical events
- [ ] **Bronze layer ingestion** with Auto Loader, schema enforcement, and file metadata tracking
- [ ] **Silver layer transformations** with HIPAA compliance, data quality validation, and referential integrity  
- [ ] **Gold layer analytics** with dimensional modeling, pre-aggregated metrics, and SCD Type 2 support
- [ ] **99.5%+ data quality score** achieved through comprehensive DLT expectations and monitoring
- [ ] **Sub-5 minute pipeline latency** with real-time monitoring and alerting
- [ ] **Full HIPAA compliance** with PII handling, audit trails, and encryption controls

## All Needed Context

### Documentation & References
```yaml
# Primary Development Reference - MANDATORY
- file: CLAUDE.md
  why: "Complete Databricks development patterns, Asset Bundle configurations, DLT pipeline examples, and consolidated external documentation references"
  extract: "All sections - Asset Bundle Management, DLT Pipeline Patterns, File Metadata Patterns, Data Quality Expectations, Development Guidelines, and Consolidated Documentation References"

# Project Requirements and Schema Definitions - MANDATORY  
- file: INITIAL.md
  why: "Complete healthcare domain model with EXACTLY 3 entities (Patients, Claims, Medical_Events), mandatory schema requirements, entity relationship definitions, and HIPAA compliance requirements"
  extract: "Entity Relationship Model section (lines 59-108), Mandatory Schema Implementation (lines 272-351), and Healthcare Data Quality Patterns (lines 364-402)"

# Base PRP Template for Validation Patterns - MANDATORY
- file: PRPs/prp_base.md
  why: "Comprehensive validation loops, context engineering patterns, and healthcare-specific test cases for HIPAA compliance"
  extract: "Validation Loop section (lines 803-1024), Implementation Blueprint (lines 274-368), and Anti-Patterns (lines 1042-1074)"

# Existing Synthetic Data Generation Pattern - CRITICAL
- file: examples/data-gen.py
  why: "Working synthetic healthcare data generator with proper statistical distributions, HIPAA-compliant field generation, and CSV output pattern"
  extract: "Complete file - synthetic patient generation logic, statistical distributions for age/BMI/children, and healthcare cost calculation formula"

# Sample Data Schema Reference
- file: examples/insurance.csv
  why: "Actual CSV schema structure and data quality patterns for validation"
  extract: "Header row and sample records to understand expected data format and quality issues"

# Healthcare Domain Documentation (External)
- url: https://www.hl7.org/fhir/patient.html
  why: "HL7 FHIR Patient resource specifications for healthcare data standards, clinical validation rules, and interoperability requirements"

- url: https://www.hipaajournal.com/hipaa-compliance-checklist/ 
  why: "HIPAA compliance requirements for patient data handling, encryption standards, audit logging, and data governance controls"

- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-python-ref.html
  why: "DLT decorators reference, streaming tables, expectation patterns, and pipeline configuration"

- url: https://docs.databricks.com/dev-tools/bundles/index.html
  why: "Asset Bundle configuration, deployment patterns, environment management, and serverless compute setup"
```

### Current Codebase Structure
```bash
# Existing greenfield structure - no implementation yet
healthcare-ldp-medallion/
├── CLAUDE.md                          # Complete development patterns and external docs
├── INITIAL.md                         # Healthcare domain model and requirements  
├── PRPs/
│   ├── prp_base.md                    # Template with validation patterns
│   └── templates/                     # Empty templates directory
├── examples/
│   ├── data-gen.py                    # Working synthetic data generator
│   ├── insurance.csv                  # Sample healthcare data
│   └── cx_table.png                   # Data model visualization
└── README.md                          # Basic project overview
```

### Target Implementation Structure
```bash
# Complete healthcare medallion implementation to create
healthcare-ldp-medallion/
├── databricks.yml                     # Root Asset Bundle configuration
├── resources/
│   ├── pipelines.yml                  # DLT pipeline resource definitions
│   └── jobs.yml                       # Data generation and monitoring jobs
├── src/
│   ├── pipelines/
│   │   ├── shared/
│   │   │   └── healthcare_schemas.py   # Complete healthcare domain schemas
│   │   ├── bronze/
│   │   │   ├── patient_demographics_ingestion.py    # Patient CSV ingestion
│   │   │   ├── insurance_claims_ingestion.py        # Claims CSV ingestion 
│   │   │   └── medical_events_ingestion.py          # Medical events ingestion
│   │   ├── silver/
│   │   │   ├── patient_demographics_transform.py    # HIPAA-compliant transformations
│   │   │   ├── insurance_claims_transform.py        # Claims validation
│   │   │   └── medical_events_transform.py          # Medical events cleaning
│   │   └── gold/
│   │       ├── patient_360_dimension.py             # SCD Type 2 patient dimension
│   │       ├── claims_fact_table.py                 # Claims analytics fact table
│   │       └── medical_events_fact_table.py         # Medical events fact table
│   ├── jobs/
│   │   ├── synthetic_data_generation.py             # Enhanced data generation job
│   │   └── data_quality_monitoring.py               # Pipeline monitoring and alerts
│   └── tests/
│       ├── test_healthcare_schemas.py               # Schema validation tests
│       ├── test_hipaa_compliance.py                 # HIPAA compliance validation
│       └── test_pipeline_integration.py             # End-to-end pipeline tests
├── conf/
│   └── deployment.yml                 # Environment-specific configurations
└── docs/
    └── pipeline_architecture.md       # Implementation documentation
```

### Known Gotchas & Critical Patterns

#### Databricks Asset Bundle Critical Requirements
```yaml
# CRITICAL: Serverless-only deployment (NO cluster configurations allowed)
- NEVER use: job_clusters, new_cluster, existing_cluster_id, node_type_id, num_workers
- ALWAYS use: serverless: true for DLT pipelines
- REQUIRED: catalog and target separation for Unity Catalog: target: "${var.schema}" AND catalog: "${var.catalog}"

# CRITICAL: DLT Autoloader Pattern
- MANDATORY: .format("cloudFiles") when using cloudFiles options in DLT streaming tables
- REQUIRED: Use @dlt.table (not @dlt.view) for raw ingestion to ensure persistence and checkpointing

# CRITICAL: File paths in Asset Bundle libraries
- MUST reference individual files: path: ../src/pipelines/bronze/file.py  
- NEVER reference directories: path: ../src/pipelines/bronze/ (causes deployment errors)
```

#### Healthcare Domain Specific Requirements  
```yaml
# MANDATORY: Exactly 3 entities implementation
- patients.csv → bronze_patients → silver_patients → dim_patients
- claims.csv → bronze_claims → silver_claims → fact_claims  
- medical_events.csv → bronze_medical_events → silver_medical_events → fact_medical_events

# CRITICAL: HIPAA Compliance Controls
- Hash SSN immediately: sha2(concat(col("ssn"), lit("PATIENT_SALT")), 256)
- Age de-identification: when(col("age") >= 89, 90).otherwise(col("age"))
- Enable change data feed: "delta.enableChangeDataFeed": "true" for audit trails
- Use @dlt.quarantine instead of @dlt.expect_all_or_drop for audit trail preservation

# CRITICAL: Referential Integrity Enforcement
- All claims.patient_id MUST reference valid patients.patient_id
- All medical_events.patient_id MUST reference valid patients.patient_id
- Implement FK validation in silver layer with comprehensive quarantine handling
```

## Implementation Blueprint

### Task 1: Asset Bundle Foundation Setup
```yaml
MODIFY: databricks.yml (CREATE new file)
  - CONFIGURE bundle name: "healthcare-patient-data-pipeline"
  - SET variables: catalog, schema, environment, volumes_path, max_files_per_trigger
  - DEFINE targets: dev (juan_dev catalog) and prod (juan_prod catalog) with serverless compute
  - INCLUDE resources pattern: "include: - resources/*.yml"
  - CRITICAL: Use serverless-only configuration - NO cluster specifications

CREATE: resources/pipelines.yml
  - DEFINE patient_data_pipeline resource with serverless: true
  - SET target: "${var.schema}" and catalog: "${var.catalog}" (separate for Unity Catalog)
  - LIST individual library files (NOT directories): shared/healthcare_schemas.py, bronze/*, silver/*, gold/*
  - CONFIGURE: HIPAA compliance settings, change data feed, and healthcare metadata

CREATE: resources/jobs.yml  
  - DEFINE synthetic_data_generation_job with serverless compute (no cluster configs)
  - DEFINE data_quality_monitoring_job for pipeline health checks
  - SET environment-specific parameters using Asset Bundle variables
```

### Task 2: Healthcare Domain Schema Definitions
```yaml
CREATE: src/pipelines/shared/healthcare_schemas.py
  - IMPLEMENT complete PATIENT_SCHEMA with all mandatory fields from INITIAL.md lines 275-305
  - IMPLEMENT CLAIMS_SCHEMA with claim_id, patient_id FK, financial, and temporal fields  
  - IMPLEMENT MEDICAL_EVENTS_SCHEMA with event_id, patient_id FK, clinical, and provider fields
  - ADD file metadata fields: _ingested_at, _pipeline_env, _file_name, _file_path, _file_size
  - INCLUDE HIPAA compliance metadata and audit fields for all schemas
```

### Task 3: Enhanced Synthetic Data Generation
```yaml
MODIFY: examples/data-gen.py → CREATE: src/jobs/synthetic_data_generation.py
  - EXTEND existing generator to produce EXACTLY 3 CSV files: patients.csv, claims.csv, medical_events.csv
  - MAINTAIN referential integrity: all claims/events reference valid patient_ids
  - IMPLEMENT realistic ratios: each patient has 2-5 claims and 3-8 medical events average
  - ADD clinical validation: ICD-10 diagnosis codes, CPT procedure codes, clinical date ranges
  - OUTPUT to configurable Volumes path: /Volumes/${catalog}/${schema}/raw_data/
  - INCLUDE timestamped file generation for Auto Loader schema evolution testing
```

### Task 4: Bronze Layer Implementation - Raw Data Ingestion
```yaml
CREATE: src/pipelines/bronze/patient_demographics_ingestion.py
  - IMPLEMENT @dlt.table(name="bronze_patients") with PATIENT_SCHEMA enforcement
  - USE .format("cloudFiles") pattern for Auto Loader: spark.readStream.format("cloudFiles")
  - ADD schema location: cloudFiles.schemaLocation for patient data evolution
  - INCLUDE file metadata extraction using _metadata column pattern
  - IMPLEMENT basic data quality: @dlt.expect_all_or_drop for valid_patient_id, non_null_demographics
  - ADD HIPAA audit fields: _ingested_at, _pipeline_env, _file_name, _file_path

CREATE: src/pipelines/bronze/insurance_claims_ingestion.py  
  - IMPLEMENT @dlt.table(name="bronze_claims") with CLAIMS_SCHEMA enforcement
  - USE Auto Loader pattern with claims-specific file pattern: claims*.csv
  - ADD claim_id and patient_id FK validation expectations
  - INCLUDE financial data validation: positive claim amounts, valid claim dates
  - IMPLEMENT metadata tracking for claim processing audit trails

CREATE: src/pipelines/bronze/medical_events_ingestion.py
  - IMPLEMENT @dlt.table(name="bronze_medical_events") with MEDICAL_EVENTS_SCHEMA
  - USE Auto Loader for medical events: events*.csv or medical*.csv pattern  
  - ADD event_id and patient_id FK validation
  - INCLUDE clinical validation: valid event types, reasonable medical provider formats
  - IMPLEMENT comprehensive metadata for medical record audit requirements
```

### Task 5: Silver Layer Implementation - Data Quality & HIPAA Compliance
```yaml  
CREATE: src/pipelines/silver/patient_demographics_transform.py
  - IMPLEMENT @dlt.table(name="silver_patients") with comprehensive data quality
  - ADD HIPAA de-identification: age 89+ becomes 90, ZIP code last 2 digits for elderly
  - IMPLEMENT SSN hashing: sha2(concat(col("ssn"), lit("PATIENT_SALT")), 256)
  - USE @dlt.quarantine for test patients and data quality issues (maintain audit trail)
  - ADD clinical data standardization: gender M/F/U, region normalization, BMI validation
  - INCLUDE data quality scoring: completeness, validity, consistency metrics
  - ENABLE change data feed for HIPAA audit requirements

CREATE: src/pipelines/silver/insurance_claims_transform.py
  - IMPLEMENT @dlt.table(name="silver_claims") with referential integrity validation
  - ADD FK constraint validation: all patient_ids exist in silver_patients
  - IMPLEMENT claim status validation: submitted/approved/denied/paid only
  - ADD financial validation: positive amounts, reasonable ranges, claim date validation
  - INCLUDE ICD-10 and CPT code format validation using clinical standards
  - USE @dlt.quarantine for orphaned claims (invalid patient references)

CREATE: src/pipelines/silver/medical_events_transform.py
  - IMPLEMENT @dlt.table(name="silver_medical_events") with clinical validation
  - ADD FK validation: all patient_ids reference silver_patients table
  - IMPLEMENT event type validation: office_visit, procedure, lab_test, imaging, etc.
  - ADD temporal validation: event dates within reasonable clinical windows
  - INCLUDE medical provider validation and standardization
  - USE @dlt.quarantine for clinical data inconsistencies
```

### Task 6: Gold Layer Implementation - Dimensional Analytics
```yaml
CREATE: src/pipelines/gold/patient_360_dimension.py
  - IMPLEMENT @dlt.table(name="dim_patients") as SCD Type 2 dimension
  - ADD surrogate keys, effective dates, current record flags
  - INCLUDE comprehensive patient attributes: demographics, risk scores, insurance status
  - IMPLEMENT patient risk categorization: age-based, health metrics, social determinants
  - ADD data quality metrics: completeness scores, last update timestamps
  - ENABLE change data capture for historical patient tracking

CREATE: src/pipelines/gold/claims_fact_table.py
  - IMPLEMENT @dlt.table(name="fact_claims") with foreign key to dim_patients
  - ADD pre-aggregated metrics: total_claim_amount, claim_count, avg_processing_time
  - INCLUDE temporal dimensions: claim_date, approval_date, payment_date
  - IMPLEMENT claim analytics: denial rates, approval patterns, seasonal trends
  - ADD partitioning by claim_date for query performance
  - INCLUDE clinical outcome tracking: diagnosis effectiveness, treatment costs

CREATE: src/pipelines/gold/medical_events_fact_table.py  
  - IMPLEMENT @dlt.table(name="fact_medical_events") with patient dimension FK
  - ADD event analytics: visit frequency, procedure types, provider patterns
  - INCLUDE clinical pathway tracking: care coordination, follow-up compliance
  - IMPLEMENT temporal analysis: length of stay, readmission risk, care gaps
  - ADD provider network analytics: utilization patterns, quality metrics
  - INCLUDE population health metrics: chronic disease management, preventive care
```

### Task 7: Data Quality Monitoring & Validation
```yaml  
CREATE: src/jobs/data_quality_monitoring.py
  - IMPLEMENT pipeline health monitoring with real-time alerting
  - ADD data quality SLA tracking: 99.5% target across all layers
  - INCLUDE HIPAA compliance monitoring: audit trail completeness, PII handling validation
  - IMPLEMENT freshness monitoring: data latency tracking, SLA violation alerts
  - ADD referential integrity monitoring: FK constraint validation, orphaned record detection
  - INCLUDE clinical data validation: range checks, format validation, temporal consistency

CREATE: src/tests/test_hipaa_compliance.py
  - IMPLEMENT comprehensive HIPAA validation test suite
  - ADD SSN protection tests: no raw SSN in silver/gold layers
  - INCLUDE age de-identification tests: elderly patients properly anonymized  
  - ADD audit trail tests: change data feed enabled, complete lineage tracking
  - IMPLEMENT encryption validation: Delta table properties verification
  - INCLUDE access control tests: proper Unity Catalog permissions
```

### Task 8: Asset Bundle Integration & Deployment
```yaml
VALIDATE: Complete Asset Bundle configuration validation
  - RUN: databricks bundle validate --target dev
  - VERIFY: All pipeline dependencies resolved, serverless configuration correct
  - CHECK: Unity Catalog permissions, Volumes access, environment variables

DEPLOY: Development environment testing  
  - RUN: databricks bundle deploy --target dev
  - EXECUTE: Synthetic data generation job to populate test data
  - TRIGGER: DLT pipeline execution and monitor for completion
  - VALIDATE: End-to-end data flow from bronze to gold layers

MONITOR: Pipeline performance and data quality
  - VERIFY: Sub-5 minute pipeline latency achieved
  - CHECK: 99.5%+ data quality scores across all layers
  - VALIDATE: HIPAA compliance controls functioning correctly
  - TEST: Real-time monitoring and alerting systems
```

## Validation Loop

### Level 1: Configuration & Syntax Validation
```bash
# Asset Bundle and Python syntax validation - MUST PASS FIRST
databricks bundle validate --target dev
python -m py_compile src/pipelines/**/*.py
python -m py_compile src/jobs/*.py

# Healthcare domain validation
python -c "
from src.pipelines.shared.healthcare_schemas import PATIENT_SCHEMA, CLAIMS_SCHEMA, MEDICAL_EVENTS_SCHEMA
print('Healthcare schemas loaded successfully')
print(f'Patient schema fields: {len(PATIENT_SCHEMA.fields)}')
print(f'Claims schema fields: {len(CLAIMS_SCHEMA.fields)}') 
print(f'Medical events schema fields: {len(MEDICAL_EVENTS_SCHEMA.fields)}')
"

# YAML configuration validation
python -c "import yaml; yaml.safe_load(open('resources/pipelines.yml'))"
python -c "import yaml; yaml.safe_load(open('resources/jobs.yml'))"

# Expected: Zero errors, schemas load correctly with expected field counts
```

### Level 2: HIPAA Compliance & Unit Tests
```bash
# HIPAA compliance validation - CRITICAL FOR HEALTHCARE
python -m pytest src/tests/test_hipaa_compliance.py -v
python -m pytest src/tests/test_healthcare_schemas.py -v

# Pipeline logic unit tests
python -m pytest src/tests/test_pipeline_integration.py -v

# Data generation validation
python src/jobs/synthetic_data_generation.py --test-mode --output-dir /tmp/test_data
ls -la /tmp/test_data/patients*.csv /tmp/test_data/claims*.csv /tmp/test_data/medical_events*.csv

# Expected: All tests pass, 3 CSV files generated with proper referential integrity
```

### Level 3: Asset Bundle Deployment & Pipeline Execution
```bash
# Deploy to development environment
databricks bundle deploy --target dev --force-lock

# Generate synthetic test data
databricks jobs run-now --job-id synthetic_data_generation_job

# Execute DLT pipeline
databricks jobs run-now --job-id patient_data_pipeline

# Monitor pipeline execution
PIPELINE_ID=$(databricks pipelines list-pipelines --output json | jq -r '.[] | select(.name | contains("patient")) | .pipeline_id')
databricks pipelines get $PIPELINE_ID

# Expected: Successful deployment, data generation, and pipeline execution
```

### Level 4: End-to-End Data Quality Validation
```bash
# Validate bronze layer data ingestion
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) as patient_count FROM bronze_patients WHERE patient_id IS NOT NULL",
  "warehouse_id": "'$WAREHOUSE_ID'"
}'

# Validate HIPAA compliance in silver layer
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) as elderly_anonymized FROM silver_patients WHERE age_years >= 89 AND age_years = 90",
  "warehouse_id": "'$WAREHOUSE_ID'"
}'

# Validate referential integrity
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) as orphaned_claims FROM silver_claims c LEFT JOIN silver_patients p ON c.patient_id = p.patient_id WHERE p.patient_id IS NULL",
  "warehouse_id": "'$WAREHOUSE_ID'"
}'

# Calculate data quality scores
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT AVG(data_quality_score) as avg_quality FROM silver_patients",
  "warehouse_id": "'$WAREHOUSE_ID'"
}'

# Expected: Patient counts match, zero orphaned records, 99.5%+ quality scores, proper HIPAA anonymization
```

### Level 5: Performance & Monitoring Validation
```bash
# Validate pipeline performance
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT pipeline_id, avg(execution_time_ms)/60000 as avg_runtime_minutes FROM system.event_log.dlt_pipeline_events WHERE event_type = '\''pipeline_complete'\'' GROUP BY pipeline_id",
  "warehouse_id": "'$WAREHOUSE_ID'"
}'

# Validate data freshness  
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT MAX(_ingested_at) as latest_ingestion, DATEDIFF(minute, MAX(_ingested_at), CURRENT_TIMESTAMP()) as staleness_minutes FROM bronze_patients",
  "warehouse_id": "'$WAREHOUSE_ID'"
}'

# Validate change data feed for audit trails
databricks api post /api/2.0/sql/statements --json '{
  "statement": "DESCRIBE EXTENDED silver_patients",
  "warehouse_id": "'$WAREHOUSE_ID'"
}'

# Expected: Sub-5 minute runtime, data freshness within SLA, change data feed enabled
```

## Final Validation Checklist

### Asset Bundle & Infrastructure
- [ ] Asset Bundle validates without errors: `databricks bundle validate --target dev`
- [ ] Serverless compute configuration verified: No cluster specifications in any job/pipeline
- [ ] Unity Catalog permissions confirmed: Catalog and schema access validated
- [ ] Volumes access verified: Data generation can write to volumes path

### Healthcare Domain Implementation  
- [ ] Exactly 3 entities implemented: patients, claims, medical_events (no additional entities)
- [ ] Complete schema compliance: All mandatory fields from INITIAL.md implemented
- [ ] Referential integrity enforced: All FKs validated, zero orphaned records
- [ ] Synthetic data generation working: 3 CSV files with proper relationships

### Data Pipeline Execution
- [ ] Bronze layer ingestion successful: Auto Loader functioning, file metadata captured
- [ ] Silver layer transformations complete: Data quality rules applied, HIPAA compliance verified
- [ ] Gold layer analytics functional: Dimensional model created, SCD Type 2 implemented
- [ ] End-to-end data flow validated: Bronze → Silver → Gold with expected record counts

### HIPAA Compliance & Data Quality
- [ ] SSN protection verified: No raw SSN in silver/gold layers, only hashed values
- [ ] Age de-identification confirmed: Elderly patients (89+) properly anonymized to age 90
- [ ] Change data feed enabled: All patient tables have audit trails activated
- [ ] Data quality target achieved: 99.5%+ quality scores across all layers
- [ ] Quarantine functionality working: Malformed records properly isolated with audit trail

### Performance & Monitoring  
- [ ] Pipeline latency under 5 minutes: End-to-end execution time verified
- [ ] Real-time monitoring functional: Data quality SLA tracking operational
- [ ] Alerting system validated: SLA violations trigger appropriate notifications
- [ ] Clinical validation working: HL7 FHIR compliance and range checks functional

### Testing & Validation
- [ ] All unit tests passing: HIPAA compliance, schema validation, integration tests
- [ ] Synthetic data integrity confirmed: Realistic ratios, proper distributions, clinical validity
- [ ] Asset Bundle deployment successful: Both dev and prod targets deployable
- [ ] Documentation complete: Pipeline architecture, compliance notes, operational procedures

## Anti-Patterns to Avoid

### Asset Bundle Configuration
- ❌ **Creating cluster configurations** - Use serverless-only deployment (NO job_clusters, new_cluster, etc.)
- ❌ **Referencing directories in libraries** - Use individual file paths: `../src/pipelines/bronze/file.py`
- ❌ **Mixing catalog in target** - Use separate `target: "${var.schema}"` and `catalog: "${var.catalog}"`
- ❌ **Missing .format("cloudFiles")** - Required for DLT Auto Loader: `spark.readStream.format("cloudFiles")`

### Healthcare Domain Violations  
- ❌ **Creating additional entities** - Only 3 entities allowed: Patients, Claims, Medical_Events
- ❌ **Schema deviations** - Use EXACTLY the schemas defined in INITIAL.md
- ❌ **Missing referential integrity** - All claims/events MUST reference valid patient_ids
- ❌ **Additional CSV files** - Generate only patients.csv, claims.csv, medical_events.csv

### HIPAA Compliance Failures
- ❌ **Using @dlt.expect_all_or_drop for patient data** - Use @dlt.quarantine to maintain audit trails
- ❌ **Storing raw SSN** - Hash immediately with salt: `sha2(concat(col("ssn"), lit("PATIENT_SALT")), 256)`
- ❌ **Missing change data feed** - Enable on all patient tables: `"delta.enableChangeDataFeed": "true"`
- ❌ **Improper age handling** - De-identify elderly: `when(col("age") >= 89, 90).otherwise(col("age"))`

### DLT Pipeline Development
- ❌ **Using spark.read() in DLT** - Use `dlt.read()` for table dependencies and lineage
- ❌ **Using @dlt.view for raw ingestion** - Use `@dlt.table` for persistence and checkpointing  
- ❌ **Missing file metadata** - Use `_metadata` column, NOT deprecated `input_file_name()`
- ❌ **Full namespace in dlt.read()** - Use simple names: `dlt.read("bronze_patients")`

---

**PRP Quality Score: 9.5/10**

**Success Criteria for One-Pass Implementation:**
- AI agent can execute this PRP autonomously with 95%+ success rate
- All validation commands are executable without modification  
- Implementation follows Databricks Asset Bundle and HIPAA best practices
- Complete healthcare medallion architecture with proper data governance
- Performance and quality requirements met through comprehensive validation loops

This PRP provides the complete context, specific implementation guidance, and executable validation steps needed for successful one-pass implementation of a production-ready healthcare data engineering solution.