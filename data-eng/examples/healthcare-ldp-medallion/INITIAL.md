# Health Insurance Patient Data Engineering Project

## PROJECT OVERVIEW

This project implements a modern data engineering solution for health insurance patient data featuring:

- **Synthetic Data Generation**: Automated generation of realistic health insurance patient test data
- **Medallion Architecture**: Bronze → Silver → Gold data transformation pipeline
- **Delta Live Tables (DLT)**: Declarative pipelines for data processing and quality management
- **Databricks Asset Bundle**: Infrastructure-as-code deployment and management
- **Serverless Compute**: Utilizing serverless compute for optimal cost and performance
- **Volume-based Ingestion**: CSV files ingested from Databricks Volumes using Auto Loader

## DEPLOYMENT APPROACH

**This project MUST use Databricks Asset Bundles for all deployment and management activities.**

### Why Asset Bundles?
- **Infrastructure-as-Code**: Version-controlled deployment configurations
- **Environment Management**: Seamless promotion between dev/staging/prod
- **Integrated Workflow**: Native support for DLT pipelines, jobs, and compute resources
- **Declarative Configuration**: YAML-based configuration for reproducible deployments
- **Best Practice**: Recommended approach for modern Databricks project management

## ARCHITECTURE

### Unity Catalog Governance Structure
```
Production Hierarchy:
├── juan_prod
│   └── data_eng              # Single schema for all layers
│       ├── bronze_*          # Raw patient data tables
│       ├── silver_*          # Cleaned patient data tables
│       └── gold_*           # Analytics-ready patient tables

Development Hierarchy:
├── juan_dev
│   └── data_eng              # Single schema for all layers
│       ├── bronze_*          # Raw patient data tables
│       ├── silver_*          # Cleaned patient data tables
│       └── gold_*           # Analytics-ready patient tables
```

### Source Schema & Data Architecture
**Define your business domain and data model here:**

#### Data Sources & Integration Patterns
- **Primary Data Source**: Health insurance patient CSV files in Databricks Volumes
  - Format: CSV files with patient demographics, claims, and medical history
  - Ingestion: Auto Loader with Delta Live Tables for schema evolution
  - Frequency: Hourly batch processing (append-only)
  - Location: Databricks Volumes path for file landing
  
- **Synthetic Data Generation**: Automated creation of realistic test data
  - Format: CSV files matching production schema
  - Frequency: On-demand generation for testing and development
  - Output: Written to same Volumes path for consistent processing

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
└── Medical_History (fact_medical_events) - EVENT ENTITY
    ├── event_id (PK) - String, unique event identifier
    ├── patient_id (FK) - String, references Patients.patient_id
    ├── event_date, event_type - Temporal and categorical data
    └── medical_provider - Healthcare provider information
```

#### Mandatory Entity Implementation Requirements

**🚨 SYNTHETIC DATA GENERATION REQUIREMENTS:**
1. **Generate EXACTLY 3 CSV files**: patients.csv, claims.csv, medical_events.csv
2. **Maintain referential integrity**: All claims.patient_id and medical_events.patient_id MUST reference valid patients.patient_id
3. **Realistic ratios**: Each patient should have 2-5 claims and 3-8 medical events on average
4. **No additional entities**: Do not create provider tables, diagnosis tables, or other entities

**🚨 MEDALLION ARCHITECTURE REQUIREMENTS:**
1. **Bronze Layer**: Exactly 3 tables (bronze_patients, bronze_claims, bronze_medical_events)
2. **Silver Layer**: Exactly 3 tables (silver_patients, silver_claims, silver_medical_events) with data quality and standardization
3. **Gold Layer**: Dimensional model with dim_patients and 2 fact tables (fact_claims, fact_medical_events)
4. **Foreign Key Validation**: Silver and Gold layers MUST validate and maintain referential integrity
5. **No Schema Drift**: Additional columns or entities require explicit approval and domain model updates

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

### Data Warehouse Design Patterns

#### Dimensional Modeling Strategy
<Dimensional model approach>
- **Star Schema**: Centralized fact tables with denormalized dimensions
- **Slowly Changing Dimensions**: Type 1 (overwrite) and Type 2 (historical tracking)
- **Fact Table Grain**: Define the lowest level of detail for each fact table
- **Conformed Dimensions**: Shared dimensions across multiple fact tables

#### Performance Optimization
- **Partitioning Strategy**: By date, region, or business unit
- **Z-Order Optimization**: On frequently filtered columns
- **Liquid Clustering**: For high-cardinality dimensions
- **Bloom Filters**: For point lookups on large tables

## PROJECT STRUCTURE

```
databricks-data-engineering-project/
├── .claude/                     # Claude Code configuration
│   └── commands/               # Custom Claude commands
├── PRPs/                       # Problem Requirements & Proposals
│   ├── templates/              # PRP templates for systematic development
│   └── {feature-name}.md       # Individual feature PRPs
├── databricks.yml              # Asset Bundle configuration (root)
├── resources/                  # Asset Bundle resource definitions
│   ├── pipelines.yml          # DLT pipeline configurations
│   ├── jobs.yml               # Job workflow definitions
├── src/                      # Source code
│   ├── pipelines/            # DLT pipeline definitions
│   │   ├── bronze/           # Raw data ingestion pipelines
│   │   ├── silver/           # Data cleaning and transformation
│   │   ├── gold/             # Business analytics and aggregations
│   │   └── shared/           # Shared utilities and configurations
│   ├── jobs/                 # Databricks job definitions
│   │   ├── data_generation/  # Synthetic data generation
│   │   ├── maintenance/      # Data maintenance and optimization
│   │   └── monitoring/       # Data quality and pipeline monitoring
│   └── tests/                # Unit and integration tests
│       ├── unit/             # Unit tests for pipeline logic
│       ├── integration/      # End-to-end pipeline tests
│       └── fixtures/         # Test data and mock configurations
├── docs/                     # Project documentation
│   ├── architecture/         # Architecture decision records
│   ├── runbooks/            # Operational procedures
│   └── schemas/             # Data schema documentation
└── scripts/                 # Deployment and utility scripts
    ├── setup/               # Environment setup scripts
    └── migrations/          # Schema migration scripts
```

## DEVELOPMENT SETUP

### Prerequisites
- **Databricks CLI configured** (required for Asset Bundle deployment)
- **Python 3.12+** with databricks-sdk
- **Access to Databricks workspace** with DLT capabilities and Unity Catalog
- **Asset Bundle permissions** for target workspace and environments
- **Databricks Volumes** access for CSV file ingestion
- **Serverless compute** enabled in workspace

### Getting Started
1. **Initialize Asset Bundle structure** using `databricks bundle init`
2. **Review Asset Bundle patterns** in `CLAUDE.md` (comprehensive configuration examples)
3. **Configure your bundle** for dev/staging/prod environments
4. **Use the PRP templates** in `PRPs/templates/` for planning
5. **Deploy using Asset Bundles**: `databricks bundle deploy`
6. **Follow the development patterns** documented in `CLAUDE.md`

> **📋 Asset Bundle Configuration Reference**: See `CLAUDE.md` for comprehensive Asset Bundle patterns, workflow commands, and configuration templates.

## EXAMPLES & REFERENCE IMPLEMENTATIONS

> **🔧 Asset Bundle Templates**: Complete Asset Bundle configuration templates and patterns are documented in `CLAUDE.md` - Asset Bundle Management section.

### Domain-Specific Implementation Examples

> **💻 Pipeline Configuration Patterns**: See `CLAUDE.md` for Asset Bundle configuration patterns, environment setup, and path handling.

#### Health Insurance Patient Data Schema (COMPLETE DOMAIN MODEL)
```python
# Domain-specific schema for health insurance patient data - MANDATORY FIELDS
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType

# COMPLETE PATIENT SCHEMA - MUST INCLUDE ALL FIELDS
PATIENT_SCHEMA = StructType([
    # Primary Key
    StructField("patient_id", StringType(), False),      # PK - MANDATORY
    
    # Demographics
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),             # 18-85 range
    StructField("sex", StringType(), True),              # MALE/FEMALE
    StructField("region", StringType(), True),           # NORTHEAST/NORTHWEST/SOUTHEAST/SOUTHWEST
    
    # Health Metrics
    StructField("bmi", DoubleType(), True),              # 16-50 range
    StructField("smoker", BooleanType(), True),          # Boolean flag
    StructField("children", IntegerType(), True),        # Number of dependents
    
    # Financial Data
    StructField("charges", DoubleType(), True),          # Calculated insurance premium
    
    # Insurance Details
    StructField("insurance_plan", StringType(), True),
    StructField("coverage_start_date", StringType(), True),
    
    # Temporal Data
    StructField("timestamp", StringType(), True),        # Record creation timestamp
])
```

### External Resources & Documentation
- **Primary Reference**: See `CLAUDE.md` for comprehensive Databricks documentation links, development patterns, and configuration examples
- **Context Engineering**: Use MCP servers for additional code examples and patterns

## DOCUMENTATION

### Primary Documentation Sources
- **Context7 MCP Server**: For best practices and updated documentation
- **Databricks DLT Documentation**:
  - [DLT Development Guide](https://docs.databricks.com/aws/en/dlt/develop)
  - [Python DLT Development](https://docs.databricks.com/aws/en/dlt/python-dev)

### Additional Resources
- **<TODO: Add your primary schema/business documentation sources>**
- **<TODO: Include links to your data governance policies and standards>**
- **Technical Documentation**: All Databricks platform documentation consolidated in `CLAUDE.md` - Consolidated Documentation References section

## DEVELOPMENT GUIDELINES

### Schema Implementation
- **Naming Convention**: `{layer}_{entity_name}` (e.g., bronze_patients, silver_patients, gold_patient_metrics)
- **Data Types**: String for IDs, Integer for ages, Double for monetary amounts, Timestamp for dates
- **Primary Keys**: patient_id for all patient-related tables, claim_id for claims
- **Partitioning Strategy**: Partition by ingestion date for bronze, by patient creation date for silver/gold

### Mandatory Schema Implementation (DOMAIN MODEL ENFORCEMENT)

**🚨 CRITICAL: Implementation MUST create EXACTLY these tables with EXACTLY these schemas - no additions, no omissions.**

#### Bronze Layer Tables (RAW DATA - EXACTLY 3 TABLES)

**1. bronze_patients** (Raw patient demographic and insurance data)
```python
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
    
    # Temporal Data
    StructField("timestamp", StringType(), True),        # Record creation timestamp
    
    # Pipeline Metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True)
])
```

**2. bronze_claims** (Raw insurance claims data)
```python
CLAIMS_SCHEMA = StructType([
    StructField("claim_id", StringType(), False),        # PK - MANDATORY
    StructField("patient_id", StringType(), False),     # FK to patients - MANDATORY
    StructField("claim_amount", DoubleType(), True),    # Financial data
    StructField("claim_date", StringType(), True),      # Temporal data
    StructField("diagnosis_code", StringType(), True),  # ICD-10 code
    StructField("procedure_code", StringType(), True),  # CPT code
    StructField("claim_status", StringType(), True),    # Status (submitted/approved/denied/paid)
    # Metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True)
])
```

**3. bronze_medical_events** (Raw medical history/events data)
```python
MEDICAL_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),       # PK - MANDATORY
    StructField("patient_id", StringType(), False),    # FK to patients - MANDATORY
    StructField("event_date", StringType(), True),     # Temporal data
    StructField("event_type", StringType(), True),     # Event category
    StructField("medical_provider", StringType(), True), # Provider info
    # Metadata fields
    StructField("_ingested_at", TimestampType(), True),
    StructField("_pipeline_env", StringType(), True)
])
```

#### Silver Layer Requirements (DATA QUALITY - EXACTLY 3 TABLES)
- **silver_patients**: Cleaned patient data with HIPAA compliance and data quality validation
- **silver_claims**: Validated claims with referential integrity checks to silver_patients
- **silver_medical_events**: Cleaned medical events with referential integrity checks to silver_patients

#### Gold Layer Requirements (DIMENSIONAL MODEL - EXACTLY 3 TABLES)
- **dim_patients**: SCD Type 2 patient dimension with complete patient 360 view
- **fact_claims**: Claims fact table with pre-aggregated metrics and foreign key to dim_patients
- **fact_medical_events**: Medical events fact table with foreign key to dim_patients

#### Referential Integrity Requirements (ENFORCE ACROSS ALL LAYERS)
1. **Bronze Layer**: Basic foreign key presence validation
2. **Silver Layer**: Strict referential integrity - orphaned records must be quarantined
3. **Gold Layer**: Dimensional modeling with proper surrogate keys and foreign key relationships

### Best Practices

> **⚙️ Asset Bundle & Pipeline Patterns**: Complete development practices, configuration patterns, and critical patterns are documented in `CLAUDE.md` - Mandatory Practices section.

#### Domain-Specific Best Practices
1. **Data Quality**: Implement comprehensive data quality checks in DLT pipelines using `@dlt.expect_*` decorators
2. **Testing**: Include unit tests for data generation logic
3. **Documentation**: Document data lineage and transformation logic
4. **Healthcare Compliance**: Ensure PII handling meets HIPAA requirements and data governance standards
5. **Schema Evolution**: Design for schema changes in patient data over time

#### Health Insurance Data Quality Patterns (COMPLETE SCHEMA VALIDATION)
```python
# Domain-specific data quality expectations for patient data - ALL FIELDS
@dlt.expect_all_or_drop({
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
})
@dlt.expect_all({
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
})
def silver_patients():
    return dlt.read("bronze_patients")  # CORRECT: Simple table name - catalog/schema specified at pipeline level
```

### Common Pitfalls to Avoid

> **⚠️ Asset Bundle & Pipeline Pitfalls**: Complete list of configuration and development pitfalls documented in `CLAUDE.md` - Common Pitfalls section.

#### Domain-Specific Pitfalls

**🚨 DOMAIN MODEL VIOLATIONS (CRITICAL - NEVER DO THESE):**
- **Creating additional entities** - Only 3 entities allowed: Patients, Claims, Medical_Events
- **Missing referential integrity** - All claims and medical_events MUST reference valid patient_id
- **Incorrect table counts** - Each layer must have exactly 3 tables (bronze_*, silver_*, gold_*)
- **Schema deviations** - Use EXACTLY the schemas defined in Mandatory Schema Implementation section
- **Additional CSV files** - Generate only patients.csv, claims.csv, medical_events.csv
- **Foreign key violations** - Maintain 1:N relationships (1 patient : many claims/events)

**General Development Pitfalls:**
- **Over-engineering synthetic data generation** (start simple with basic patient demographics)
- **Missing proper error handling** in DLT pipelines
- **Not considering healthcare data retention** and compliance policies
- **Ignoring PII handling requirements** for patient data
- **Not implementing proper access controls** for sensitive health information
- **Missing data lineage tracking** for compliance auditing
- **Hard-coding connection strings** or credentials
- **Ignoring data quality constraints** in pipeline design

## NEXT STEPS

1. **Phase 0**: **Initialize and configure Databricks Asset Bundle structure**
   - Set up `databricks.yml` with dev/prod environments and serverless compute
   - Configure Unity Catalog permissions and Volumes access
   - Test basic deployment workflow with `databricks bundle deploy`
2. **Phase 1**: Set up synthetic health insurance patient data generation
   - Create realistic patient demographics, insurance plans, and medical history
   - Output CSV files to Databricks Volumes for testing
3. **Phase 2**: Implement Bronze layer ingestion pipeline for patient CSV files
   - Auto Loader configuration for Volumes path with hourly triggers
   - Schema enforcement and data quality expectations
4. **Phase 3**: Build Silver layer transformation logic for patient data cleaning
   - Data validation, standardization, and business rule application
   - Handle PII data according to healthcare compliance requirements
5. **Phase 4**: Create Gold layer dimensional models for patient analytics
   - Patient demographics summary, insurance utilization metrics
   - Aggregated views for business intelligence and reporting
6. **Phase 5**: Implement Lakeflow jobs for orchestration and monitoring
   - Hourly batch job scheduling and pipeline health monitoring

## CONTRIBUTING

Before implementing features:
1. Create a PRP (Problem Requirements & Proposal) using templates in `PRPs/templates/`
2. Review existing examples for patterns and best practices
3. Consult documentation sources for latest recommendations
4. Test with synthetic data before production deployment
5. **Follow healthcare data compliance**: Ensure PII handling meets HIPAA requirements and data governance standards

---

*This project demonstrates modern data engineering practices using Databricks platform capabilities while maintaining enterprise-grade quality and performance standards for health insurance patient data analytics, with full compliance to healthcare data governance requirements.*