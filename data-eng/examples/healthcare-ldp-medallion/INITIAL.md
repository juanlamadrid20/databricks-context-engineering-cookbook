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

#### Entity Relationship Model
```
Health Insurance Domain: Patient Analytics
├── Patients (dim_patients)
│   ├── patient_id (PK)
│   ├── demographics (age, gender, location)
│   ├── insurance_details (plan_type, coverage_start_date)
│   └── SCD Type 2 for historical tracking
├── Claims (fact_claims)
│   ├── claim_id (PK)
│   ├── patient_id (FK)
│   ├── claim_amount, claim_date
│   ├── diagnosis_code, procedure_code
│   └── claim_status
└── Medical_History (fact_medical_events)
    ├── event_id (PK)
    ├── patient_id (FK)
    ├── event_date, event_type
    └── medical_provider
```

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
<TODO: Define your specific dimensional model approach>
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
├── environments/              # Environment-specific configurations
│   ├── dev.yml               # Development environment settings
│   ├── staging.yml           # Staging environment settings
│   └── prod.yml              # Production environment settings
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
- **Python 3.8+** with databricks-sdk
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

#### Health Insurance Patient Data Schema
```python
# Domain-specific schema for health insurance patient data
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

PATIENT_SCHEMA = StructType([
    StructField("patient_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("insurance_plan", StringType(), True),
    StructField("coverage_start_date", StringType(), True),
    StructField("claim_history", StringType(), True)
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

### Source Schema Elements
#### Primary Tables:
- **bronze_patients**: Raw patient demographic and insurance data from CSV files
  - patient_id (String, PK), first_name, last_name, age (Integer), gender, address, insurance_plan, coverage_start_date
- **silver_patients**: Cleaned and validated patient data with data quality checks
- **gold_patient_demographics**: Aggregated patient analytics for reporting
- **Metadata Fields**: _ingested_at (Timestamp), _pipeline_env (String), _rescued_data (for malformed records)
- **Data Quality**: Comprehensive expectations for age ranges, valid gender codes, non-null patient IDs
- **Incremental Loading**: Append-only pattern for new patient registrations
- **SCD Type 2**: For tracking changes in patient insurance plans over time

### Best Practices

> **⚙️ Asset Bundle & Pipeline Patterns**: Complete development practices, configuration patterns, and critical patterns are documented in `CLAUDE.md` - Mandatory Practices section.

#### Domain-Specific Best Practices
1. **Data Quality**: Implement comprehensive data quality checks in DLT pipelines using `@dlt.expect_*` decorators
2. **Testing**: Include unit tests for data generation logic
3. **Documentation**: Document data lineage and transformation logic
4. **Healthcare Compliance**: Ensure PII handling meets HIPAA requirements and data governance standards
5. **Schema Evolution**: Design for schema changes in patient data over time

#### Health Insurance Data Quality Patterns
```python
# Domain-specific data quality expectations for patient data
@dlt.expect_all_or_drop({
    "valid_patient_id": "patient_id IS NOT NULL AND LENGTH(patient_id) >= 5",
    "valid_age": "age IS NOT NULL AND age BETWEEN 0 AND 150",
    "valid_gender": "gender IN ('M', 'F', 'Other', 'Unknown')"
})
@dlt.expect_all({
    "complete_name": "first_name IS NOT NULL AND last_name IS NOT NULL",
    "valid_coverage_date": "coverage_start_date IS NOT NULL",
    "valid_insurance_plan": "insurance_plan IS NOT NULL"
})
def silver_patients():
    return dlt.read(f"{CATALOG}.{SCHEMA}.bronze_patients")
```

### Common Pitfalls to Avoid

> **⚠️ Asset Bundle & Pipeline Pitfalls**: Complete list of configuration and development pitfalls documented in `CLAUDE.md` - Common Pitfalls section.

#### Domain-Specific Pitfalls
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