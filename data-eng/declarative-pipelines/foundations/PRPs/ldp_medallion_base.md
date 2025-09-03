# Medallion Architecture Data Pipeline PRP Template

## How to Use This Template

**🚨 IMPORTANT: This is a GENERIC TEMPLATE for building medallion architecture pipelines with Lakeflow Declarative Pipelines (DLT). You MUST customize it for your specific domain and business entities.**

### Template Customization Steps

1. **Replace `{DOMAIN}` placeholders** with your business domain (e.g., healthcare, retail, finance, manufacturing)
2. **Replace `{ENTITY_1}`, `{ENTITY_2}`, `{ENTITY_3}` placeholders** with your specific business entities
3. **Update data sources** to match your actual systems (replace `{SOURCE_SYSTEM_1}`, etc.)
4. **Customize compliance requirements** based on your industry (GDPR, HIPAA, PCI, etc.)
5. **Add domain-specific validation rules** and data quality expectations
6. **Update schema definitions** to match your actual data structures

### Quick Customization Example
```yaml
# Healthcare Domain Example:
{DOMAIN} → healthcare
{ENTITY_1} → patients
{ENTITY_2} → claims  
{ENTITY_3} → medical_events
{SOURCE_SYSTEM_1} → EHR_SYSTEM
{COMPLIANCE_FRAMEWORK} → HIPAA

# E-commerce Domain Example:
{DOMAIN} → ecommerce
{ENTITY_1} → customers
{ENTITY_2} → orders
{ENTITY_3} → products
{SOURCE_SYSTEM_1} → SHOPIFY_API
{COMPLIANCE_FRAMEWORK} → GDPR
```

---

## Goal
**Build a production-ready {DOMAIN} data medallion pipeline using Lakeflow Declarative Pipelines (DLT) that processes {DOMAIN} business data through Bronze → Silver → Gold layers with comprehensive data quality controls, {COMPLIANCE_FRAMEWORK} compliance, identity resolution, and real-time monitoring.**

## Why
- **Business Value**: Establish enterprise-grade data platform for {DOMAIN} analytics with proper governance and regulatory compliance
- **Operational Impact**: Enable accurate {ENTITY_1} 360° views, {DOMAIN} analytics, and operational insights for improved business outcomes
- **Regulatory Compliance**: Ensure {COMPLIANCE_FRAMEWORK}-compliant data processing with audit trails and access controls
- **Data Quality**: Achieve 99.5%+ data quality score through comprehensive validation and {DOMAIN}-specific validation rules
- **Real-Time Analytics**: Support {DOMAIN} decision-making with low-latency data processing

## What
**A complete medallion architecture pipeline processing exactly 3 {DOMAIN} business entities ({ENTITY_1}, {ENTITY_2}, {ENTITY_3}) with identity resolution, synthetic data generation, and comprehensive observability.**

### Success Criteria
- [ ] **Exactly 3 business entities**: {ENTITY_1}, {ENTITY_2}, {ENTITY_3} (strict domain model enforcement)
- [ ] **Complete medallion pipeline**: Bronze → Silver → Gold with proper DLT data quality expectations
- [ ] **99.5% data quality score** with comprehensive validation rules and {COMPLIANCE_FRAMEWORK} compliance 
- [ ] **Identity resolution implementation**: Multi-source enrichment and conflict resolution
- [ ] **Synthetic data generation job**: Automated CSV generation with timestamped filenames and referential integrity
- [ ] **Observable pipeline metrics**: Real-time monitoring, alerting, and data governance dashboards
- [ ] **Asset Bundle deployment**: Infrastructure-as-code with serverless compute configuration
- [ ] **{COMPLIANCE_FRAMEWORK} compliance**: PII handling, audit trails, change data feed, and data protection

### {DOMAIN} Entity Model (MANDATORY - STRICT ENFORCEMENT)

**🚨 CRITICAL: Implementation MUST create EXACTLY these 3 entities with EXACTLY these schemas - no additions, no omissions.**

```
Data Sources → Core Business Entities Enrichment Model
│
├── {SOURCE_SYSTEM_1} ({SOURCE_SYSTEM_1_DESCRIPTION})
│   ├── PRIMARY ENRICHMENT: bronze_{ENTITY_1} → silver_{ENTITY_1}
│   │   ├── {ENTITY_1} core attributes (TODO: customize for your domain)
│   │   ├── {ENTITY_1} metadata and identifiers (TODO: customize for your domain)
│   │   └── {ENTITY_1} business context (TODO: customize for your domain)
│
├── {SOURCE_SYSTEM_2} ({SOURCE_SYSTEM_2_DESCRIPTION})  
│   ├── PRIMARY ENRICHMENT: bronze_{ENTITY_2} → silver_{ENTITY_2}
│   │   ├── {ENTITY_2} transactional data (TODO: customize for your domain)
│   │   ├── {ENTITY_2} business rules and validation (TODO: customize for your domain)
│   │   └── {ENTITY_2} temporal tracking (TODO: customize for your domain)
│
└── {SOURCE_SYSTEM_3} ({SOURCE_SYSTEM_3_DESCRIPTION})
    ├── PRIMARY ENRICHMENT: bronze_{ENTITY_3} → silver_{ENTITY_3}
    │   ├── {ENTITY_3} events and activities (TODO: customize for your domain)
    │   ├── {ENTITY_3} contextual information (TODO: customize for your domain)
    │   └── {ENTITY_3} outcome tracking (TODO: customize for your domain)
```

**Mandatory Entity Implementation:**
1. **{ENTITY_1}** (dim_{ENTITY_1}): Primary entity with core attributes and business context
2. **{ENTITY_2}** (fact_{ENTITY_2}): Transactional entity with business transactions and amounts
3. **{ENTITY_3}** (fact_{ENTITY_3}): Event entity with activities and outcomes

## All Needed Context

### Primary Development Reference
```yaml
# MANDATORY - Complete technical development context
- file: CLAUDE.md
  why: "Complete Databricks development patterns, Asset Bundle configurations, DLT pipeline examples, serverless compute requirements, anti-patterns to prevent, environment configuration patterns, file metadata handling, and all external documentation references"
  extract: "All sections for comprehensive context - Critical DLT patterns, Asset Bundle serverless requirements, Unity Catalog naming, data quality expectations, anti-patterns documentation, and consolidated documentation references"

# Domain-Specific Documentation (TODO: CUSTOMIZE FOR YOUR DOMAIN)  
- url: {DOMAIN_SPECIFIC_STANDARDS_URL}
  why: "{DOMAIN} data standards specifications for {DOMAIN} validation rules and industry interoperability requirements (TODO: Add your industry standards)"
  
- url: {COMPLIANCE_FRAMEWORK_URL}
  why: "{COMPLIANCE_FRAMEWORK} compliance requirements for data handling, encryption, audit logging, and access controls (TODO: Add your compliance documentation)"

- url: https://databrickslabs.github.io/dbldatagen/public_docs/index.html
  why: "Synthetic {DOMAIN} data generation patterns, realistic {ENTITY_1}/{ENTITY_2}/{ENTITY_3} data modeling with proper referential integrity"

# Critical Databricks Documentation (from CLAUDE.md consolidated references)
- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-python-ref.html
  why: "DLT decorators (@dlt.table, @dlt.expect_*), streaming tables, pipeline patterns, and serverless compute requirements"

- url: https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html  
  why: "Data quality expectations, quarantine patterns, validation strategies, and {COMPLIANCE_FRAMEWORK} compliance controls"

- url: https://docs.databricks.com/data-governance/unity-catalog/best-practices.html
  why: "Three-part naming conventions, governance patterns, PII handling, and {DOMAIN} data classification"

# TODO: Add additional domain-specific documentation URLs
# Examples for different domains:
# Finance: SEC reporting requirements, PCI DSS standards
# Healthcare: HL7 FHIR, HIPAA compliance guidelines
# Retail: PCI compliance, inventory management standards
# Manufacturing: ISO standards, quality control requirements
```

### Implementation Reference
**CRITICAL**: All technical implementation details, DLT patterns, Asset Bundle configurations, and anti-patterns are documented in CLAUDE.md. Reference CLAUDE.md for:
- Environment-aware configuration loading patterns
- Critical DLT streaming and Autoloader patterns
- Asset Bundle serverless configurations 
- File metadata handling patterns
- Complete anti-pattern documentation to prevent common errors

## Implementation Blueprint

### Core Business Entity Schemas (TEMPLATE - CUSTOMIZE FOR YOUR DOMAIN)

**🚨 IMPORTANT: These are TEMPLATE schemas that you MUST customize for your specific domain and business entities.**

```python
# EXAMPLE: {ENTITY_1} ENTITY SCHEMAS - CUSTOMIZE FOR YOUR DOMAIN
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType, BooleanType, DateType

# Bronze: Raw {SOURCE_SYSTEM_1} format (EXAMPLE - Primary Entity)
BRONZE_{ENTITY_1}_SCHEMA = StructType([
    StructField("{entity_1}_id", StringType(), False),     # Primary {entity_1} identifier
    # TODO: Add your {entity_1}-specific fields here
    StructField("{field_1}", StringType(), True),          # TODO: Replace with actual field name and description
    StructField("{field_2}", StringType(), True),          # TODO: Replace with actual field name and description
    StructField("{sensitive_field}", StringType(), True),  # TODO: Replace with actual sensitive field (will be hashed if needed)
    StructField("{business_identifier}", StringType(), True), # TODO: Replace with business-specific identifier
    StructField("source_system", StringType(), False),     # {SOURCE_SYSTEM_1} system identifier
    StructField("_ingested_at", TimestampType(), True)     # Pipeline metadata
])

# Silver: Standardized business format (EXAMPLE - Primary Entity)
SILVER_{ENTITY_1}_SCHEMA = StructType([
    StructField("{entity_1}_id", StringType(), False),     # Primary {entity_1} identifier
    # TODO: Add your standardized {entity_1} fields here
    StructField("{derived_field_1}", IntegerType(), True), # TODO: Replace with calculated/derived field
    StructField("{category_field}", StringType(), True),   # TODO: Replace with categorization field
    StructField("{sensitive_field}_hash", StringType(), True), # TODO: Replace with hashed sensitive field for privacy
    StructField("{entity_1}_risk_category", StringType(), True), # TODO: Replace with risk classification
    StructField("data_quality_score", DoubleType(), True), # Data quality metric
    StructField("effective_date", TimestampType(), False), # When record becomes effective
    StructField("processed_at", TimestampType(), True)     # Pipeline metadata
])

# Gold: Analytics-ready dimension with SCD Type 2 (EXAMPLE - Primary Entity)
GOLD_{ENTITY_1}_SCHEMA = StructType([
    StructField("{entity_1}_id", StringType(), False),     # Primary {entity_1} identifier
    # TODO: Add your analytics-ready {entity_1} fields here
    StructField("{derived_field_1}", IntegerType(), True), # TODO: Replace with analytics field
    StructField("{category_field}", StringType(), True),   # TODO: Replace with categorization field
    StructField("{entity_1}_risk_category", StringType(), True), # TODO: Replace with risk classification
    StructField("effective_from", TimestampType(), False), # SCD Type 2 effective start
    StructField("effective_to", TimestampType(), True),    # SCD Type 2 effective end
    StructField("is_current", BooleanType(), False)        # Current record flag
])

# TODO: CREATE SIMILAR SCHEMA DEFINITIONS FOR {ENTITY_2} AND {ENTITY_3}
# 
# Following the same pattern above, create:
# - BRONZE_{ENTITY_2}_SCHEMA (transactional entity with foreign key to {ENTITY_1})
# - SILVER_{ENTITY_2}_SCHEMA (standardized transactional data)
# - GOLD_{ENTITY_2}_SCHEMA (fact table with partitioning)
#
# - BRONZE_{ENTITY_3}_SCHEMA (event entity with foreign key to {ENTITY_1})
# - SILVER_{ENTITY_3}_SCHEMA (standardized event data)
# - GOLD_{ENTITY_3}_SCHEMA (fact table with partitioning)
#
# Key patterns to follow:
# 1. Bronze: Raw string fields from source systems
# 2. Silver: Standardized data types, derived fields, data quality scores
# 3. Gold: Analytics-ready with partitioning and SCD Type 2 for dimensions
# 4. All entities except {ENTITY_1} should have foreign key to {ENTITY_1}
# 5. Include source_system and timestamp fields for lineage tracking
```

### Task Implementation Sequence (Execute in Order)

#### Task 0: Synthetic Data Generation Job Setup (FIRST PRIORITY)
**CRITICAL: This must be implemented FIRST before any pipeline development**

```yaml
Task_0_Synthetic_Data_Generation:
  CREATE: src/jobs/data_generation/synthetic_{entity_1}_generator.py
    - IMPLEMENT: Generate realistic {entity_1} data with {domain} patterns
    - SCHEMA: Use exact BRONZE_{ENTITY_1}_SCHEMA fields with realistic data distributions
    - PATTERNS: TODO: Add your domain-specific data patterns (e.g., age distributions, regional patterns, business rules)
    - REFERENTIAL_INTEGRITY: Generate {entity_1}_id as primary key for {entity_2}/{entity_3} relationships
    
  CREATE: src/jobs/data_generation/synthetic_{entity_2}_generator.py  
    - IMPLEMENT: Generate {entity_2} transactions linked to existing {entity_1}_ids
    - RATIOS: TODO: Define realistic ratios (e.g., 2-5 {entity_2} per {entity_1} on average)
    - VALIDATION: Ensure all {entity_2}.{entity_1}_id references valid {entity_1}.{entity_1}_id
    - CODING: TODO: Include realistic business codes specific to your domain
    
  CREATE: src/jobs/data_generation/synthetic_{entity_3}_generator.py
    - IMPLEMENT: Generate {entity_3} events linked to existing {entity_1}_ids  
    - RATIOS: TODO: Define realistic ratios (e.g., 3-8 {entity_3} per {entity_1} on average)
    - TYPES: TODO: Include domain-specific event types (e.g., activities, interactions, transactions)
    - VALIDATION: Ensure all {entity_3}.{entity_1}_id references valid {entity_1}.{entity_1}_id
    
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
    - ADD: Bundle configuration with {compliance_framework} compliance variables
    - CONFIGURE: Dev/staging/prod targets with Unity Catalog integration
    - VARIABLES: catalog, schema, environment, volumes_path, max_files_per_trigger
    - INCLUDE: resources/*.yml pattern for resource organization
    
  CREATE: resources/pipelines.yml
    - ADD: {domain}_data_medallion_pipeline DLT pipeline resource
    - SERVERLESS: true (MANDATORY - NO cluster configurations)  
    - CATALOG: ${var.catalog} and target: ${var.schema} (separate specification)
    - LIBRARIES: Individual file paths (NOT directories) for all pipeline files
    - CONFIGURATION: {COMPLIANCE_FRAMEWORK} compliance settings, change data feed enabled
    
  CREATE: resources/workflows.yml  
    - ADD: Data generation job scheduling and orchestration
    - ADD: Pipeline monitoring and alerting workflows
    - SERVERLESS: All jobs use serverless compute (NO cluster configurations)
```

#### Task 2: Bronze Layer - Raw Data Ingestion (3 Tables Exactly)
```yaml
Task_2_Bronze_Layer_Implementation:
  CREATE: src/pipelines/bronze/bronze_{entity_1}.py
    - IMPLEMENT: @dlt.table with bronze_{entity_1} table  
    - AUTOLOADER: Use .format("cloudFiles") with CSV ingestion from Volumes
    - SCHEMA: Use exact BRONZE_{ENTITY_1}_SCHEMA with schema enforcement
    - METADATA: Include _metadata column handling for file tracking
    - EXPECTATIONS: @dlt.expect_all for basic {entity_1} data validation
    - AUDIT: Enable change data feed for {COMPLIANCE_FRAMEWORK} compliance
    
  CREATE: src/pipelines/bronze/bronze_{entity_2}.py
    - IMPLEMENT: @dlt.table with bronze_{entity_2} table
    - AUTOLOADER: CSV ingestion with schema evolution for {entity_2} data
    - SCHEMA: Use exact BRONZE_{ENTITY_2}_SCHEMA with validation
    - FOREIGN_KEY: Basic validation that {entity_1}_id is not null
    - EXPECTATIONS: @dlt.expect_all_or_drop for malformed {entity_2} records
    
  CREATE: src/pipelines/bronze/bronze_{entity_3}.py  
    - IMPLEMENT: @dlt.table with bronze_{entity_3} table
    - AUTOLOADER: CSV ingestion for {entity_3} from {SOURCE_SYSTEM_3} systems
    - SCHEMA: Use exact BRONZE_{ENTITY_3}_SCHEMA
    - FOREIGN_KEY: Basic validation that {entity_1}_id is not null
    - EXPECTATIONS: @dlt.expect_all for {domain} data validation
    
  CREATE: src/pipelines/shared/{domain}_schemas.py
    - IMPLEMENT: Centralized schema definitions for all layers
    - INCLUDE: All BRONZE/SILVER/GOLD schema definitions
    - CONSTANTS: {DOMAIN} validation constants and business ranges
```

#### Task 3: Silver Layer - Data Transformation & Standardization (3 Tables Exactly)  
```yaml
Task_3_Silver_Layer_Implementation:
  CREATE: src/pipelines/silver/silver_{entity_1}.py
    - IMPLEMENT: @dlt.table with silver_{entity_1} table
    - DEPENDENCIES: Use dlt.read("bronze_{entity_1}") for input
    - TRANSFORMATIONS: TODO: Add domain-specific transformations (e.g., standardization, derivation, categorization)
    - COMPLIANCE: {COMPLIANCE_FRAMEWORK} data protection (TODO: customize based on sensitive fields)
    - EXPECTATIONS: @dlt.expect_all_or_drop for comprehensive {entity_1} validation
    - QUALITY_SCORING: Calculate data_quality_score based on completeness
    - RISK_ASSESSMENT: Derive {entity_1}_risk_category from business factors
    
  CREATE: src/pipelines/silver/silver_{entity_2}.py  
    - IMPLEMENT: @dlt.table with silver_{entity_2} table
    - DEPENDENCIES: Use dlt.read("bronze_{entity_2}") for input
    - REFERENTIAL_INTEGRITY: Validate {entity_1}_id exists in silver_{entity_1}
    - TRANSFORMATIONS: Amount conversion, date standardization, status normalization  
    - EXPECTATIONS: @dlt.expect_all_or_drop for {entity_2} validation
    - BUSINESS_CODING: TODO: Validate domain-specific codes and business rules
    
  CREATE: src/pipelines/silver/silver_{entity_3}.py
    - IMPLEMENT: @dlt.table with silver_{entity_3} table  
    - DEPENDENCIES: Use dlt.read("bronze_{entity_3}") for input
    - REFERENTIAL_INTEGRITY: Validate {entity_1}_id exists in silver_{entity_1}
    - TRANSFORMATIONS: Date standardization, event type categorization
    - EXPECTATIONS: @dlt.expect_all for {entity_3} validation
    - DOMAIN_VALIDATION: TODO: Implement domain-specific compliance and validation rules
```

#### Task 4: Gold Layer - Analytics-Ready Dimensional Model (3 Tables Exactly)
```yaml  
Task_4_Gold_Layer_Implementation:
  CREATE: src/pipelines/gold/gold_{entity_1}.py
    - IMPLEMENT: @dlt.table with gold_{entity_1} (dim_{entity_1}) table
    - DEPENDENCIES: Use dlt.read("silver_{entity_1}") for input  
    - SCD_TYPE_2: Implement Slowly Changing Dimension with effective_from/effective_to
    - HISTORICAL_TRACKING: Track {entity_1} changes over time with is_current flag
    - EXPECTATIONS: @dlt.expect_or_fail for dimensional integrity
    - CHANGE_DATA_FEED: Enable for audit trail and downstream analytics
    
  CREATE: src/pipelines/gold/gold_{entity_2}.py
    - IMPLEMENT: @dlt.table with gold_{entity_2} (fact_{entity_2}) table  
    - DEPENDENCIES: Use dlt.read("silver_{entity_2}") for input
    - FOREIGN_KEY: Validate {entity_1}_id exists in gold_{entity_1}
    - PARTITIONING: Partition by {time_partition_field}/{year_field} for performance
    - AGGREGATIONS: Pre-calculate {entity_2} metrics and categories
    - EXPECTATIONS: @dlt.expect_or_fail for fact table integrity
    
  CREATE: src/pipelines/gold/gold_{entity_3}.py
    - IMPLEMENT: @dlt.table with gold_{entity_3} (fact_{entity_3}) table
    - DEPENDENCIES: Use dlt.read("silver_{entity_3}") for input
    - FOREIGN_KEY: Validate {entity_1}_id exists in gold_{entity_1}  
    - PARTITIONING: Partition by {time_partition_field}/{year_field} for performance
    - BUSINESS_ANALYTICS: TODO: Include domain-specific outcome metrics and analytics
    - EXPECTATIONS: @dlt.expect_or_fail for fact table integrity
```

#### Task 5: Identity Resolution Layer (Enrichment)
```yaml
Task_5_Identity_Resolution_Implementation:
  CREATE: src/pipelines/identity_resolution/{entity_1}_identity_resolution.py
    - IMPLEMENT: Multi-source {entity_1} identity enrichment
    - SOURCES: {SOURCE_SYSTEM_1}, {SOURCE_SYSTEM_2}, {SOURCE_SYSTEM_3} identity resolution ingestion
    - CONFLICT_RESOLUTION: Handle conflicting {entity_1} data with highest_confidence strategy
    - TEMPORAL_ALIGNMENT: Maintain point-in-time {domain} accuracy
    - SCHEMA_EVOLUTION: Handle changing {domain} data structures
    
  CREATE: src/pipelines/identity_resolution/identity_resolution_engine.py  
    - IMPLEMENT: {ENTITY_1} entity resolution with {domain} identity awareness
    - MATCHING: TODO: Define matching keys (e.g., {entity_1}_id, {business_identifier}, composite keys)
    - CONFIDENCE_SCORING: Calculate resolution confidence using {domain} validation
    - LINEAGE_TRACKING: Maintain identity resolution provenance and audit trail
    - QUALITY_MONITORING: Real-time identity resolution quality metrics
    
  CREATE: src/pipelines/identity_resolution/identity_resolution_quality_monitoring.py
    - IMPLEMENT: SLA monitoring for identity resolution quality  
    - METRICS: Coverage, freshness, confidence, and completeness tracking
    - ALERTING: SLA violation detection for {domain} safety
    - DRIFT_MONITORING: Track identity resolution distribution changes
    - COMPLIANCE_REPORTING: Generate reports for {domain} stakeholders
```

### {DOMAIN}-Specific Data Quality Expectations Template

**🚨 IMPORTANT: These are TEMPLATE data quality expectations that you MUST customize for your specific domain and business rules.**

```python
# MANDATORY: Comprehensive data quality expectations for each layer (TODO: Customize for your domain)

# Bronze Layer - Basic Validation
@dlt.expect_all_or_drop({
    "valid_{entity_1}_id": "{entity_1}_id IS NOT NULL AND LENGTH({entity_1}_id) >= {MIN_ID_LENGTH}",  # TODO: Define minimum ID length
    "no_test_data": "TODO: Add test data detection rules for your domain",  # TODO: Define test data patterns
    "{compliance_framework}_compliant": "TODO: Add compliance validation rules",  # TODO: Add sensitive data validation
    "valid_source": "source_system IN ('{SOURCE_SYSTEM_1}', '{SOURCE_SYSTEM_2}', '{SOURCE_SYSTEM_3}')"  # TODO: Update with actual systems
})

# Silver Layer - Business Rule Validation  
@dlt.expect_all_or_drop({
    "valid_{entity_1}_core": "{entity_1}_id IS NOT NULL AND {REQUIRED_FIELD_1} IS NOT NULL AND {REQUIRED_FIELD_2} IS NOT NULL",  # TODO: Define required fields
    "{domain}_safety_{field}": "{NUMERIC_FIELD} >= {MIN_VALUE} AND {NUMERIC_FIELD} <= {MAX_VALUE}",  # TODO: Define business ranges
    "valid_{category_field}": "{CATEGORY_FIELD} IN ({VALID_VALUES})",  # TODO: Define valid category values
    "valid_{calculated_field}": "{CALCULATED_FIELD} IS NULL OR ({CALCULATED_FIELD} >= {MIN_VALUE} AND {CALCULATED_FIELD} <= {MAX_VALUE})",  # TODO: Define calculated field ranges
    "{compliance_framework}_data_protection": "TODO: Add domain-specific de-identification rules",  # TODO: Add data protection rules
    "data_quality_threshold": "data_quality_score >= 0.6"
})

# Gold Layer - Dimensional Integrity
@dlt.expect_or_fail({
    "no_duplicate_{entity_1}": "COUNT(*) = COUNT(DISTINCT {entity_1}_id)",
    "scd_integrity": "effective_from IS NOT NULL AND (effective_to IS NULL OR effective_from < effective_to)",
    "current_record_exists": "COUNT(*) > 0",
    "referential_integrity": "_rescued_data IS NULL"
})

# {ENTITY_2}-Specific Validation (TODO: Customize for your transactional entity)
@dlt.expect_all_or_drop({
    "valid_{amount_field}": "{amount_field} > 0 AND {amount_field} < {MAX_AMOUNT}",  # TODO: Define amount limits
    "valid_{status_field}": "{status_field} IN ({VALID_STATUS_VALUES})",  # TODO: Define valid status values
    "valid_{code_field}": "{code_field} IS NULL OR ({code_field} RLIKE '{CODE_REGEX_PATTERN}')",  # TODO: Define code validation pattern
    "{entity_1}_exists": "{entity_1}_id IS NOT NULL"
})

# {ENTITY_3}-Specific Validation (TODO: Customize for your event entity)
@dlt.expect_all({
    "valid_{date_field}": "{date_field} IS NOT NULL AND {date_field} <= current_date()",
    "valid_{type_field}": "{type_field} IN ({VALID_EVENT_TYPES})",  # TODO: Define valid event types
    "{provider_field}_information": "{provider_field} IS NOT NULL OR {type_field} = '{EXCEPTION_TYPE}'"  # TODO: Define provider requirements
})
```

### {COMPLIANCE_FRAMEWORK} Compliance Implementation Patterns Template

**🚨 IMPORTANT: These are TEMPLATE compliance patterns that you MUST customize for your specific compliance framework and sensitive data handling requirements.**

```python
# CRITICAL: {COMPLIANCE_FRAMEWORK} compliance patterns that MUST be implemented (TODO: Customize for your compliance framework)

# 1. Sensitive Data Hashing and Protection (TODO: Customize for your sensitive fields)
def apply_{compliance_framework}_data_protection(df):
    return (
        df
        # TODO: Hash sensitive fields immediately upon ingestion  
        .withColumn("{sensitive_field}_hash", sha2(concat(col("{sensitive_field}"), lit("{DOMAIN}_SALT")), 256))
        .drop("{sensitive_field}")  # Remove raw sensitive field immediately
        
        # TODO: Add domain-specific de-identification rules
        .withColumn("{field}_protected", 
                   when(col("{PROTECTION_CONDITION}"), "{PROTECTED_VALUE}")  # TODO: Define protection condition and value
                   .otherwise(col("{field}")))
                   
        # TODO: Add additional data protection transformations for your domain
        .withColumn("{location_field}_protected",
                   when(col("{PROTECTION_CONDITION}"), regexp_replace(col("{location_field}"), "{REGEX_PATTERN}", "{REPLACEMENT}"))
                   .otherwise(col("{location_field}")))
    )

# 2. Change Data Feed for Audit Trails (MANDATORY)
@dlt.table(
    table_properties={
        "delta.enableChangeDataFeed": "true",  # Compliance audit requirement
        "pipelines.pii.fields": "{entity_1}_id,{business_identifier},{sensitive_field}_hash",  # TODO: List PII fields
        "compliance.framework": "{COMPLIANCE_FRAMEWORK}",  # TODO: Set compliance framework
        "data.classification": "{DATA_CLASSIFICATION}"  # TODO: Set data classification (e.g., PII, PHI, Confidential)
    }
)

# 3. Data Quality Quarantine for {DOMAIN} Safety (TODO: Customize for your domain safety requirements)
@dlt.expect_all_or_drop({
    "{domain}_data_integrity": "{entity_1}_id IS NOT NULL AND {business_identifier} IS NOT NULL",  # TODO: Define critical fields
    "temporal_validity": "effective_date <= current_timestamp()",
    "no_future_{events}": "{date_field} <= current_date()",  # TODO: Define temporal validation rules
    # TODO: Add additional domain-specific safety validations
    "{domain}_business_rule_validation": "{BUSINESS_RULE_CONDITION}"  # TODO: Define business rule conditions
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

# Domain-specific validations (TODO: Customize for your domain)
python -m pytest tests/test_{compliance_framework}_compliance.py -v  # TODO: Replace with your compliance tests
python -c "import yaml; yaml.safe_load(open('resources/pipelines.yml'))"

# Expected: Zero errors. All {COMPLIANCE_FRAMEWORK} compliance tests must pass.
```

### Level 2: Unit Tests for Pipeline Logic  
```bash
# MANDATORY: Create and run comprehensive unit tests

# Create test files (TODO: Customize for your domain):
# - tests/test_{domain}_pipeline.py (bronze/silver/gold transformation tests)
# - tests/test_{compliance_framework}_compliance.py ({COMPLIANCE_FRAMEWORK} de-identification and audit tests) 
# - tests/test_data_quality.py (DLT expectation validation tests)
# - tests/test_synthetic_data.py (synthetic data generation validation)

# Run unit tests
python -m pytest tests/ -v --cov=src --cov-report=html

# Expected: 95%+ test coverage, all {COMPLIANCE_FRAMEWORK} compliance tests passing
```

### Level 3: Asset Bundle Deployment and Integration Testing
```bash
# Deploy to development environment
databricks bundle deploy --target dev

# Trigger synthetic data generation job FIRST
databricks jobs run-now --job-id synthetic_data_generation_job

# Monitor data generation completion
databricks jobs get-run <run_id>

# Trigger {domain} data pipeline run  
databricks jobs run-now --job-id {domain}_data_medallion_pipeline

# Monitor pipeline execution
databricks jobs get-run <run_id>

# Expected: Pipeline completes successfully with expected {entity_1}/{entity_2}/{entity_3} counts
```

### Level 4: Data Quality and {COMPLIANCE_FRAMEWORK} Compliance Validation
```bash
# Validate {entity_1} data ingestion and quality (TODO: Customize table and field names)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) as {entity_1}_count FROM bronze_{entity_1} WHERE {entity_1}_id IS NOT NULL",
  "warehouse_id": "<warehouse_id>"
}'

# Validate {COMPLIANCE_FRAMEWORK} compliance - no raw sensitive data in silver/gold layers (TODO: Customize sensitive field validation)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM silver_{entity_1} WHERE {sensitive_field}_hash IS NOT NULL AND {sensitive_field} IS NULL", 
  "warehouse_id": "<warehouse_id>"
}'

# Validate domain-specific data protection ({COMPLIANCE_FRAMEWORK} requirement) (TODO: Customize protection validation)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM silver_{entity_1} WHERE {PROTECTION_VALIDATION_CONDITION}",
  "warehouse_id": "<warehouse_id>"  
}'

# Validate referential integrity across entities (TODO: Customize entity relationships)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) as orphaned_{entity_2} FROM silver_{entity_2} c LEFT JOIN silver_{entity_1} p ON c.{entity_1}_id = p.{entity_1}_id WHERE p.{entity_1}_id IS NULL",
  "warehouse_id": "<warehouse_id>"
}'

# Validate data quality scores
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT AVG(data_quality_score) as avg_quality FROM silver_{entity_1}",
  "warehouse_id": "<warehouse_id>"
}'

# Expected Results (TODO: Customize for your domain):
# - {ENTITY_1} count > {MIN_TEST_DATA_VOLUME} (adequate test data volume)
# - Zero {entity_1} with raw sensitive data in silver/gold layers  
# - Zero {entity_1} violating {COMPLIANCE_FRAMEWORK} protection rules
# - Zero orphaned {entity_2} (referential integrity maintained)
# - Average data quality score >= 0.95 (99.5% target)
```

### Level 5: Identity Resolution Quality Monitoring (TODO: Customize for your domain)
```bash
# Validate identity resolution enrichment quality (TODO: Replace with your identity resolution table names)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT AVG(confidence_score) as avg_confidence FROM silver_resolved_identity_resolution WHERE confidence_score >= 0.85",
  "warehouse_id": "<warehouse_id>"
}'

# Check SLA compliance for identity resolution freshness (TODO: Customize SLA thresholds for your domain)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) as stale_identity_resolutions FROM silver_enriched_identity_resolution WHERE identity_resolution_age_hours > {SLA_HOURS_THRESHOLD}",
  "warehouse_id": "<warehouse_id>"
}'

# Expected: Average confidence >= 0.90, Zero stale identity resolutions
```

### Level 6: Performance and Optimization Validation
```bash
# Check pipeline execution time and performance metrics (TODO: Update pipeline name pattern)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT pipeline_id, update_id, execution_time_ms FROM system.event_log.dlt_pipeline_events WHERE pipeline_name LIKE '%{domain}_data_medallion%' ORDER BY timestamp DESC LIMIT 10",
  "warehouse_id": "<warehouse_id>"
}'

# Validate table optimization and liquid clustering (TODO: Update table name)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "DESCRIBE DETAIL silver_{entity_1}",
  "warehouse_id": "<warehouse_id>"
}'

# Expected: End-to-end pipeline execution < {PERFORMANCE_SLA_MINUTES} minutes, tables properly optimized
```

### Level 7: Final Production Readiness Validation
```bash
# Deploy to staging environment for final validation
databricks bundle deploy --target staging

# Run complete end-to-end pipeline in staging (TODO: Update job name)
databricks jobs run-now --job-id {domain}_data_medallion_pipeline_staging

# Validate Unity Catalog governance and permissions
databricks catalogs list
databricks schemas list --catalog-name <catalog>
databricks tables list --catalog-name <catalog> --schema-name <schema>

# Final {COMPLIANCE_FRAMEWORK} audit trail validation (TODO: Update table name and compliance requirements)
databricks api post /api/2.0/sql/statements --json '{
  "statement": "SELECT COUNT(*) FROM table_changes('silver_{entity_1}', 1) WHERE _change_type IN ('insert', 'update', 'delete')",
  "warehouse_id": "<warehouse_id>"  
}'

# Expected: All tables visible in Unity Catalog, change data feed working properly
```

## Final Validation Checklist Template

**🚨 IMPORTANT: This is a TEMPLATE checklist that you MUST customize for your specific domain, entities, and compliance requirements.**

### Technical Validation
- [ ] **Asset Bundle validates**: `databricks bundle validate --target dev` (zero errors)
- [ ] **Python syntax clean**: `python -m py_compile src/pipelines/**/*.py` (zero errors)  
- [ ] **All tests pass**: `python -m pytest tests/ -v` (95%+ coverage)
- [ ] **Pipeline deploys successfully**: `databricks bundle deploy --target dev` (successful deployment)
- [ ] **Synthetic data generation works**: Automated CSV generation with proper naming and referential integrity
- [ ] **Pipeline execution successful**: Complete Bronze → Silver → Gold data flow with expected volumes

### Data Quality Validation (TODO: Customize for your domain)
- [ ] **99.5%+ data quality achieved**: Average data_quality_score >= 0.95 across all {entity_1} records
- [ ] **Comprehensive expectations working**: All @dlt.expect_* decorators functioning properly with appropriate quarantine
- [ ] **Referential integrity maintained**: Zero orphaned {entity_2}/{entity_3} (all reference valid {entity_1}_ids)
- [ ] **{DOMAIN} validation rules**: TODO: Add domain-specific validation (e.g., business codes, industry standards compliance)
- [ ] **Performance within limits**: End-to-end pipeline execution < {PERFORMANCE_SLA_MINUTES} minutes

### {COMPLIANCE_FRAMEWORK} Compliance Validation (TODO: Customize for your compliance framework)
- [ ] **No raw sensitive data in silver/gold layers**: Only {sensitive_field}_hash present, raw {sensitive_field} removed immediately after hashing
- [ ] **{DOMAIN}-specific data protection**: TODO: Add domain-specific protection validation (e.g., age de-identification, location masking, etc.)
- [ ] **Change data feed enabled**: Audit trail available for all {entity_1} tables with CDC tracking
- [ ] **PII/sensitive fields properly marked**: Table properties include sensitive field identification for governance
- [ ] **Access controls implemented**: Unity Catalog permissions properly configured for {DATA_CLASSIFICATION} data

### Identity Resolution Validation (TODO: Customize for your domain)
- [ ] **Multi-source enrichment working**: {SOURCE_SYSTEM_1}, {SOURCE_SYSTEM_2}, and {SOURCE_SYSTEM_3} data properly integrated
- [ ] **Conflict resolution functioning**: Highest confidence strategy working for conflicting {entity_1} data  
- [ ] **Temporal alignment correct**: Point-in-time {domain} accuracy maintained across sources
- [ ] **Quality monitoring active**: Real-time SLA monitoring and alerting functional
- [ ] **Confidence thresholds met**: Average identity resolution confidence >= 0.90

### Production Readiness Validation
- [ ] **Observable pipeline metrics**: Real-time monitoring, alerting, and governance dashboards functional
- [ ] **Asset Bundle serverless deployment**: All components using serverless compute (no cluster configurations)
- [ ] **Unity Catalog integration**: Proper three-part naming, governance, and data classification
- [ ] **Documentation complete**: Comprehensive pipeline comments, {COMPLIANCE_FRAMEWORK} compliance notes, and operational guides
- [ ] **Disaster recovery tested**: Pipeline recovery procedures validated in staging environment

## Implementation Quality Score: __/10

### Scoring Matrix for One-Pass Implementation Success:

**Context Completeness (25%)**: 
- ✅ All necessary CLAUDE.md patterns and Databricks documentation referenced
- ✅ Real implementation examples with specific line references and anti-patterns  
- ✅ {DOMAIN}-specific standards and {COMPLIANCE_FRAMEWORK} requirements (TODO: Customize for your domain)
- ✅ Complete synthetic data generation and Asset Bundle serverless patterns

**Implementation Specificity (20%)**:
- ✅ Template schema definitions for all 3 entities across all medallion layers (TODO: Customize schemas)
- ✅ Specific DLT decorators with comprehensive data quality expectations (TODO: Customize validation rules)
- ✅ Unity Catalog three-part naming and governance patterns enforced
- ✅ Asset Bundle serverless configuration with individual file library references

**Validation Executability (20%)**:  
- ✅ Multi-level validation strategy with specific Databricks CLI commands
- ✅ {DOMAIN}-specific {COMPLIANCE_FRAMEWORK} compliance validation with expected results (TODO: Customize validation)
- ✅ Performance monitoring and identity resolution quality tracking
- ✅ Complete production readiness checklist with success criteria

**Data Quality Coverage (15%)**:
- ✅ Comprehensive @dlt.expect_* decorators template for all {domain} validation rules (TODO: Customize rules)
- ✅ {DOMAIN} data validation with industry standards compliance and business coding validation (TODO: Customize standards)
- ✅ {COMPLIANCE_FRAMEWORK} data protection and PII handling with audit trail requirements (TODO: Customize protection)
- ✅ 99.5% data quality target with specific measurement and monitoring

**Governance Compliance (10%)**:
- ✅ Unity Catalog naming conventions and {domain} data classification (TODO: Customize classification)
- ✅ {COMPLIANCE_FRAMEWORK} compliance with sensitive field hashing, audit trails, and change data feed (TODO: Customize compliance)
- ✅ Identity resolution governance with {domain} validation and provenance tracking (TODO: Customize validation)
- ✅ Asset Bundle infrastructure-as-code with serverless security controls

**Performance Optimization (10%)**:
- ✅ Serverless compute scaling with appropriate partitioning strategies  
- ✅ {DOMAIN} data optimization with liquid clustering and Z-ordering (TODO: Customize optimization)
- ✅ Identity resolution performance with relationship indexing and traversal limits
- ✅ Real-time monitoring with {domain} latency requirements (TODO: Define SLA)

### Target Score: 9/10 for One-Pass Implementation Success

**Success Criteria Met:**
- ✅ AI agent can execute PRP autonomously with 95%+ success rate after customization
- ✅ All validation commands are Databricks Asset Bundle compatible with serverless compute
- ✅ DLT pipeline template follows medallion architecture with proper {domain} data quality patterns
- ✅ Unity Catalog governance and {COMPLIANCE_FRAMEWORK} compliance patterns enforced throughout
- ✅ Performance considerations with {domain} latency and quality requirements
- ✅ Comprehensive identity resolution template with {domain}-specific conflict resolution patterns
- ✅ Complete synthetic data generation template with realistic {domain} data patterns

---

## How to Use This Foundation Template

**This generic foundation PRP template provides comprehensive, production-ready guidance for implementing ANY domain medallion pipeline with identity resolution, compliance framework support, and 99.5% data quality through Databricks Asset Bundles and serverless compute architecture.**

### Next Steps for Implementation:

1. **Customize the template** by replacing all `{PLACEHOLDER}` values with your specific domain values
2. **Define your business entities** and update all schema definitions
3. **Add domain-specific validation rules** and compliance requirements
4. **Update data quality expectations** for your business rules
5. **Customize identity resolution** for your specific matching requirements
6. **Test and validate** using the provided validation framework

This template ensures consistent, high-quality implementations across different domains while maintaining the proven patterns from the original healthcare implementation.